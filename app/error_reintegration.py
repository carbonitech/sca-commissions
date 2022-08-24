from typing import Hashable
import pandas as pd

from app import event
from db.db_services import DatabaseServices
from entities.error import ErrorType

class EmptyTableException(Exception):
    pass

class Reintegrator:

    def __init__(self, 
            target_err: ErrorType,
            error_table: pd.DataFrame,
            database: DatabaseServices
        ):
        self.database = database
        self.map_customer_name = database.get_mappings("map_customer_name")
        self.map_city_names = database.get_mappings("map_city_names")
        self.map_state_names = database.get_mappings("map_state_names")
        self.branches = database.get_branches()
        self.reps_to_cust_branch_ref = database.get_reps_to_cust_branch_ref()
        self.target_err = target_err
        self.error_table = pd.concat(   # expand row_data into dataframe columns
            [
                error_table, 
                pd.json_normalize(error_table.pop("row_data"),max_level=1)
            ], 
            axis=1).reset_index(drop=True)
        return

    def __enter__(self) -> 'Reintegrator':
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """## make sure unprocessed errors re-recorded in the db"""
        return

    def _send_event_by_submission(self, table: pd.DataFrame, event_: Hashable) -> None:
        for sub_id in table["submission_id"].unique().tolist():
            mask = table["submission_id"] == sub_id
            sub_id_table = table.loc[mask,:].set_index("row_index")
            sub_id_table = sub_id_table.loc[:,~sub_id_table.columns.isin(['submission_id','id','reason'])]
            event.post_event(
                event_,
                sub_id_table,
                submission_id=sub_id,
                start_step=self.database.last_step_num(sub_id)+1
            )


    def _filter_for_existing_records_with_target_error_type(self) -> 'Reintegrator':
        mask = self.error_table["reason"] == self.target_err.value
        table_target_errors = self.error_table.loc[mask]
        if table_target_errors.empty:
            raise EmptyTableException
        self.error_table = table_target_errors.reset_index(drop=True) # fixes for for id merging strategy
        self.staged_data = self.error_table.copy()
        return self

    def remove_error_db_entries(self) -> 'Reintegrator':
        self.database.delete_errors(self.staged_data["id"].to_list())
        return self

    def fill_customer_ids(self) -> 'Reintegrator':
        """converts customer column customer id #s using the map_customer_name reference table"""
        left_on_name = "customer"
            
        merged_with_name_map = pd.merge(
                self.staged_data, self.map_customer_name,
                how="left", left_on=left_on_name, right_on="recorded_name"
            )
        # customer column is going from a name string to an id integer
        self.staged_data.loc[:,left_on_name] = merged_with_name_map.loc[:,"customer_id"].fillna(0).astype(int)
        no_match_indices = self.staged_data.loc[self.staged_data[left_on_name] == 0].index.to_list()
        unmapped_no_match_table = self.error_table.iloc[no_match_indices,:]
        # this table has extra columns and combines multiple submissions
        self._send_event_by_submission(unmapped_no_match_table,ErrorType(1))
        return self

    def fill_city_ids(self) -> 'Reintegrator':
        """converts city column city id #s using the map_city_names reference table"""
        left_on_name = "city"
            
        merged_w_cities_map = pd.merge(
                self.staged_data, self.map_city_names,
                how="left", left_on=left_on_name, right_on="recorded_name"
        )

        # city column is going from a name string to an id integer
        self.staged_data.loc[:,left_on_name] = merged_w_cities_map.loc[:,"city_id"].fillna(0).astype(int)
        no_match_indices = self.staged_data.loc[self.staged_data[left_on_name] == 0].index.to_list()
        unmapped_no_match_table = self.error_table.iloc[no_match_indices,:]
        self._send_event_by_submission(unmapped_no_match_table,ErrorType(2))
        return self

    def fill_state_ids(self) -> 'Reintegrator':
        """converts column supplied in the args to id #s using the map_state_names reference table"""
        left_on_name = "state"

        merged_w_states_map = pd.merge(
                self.staged_data, self.map_state_names,
                how="left", left_on=left_on_name, right_on="recorded_name"
            )

        # state column is going from a name string to an id integer
        self.staged_data.loc[:,left_on_name] = merged_w_states_map.loc[:,"state_id"].fillna(0).astype(int)
        no_match_indices = self.staged_data.loc[self.staged_data[left_on_name] == 0].index.to_list()
        unmapped_no_match_table = self.error_table.iloc[no_match_indices,:]
        self._send_event_by_submission(unmapped_no_match_table,ErrorType(3))
        return self

    def add_branch_id(self) -> 'Reintegrator':
        """
        Adds the customer's branch id, if the assignment exists.
        Un-matched rows will get kicked to errors and removed
        """
        new_column: str = "branch_id"
        left_on_list = ["customer","city","state"]

        merged_with_branches = pd.merge(
                self.staged_data, self.branches,
                how="left", left_on=left_on_list,
                right_on=["customer_id","city_id","state_id"],
                suffixes=(None,"_ref_table")
        ) 

        new_col_values = merged_with_branches.loc[:,"id"].fillna(0).astype(int).to_list()
        self.staged_data.loc[:,new_column] = new_col_values

        no_match_indices = self.staged_data.loc[self.staged_data[new_column]==0].index.to_list()
        unmapped_no_match_table = self.error_table.iloc[no_match_indices,:]
        self._send_event_by_submission(unmapped_no_match_table,ErrorType(4))
        return self

    def add_rep_customer_ids(self) -> 'Reintegrator':
        """
        adds a map_rep_customer id column by comparing the customer, city,
        and state columns named in ref_columns to respective columns in a derived
        reps-to-customer reference table
        # TODO : CHANGE REFERENCE USED FOR MERGE TO USE JUST THE MAP-REP-CUSTOMERS TABLE AS-IS
        """
        new_column: str = "map_rep_customer_id"
        left_on_list = ["customer","city","state"]

        merged_w_reference = pd.merge(
            self.staged_data, self.reps_to_cust_branch_ref,
            how="left", left_on=left_on_list,
            right_on=["customer_id","city_id","state_id"],
            suffixes=(None,"_ref_table")
        )

        new_col_values = merged_w_reference.loc[:,"map_rep_customer_id"].fillna(0).astype(int).to_list()
        self.staged_data.insert(0,new_column,new_col_values)

        no_match_indices = self.staged_data.loc[self.staged_data[new_column] == 0].index.to_list()
        unmapped_no_match_table = self.error_table.iloc[no_match_indices]
        self._send_event_by_submission(unmapped_no_match_table,ErrorType(5))
        return self


    def filter_out_any_rows_unmapped(self) -> 'Reintegrator':
        mask = self.staged_data.loc[:,~self.staged_data.columns.isin(["id","row_index","submission_id","inv_amt","comm_amt"])].all('columns')
        data_dropped = self.staged_data[~mask]
        self.staged_data = self.staged_data[mask]
        self._send_event_by_submission(data_dropped, "Rows Removed")
        return self

    def drop_extra_columns(self) -> 'Reintegrator':
        self.staged_data = self.staged_data.loc[:,["submission_id","map_rep_customer_id","inv_amt","comm_amt"]]
        return self

    def register_commission_data(self) -> 'Reintegrator':
        self.database.record_final_data(self.staged_data)
        self._send_event_by_submission(self.staged_data, "Data Recorded")
        return self

    def process_and_commmit(self) -> None:
        self._filter_for_existing_records_with_target_error_type() \
        .remove_error_db_entries()      \
        .fill_customer_ids()            \
        .filter_out_any_rows_unmapped() \
        .fill_city_ids()                \
        .filter_out_any_rows_unmapped() \
        .fill_state_ids()               \
        .filter_out_any_rows_unmapped() \
        .add_branch_id()                \
        .filter_out_any_rows_unmapped() \
        .add_rep_customer_ids()         \
        .filter_out_any_rows_unmapped() \
        .drop_extra_columns()           \
        .filter_out_any_rows_unmapped() \
        .register_commission_data()

        return
