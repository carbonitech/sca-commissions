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
        for sub_id in unmapped_no_match_table["submission_id"].unique().tolist():
            mask = unmapped_no_match_table["submission_id"] == sub_id
            sub_id_table = unmapped_no_match_table.loc[mask,:].set_index("row_index")
            sub_id_table = sub_id_table.loc[:,~sub_id_table.columns.isin(['submission_id','id','reason'])]
            event.post_event(
                ErrorType(1),
                sub_id_table,
                submission_id=sub_id,
                start_step=self.database.last_step_num(sub_id)+1
            )
        return self

    def process_and_commmit(self) -> None:
        self._filter_for_existing_records_with_target_error_type()
        self.fill_customer_ids()
        # self.remove_error_db_entries()
        
        return
