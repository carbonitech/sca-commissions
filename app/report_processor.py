from typing import List
import pandas as pd

from app import event
from db.db_services import DatabaseServices
from entities.preprocessor import PreProcessor
from entities.commission_data import PreProcessedData
from entities.submission import NewSubmission
from entities.error import ErrorType


class ReportProcessor:
    
    def __init__(
            self, preprocessor: PreProcessor, 
            submission: NewSubmission, database: DatabaseServices
        ):
        self.database = database
        self.submission = submission
        self.preprocessor = preprocessor
        self.map_customer_name = database.get_mappings("map_customer_name")
        self.map_city_names = database.get_mappings("map_city_names")
        self.map_state_names = database.get_mappings("map_state_names")
        self.customer_branches = database.get_customers_branches()
        self.reps_to_cust_branch_ref = database.get_reps_to_cust_branch_ref()


    def total_commissions(self) -> int:
        total_comm = self.staged_data.loc[:,"comm_amt"].sum()
        return round(total_comm)


    def total_sales(self) -> int:
        total_sales = self.staged_data.loc[:,"inv_amt"].sum()
        return round(total_sales)


    def fill_customer_ids(self) -> 'ReportProcessor':
        """converts customer column customer id #s using the map_customer_name reference table"""
        left_on_name = self.ppdata.customer_name_col
            
        merged_with_name_map = pd.merge(
                self.staged_data, self.map_customer_name,
                how="left", left_on=left_on_name, right_on="recorded_name"
            )
        
        # customer column is going from a name string to an id integer
        self.staged_data[left_on_name] = merged_with_name_map.loc[:,"customer_id"].fillna(0).astype(int)
        no_match_table = self.staged_data[self.staged_data[left_on_name] == 0]
        event.post_event(ErrorType(1), no_match_table)
        return self


    def fill_city_ids(self) -> 'ReportProcessor':
        """converts city column city id #s using the map_city_names reference table"""
        left_on_name = self.ppdata.city_name_col
            
        merged_w_cities_map = pd.merge(
                self.staged_data, self.map_city_names,
                how="left", left_on=left_on_name, right_on="recorded_name"
        )

        # city column is going from a name string to an id integer
        self.staged_data[left_on_name] = merged_w_cities_map.loc[:,"city_id"].fillna(0).astype(int)
        no_match_table = self.staged_data[self.staged_data[left_on_name] == 0]
        event.post_event(ErrorType(2), no_match_table)
        return self


    def fill_state_ids(self) -> 'ReportProcessor':
        """converts column supplied in the args to id #s using the map_state_names reference table"""
        left_on_name = self.ppdata.state_name_col

        merged_w_states_map = pd.merge(
                self.staged_data, self.map_state_names,
                how="left", left_on=left_on_name, right_on="recorded_name"
            )

        # state column is going from a name string to an id integer
        self.staged_data[left_on_name] = merged_w_states_map.loc[:,"state_id"].fillna(0).astype(int)
        no_match_table = self.staged_data[self.staged_data[left_on_name] == 0]
        event.post_event(ErrorType(3), no_match_table)
        return self


    def add_rep_customer_ids(self) -> 'ReportProcessor':
        """
        adds a map_rep_customer id column by comparing the customer, city,
        and state columns named in ref_columns to respective columns in a derived
        reps-to-customer reference table
        """
        new_column: str = "map_rep_customer_id"
        left_on_list = self.ppdata.map_rep_customer_ref_cols

        merged_w_reference = pd.merge(
            self.staged_data, self.reps_to_cust_branch_ref,
            how="left", left_on=left_on_list,
            right_on=["customer_id","city_id","state_id"]
        )

        new_col_values = merged_w_reference.loc[:,"map_rep_customer_id"].fillna(0).astype(int).to_list() # must be a list or else .insert will join on the indecies
        self.staged_data.insert(0,new_column,new_col_values) # only way i've found to avoid SettingWithCopyWarning
        no_match_table = self.staged_data.loc[self.staged_data[new_column] == 0]

        event.post_event(ErrorType(4), no_match_table)
        return self


    def filter_out_any_rows_unmapped(self) -> 'ReportProcessor':
        mask = self.staged_data.loc[:,~self.staged_data.columns.isin(["submission_id","inv_amt","comm_amt"])].all('columns')
        data_dropped = self.staged_data[~mask]
        self.staged_data = self.staged_data[mask]
        event.post_event("Rows Removed", data_dropped, self.submission_id)
        return self


    def register_submission(self) -> 'ReportProcessor':
        """reigsters a new submission to the database and returns the id number of that submission"""
        self.submission_id = self.database.record_submission(self.submission)
        # self.staged_data.insert(0,"submission_id",id_num)
        return self

    def drop_extra_columns(self) -> 'ReportProcessor':
        self.staged_data = self.staged_data.loc[:,["submission_id","map_rep_customer_id","inv_amt","comm_amt"]]
        return self

    def register_commission_data(self) -> 'ReportProcessor':
        self.database.record_final_data(self.staged_data)
        event.post_event("Data Recorded", self.staged_data, self.submission_id)
        return self

    def preprocess(self) -> 'ReportProcessor':
        data: PreProcessedData = self.preprocessor(self.submission, self.submission_id).preprocess()
        self.staged_data = data.data
        self.ppdata = data
        return self

    def insert_submission_id(self) -> 'ReportProcessor':
        self.staged_data.insert(0,"submission_id",self.submission_id)
        return self

    def process_and_commit(self) -> None:
        """
        Taking preprocessed data, use reference tables from the database
        to map customer names, city names, state names, and reps
        by id numbers

        Effects: commits the submission data, final commission data, errors, and processing steps
                to the database 
        """

        self.register_submission()              \
            .preprocess()                       \
            .insert_submission_id()             \
            .fill_customer_ids()                \
            .fill_city_ids()                    \
            .fill_state_ids()                   \
            .filter_out_any_rows_unmapped()     \
            .add_rep_customer_ids()             \
            .filter_out_any_rows_unmapped()     \
            .drop_extra_columns()               \
            .filter_out_any_rows_unmapped()     \
            .register_commission_data()

        return