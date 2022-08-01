from typing import List
import pandas as pd

from db.db_services import DatabaseServices
from entities.commission_data import PreProcessedData
from entities.processing_step import ProcessingStep
from entities.submission import NewSubmission
from entities.error import Error, ErrorType


class ReportProcessor:
    
    def __init__(
            self, data: PreProcessedData, 
            submission: NewSubmission, database: DatabaseServices
        ):
        self.database = database
        self.map_customer_name = database.get_mappings("map_customer_name")
        self.map_city_names = database.get_mappings("map_city_names")
        self.map_state_names = database.get_mappings("map_state_names")
        self.customer_branches = database.get_customers_branches()
        self.reps_to_cust_branch_ref = database.get_reps_to_cust_branch_ref()
        self.submission = submission
        self.ppdata = data
        self.staged_data = data.data
        self.process_steps = data.process_steps
        self.process_errors: List[Error] = []
        self.last_num_errors = 0

    @staticmethod
    def processing_step_factory(step_description: str) -> ProcessingStep:
        return ProcessingStep(description=step_description)

    @staticmethod
    def error_factory(data: pd.DataFrame, field: str, reason: int,
            value_type: type, value_content: str = None) -> Error:

        return_values = []
        for row_index, row_data in data.to_dict("index").items():
            if not value_content:
                value_content = row_data[field]
            error_obj = Error(
                row_index=row_index,
                field=field,
                value_type=value_type,
                value_content=value_content,
                reason=reason,
                row_data={row_index: row_data})
            return_values.append(error_obj)
        return return_values


    def total_commissions(self) -> int:
        """calculate sum of commissions (in cents, rounded to an integer) present in staged data and errors"""
        # key-value over-writes using row-index deduplicates commission values
        errors_commission_dict = {
                error_obj.row_index: error_obj.row_data[error_obj.row_index]["comm_amt"]
                for error_obj in self.process_errors
            }
        total_comm = sum(list(errors_commission_dict.values()))
        total_comm += self.staged_data.loc[:,"comm_amt"].sum()
        return round(total_comm)


    def total_sales(self) -> int:
        errors_sales_dict = {
                error_obj.row_index: error_obj.row_data[error_obj.row_index]["inv_amt"]
                for error_obj in self.process_errors
            }
        total_sales = sum(list(errors_sales_dict.values()))
        total_sales += self.staged_data.loc[:,"inv_amt"].sum()
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
        error_reason = ErrorType(1)
        self.process_errors.extend(
            self.error_factory(no_match_table, left_on_name, error_reason, str)
        )

        self.process_steps.append(
            self.processing_step_factory("replaced customer names with customer ids where "
                "a customer reference name was found in the mapping in the database")
            )
        if len(self.process_errors) > self.last_num_errors:
            self.process_steps.append(
                self.processing_step_factory("failures in customer name mapping logged")
            )
            self.last_num_errors = len(self.process_errors)
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
        error_reason = ErrorType(2)
        self.process_errors.extend(
            self.error_factory(no_match_table, left_on_name, error_reason, str)
        )

        self.process_steps.append(
            self.processing_step_factory("replaced city names with city ids where "
                "a city reference name was found in the mapping in the database")
            )

        if len(self.process_errors) > self.last_num_errors:
            self.process_steps.append(
                self.processing_step_factory("failures in city name mapping logged")
            )
            self.last_num_errors = len(self.process_errors)

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
        error_reason = ErrorType(3)
        self.process_errors.extend(
            self.error_factory(no_match_table, left_on_name, error_reason, str)
        )

        self.process_steps.append(
            self.processing_step_factory("replaced state names with state ids where "
                "a state reference name was found in the mapping in the database")
            )

        if len(self.process_errors) > self.last_num_errors:
            self.process_steps.append(
                self.processing_step_factory("failures in state name mapping logged")
            )
            self.last_num_errors = len(self.process_errors)

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

        error_reason = ErrorType(4)
        self.process_errors.extend(
            self.error_factory(no_match_table, new_column, error_reason, int, 0)
        )

        self.process_steps.append(
            self.processing_step_factory("added the rep-to-customer mapping id by looking up customer, "
                "city, and state ids in a reference table")
        )
        if len(self.process_errors) > self.last_num_errors:
            self.process_steps.append(
                self.processing_step_factory("failures in rep-to-customer mapping logged")
        )
            self.last_num_errors = len(self.process_errors)

        return self


    def filter_out_any_rows_unmapped(self) -> 'ReportProcessor':
        columns = ', '.join(self.staged_data.columns.tolist()[:-2])
        mask = self.staged_data.all('columns')
        self.staged_data = self.staged_data[mask]
        self.process_steps.append(
            self.processing_step_factory(f"removed all rows that failed to map onto {columns}")
        )
        return self


    def register_submission_and_add_id(self) -> 'ReportProcessor':
        """reigsters a new submission to the database and returns the id number of that submission"""
        id_num = self.database.record_submission(self.submission)
        self.submission_id = id_num
        self.staged_data.insert(0,"submission_id",id_num)
        return self


    def register_all_errors(self) -> 'ReportProcessor':
        for error_obj in self.process_errors:
            error_obj.add_submission_id(self.submission_id)
            self.database.record_error(error_obj=error_obj)
        return self

    
    def register_all_process_steps(self) -> 'ReportProcessor':
        for process_obj in self.process_steps:
            process_obj.add_submission_id(self.submission_id)
            self.database.record_processing_step(step_obj=process_obj)
        return self


    def drop_extra_columns(self) -> 'ReportProcessor':
        self.staged_data = self.staged_data.loc[:,["submission_id","map_rep_customer_id","inv_amt","comm_amt"]]
        return self

    def register_commission_data(self) -> 'ReportProcessor':
        self.database.record_final_data(self.staged_data)
        return self

    def process_and_commit(self) -> None:
        """
        Taking preprocessed data, use reference tables from the database
        to map customer names, city names, state names, and reps
        by id numbers

        Effects: commits the submission data, final commission data, errors, and processing steps
                to the database 
        """

        self.fill_customer_ids()                \
            .fill_city_ids()                    \
            .fill_state_ids()                   \
            .filter_out_any_rows_unmapped()     \
            .add_rep_customer_ids()             \
            .filter_out_any_rows_unmapped()     \
            .register_submission_and_add_id()   \
            .drop_extra_columns()               \
            .filter_out_any_rows_unmapped()     \
            .register_all_errors()              \
            .register_all_process_steps()       \
            .register_commission_data()

        return