from typing import List
import pandas as pd
from db import db_services

from db.db_services import DatabaseServices

from entities.commission_data import PreProcessedData
from entities.processing_step import ProcessingStep
from entities.submission import NewSubmission
from entities.error import Error


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
    def error_factory(data: pd.DataFrame, field: str, reason: str,
            value_type, value_content: str = None) -> Error:

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


    def fill_customer_ids(self) -> 'ReportProcessor':
        """converts customer column customer id #s using the map_customer_name reference table"""
        left_on_name = self.ppdata.customer_name_col
            
        merged_with_name_map = pd.merge(
                self.staged_data, self.map_customer_name,
                how="left", left_on=left_on_name, right_on="recorded_name"
            )
        
        match_is_null = merged_with_name_map["recorded_name"].isnull()
        no_match_table = merged_with_name_map[match_is_null]

        # customer column is going from a name string to an id integer
        self.staged_data[left_on_name] = merged_with_name_map.loc[:,"customer_id"].fillna(0).astype(int)
        
        error_reason = "Customer name in the commission file is not mapped to a standard name"
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

        match_is_null = merged_w_cities_map["recorded_name"].isnull()
        no_match_table = merged_w_cities_map[match_is_null]

        # city column is going from a name string to an id integer
        self.staged_data[left_on_name] = merged_w_cities_map.loc[:,"city_id"].fillna(0).astype(int)

        error_reason = "City name in the commission file is not mapped to a standard name"
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

        match_is_null = merged_w_states_map["recorded_name"].isnull()
        no_match_table = merged_w_states_map[match_is_null]

        # state column is going from a name string to an id integer
        self.staged_data[left_on_name] = merged_w_states_map.loc[:,"state_id"].fillna(0).astype(int)

        error_reason = "State name in the commission file is not mapped to a standard name"
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
        new_column = "map_rep_customer_id"
        left_on_list = self.ppdata.map_rep_customer_ref_cols

        merged_w_reference = pd.merge(
            self.staged_data, self.reps_to_cust_branch_ref,
            how="left", left_on=left_on_list,
            right_on=["customer_id","city_id","state_id"]
        )

        match_is_null = merged_w_reference["customer_id"].isnull()
        no_match_table = merged_w_reference[match_is_null]

        self.staged_data[new_column] = merged_w_reference.loc[:,"map_rep_customer_id"].fillna(0).astype(int)

        error_reason = "Customer does not have a branch association with the city and state listed"
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
        self.staged_data["submission_id"] = id_num
        return self


    def drop_extra_columns(self) -> 'ReportProcessor':
        self.staged_data = self.staged_data.loc[:,["submission_id","map_rep_customer_id","inv_amt","comm_amt"]]
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
            .drop_extra_columns()


        return