from typing import Union, List
import pandas as pd

from db import db_services

from entities.commission_data import PreProcessedData
from entities.processing_step import ProcessingStep
from entities.manufacturer import Manufacturer
from entities.submission import NewSubmission
from entities.error import Error



class ReportProcessor:
    
    def __init__(self, data: PreProcessedData, manufacturer: Manufacturer):
        self.mappings = {}
        self.map_customer_name = pd.DataFrame()
        self.map_city_names = pd.DataFrame()
        self.map_state_names = pd.DataFrame()
        self.customer_branches = pd.DataFrame()
        self.reps_to_cust_branch_ref = pd.DataFrame(),
        self.data = data
        self.manufacturer = manufacturer
        

    def fill_customer_ids(self, column: Union[str,int]) -> pd.DataFrame:
        """converts column supplied in the args to id #s using the map_customer_name reference table"""
        data = self.data.data
        data_cols = data.columns.tolist()
        left_on_name = None

        if isinstance(column,int):
            left_on_name = data_cols[column]
        elif isinstance(column,str) and column in data_cols:
            left_on_name = column

        if not left_on_name:
            raise IndexError(f"column {column} is not in the dataset")
            
        customer_name_map = self.mappings["map_customer_name"]
        merged_with_name_map = pd.merge(data, customer_name_map,
                how="left", left_on=left_on_name, right_on="recorded_name")
        
        match_is_null = merged_with_name_map["recorded_name"].isnull()
        no_match_table = merged_with_name_map[match_is_null]

        # customer column is going from a name string to an id integer
        data[left_on_name] = merged_with_name_map.loc[:,"customer_id"].fillna(0).astype(int)
        
        error_reason = "Customer name in the commission file is not mapped to a standard name"
        self.record_mapping_errors(no_match_table, left_on_name, error_reason, str)

        return data


    def fill_city_ids(self, data: pd.DataFrame, column: Union[str,int]) -> pd.DataFrame:
        """converts column supplied in the args to id #s using the map_city_names reference table"""
        data_cols = data.columns.tolist()
        left_on_name = None

        if isinstance(column,int):
            left_on_name = data_cols[column]
        elif isinstance(column,str) and column in data_cols:
            left_on_name = column

        if not left_on_name:
            raise IndexError(f"column {column} is not in the dataset")
            
        merged_w_cities_map = pd.merge(
            data, self.mappings["map_city_names"],
            how="left", left_on=left_on_name, right_on="recorded_name"
        )

        match_is_null = merged_w_cities_map["recorded_name"].isnull()
        no_match_table = merged_w_cities_map[match_is_null]

        # city column is going from a name string to an id integer
        data[left_on_name] = merged_w_cities_map.loc[:,"city_id"].fillna(0).astype(int)

        error_reason = "City name in the commission file is not mapped to a standard name"
        self.record_mapping_errors(no_match_table, left_on_name, error_reason, str)

        return data


    def fill_state_ids(self, data: pd.DataFrame, column: Union[str,int]) -> pd.DataFrame:
        """converts column supplied in the args to id #s using the map_state_names reference table"""
        data_cols = data.columns.tolist()
        left_on_name = None

        if isinstance(column,int):
            left_on_name = data_cols[column]
        elif isinstance(column,str) and column in data_cols:
            left_on_name = column

        if not left_on_name:
            raise IndexError(f"column {column} is not in the dataset")

        merged_w_states_map = pd.merge(
            data, self.mappings["map_state_names"],
            how="left", left_on=left_on_name, right_on="recorded_name"
        )

        match_is_null = merged_w_states_map["recorded_name"].isnull()
        no_match_table = merged_w_states_map[match_is_null]

        # state column is going from a name string to an id integer
        data[left_on_name] = merged_w_states_map.loc[:,"state_id"].fillna(0).astype(int)

        error_reason = "State name in the commission file is not mapped to a standard name"
        self.record_mapping_errors(no_match_table, left_on_name, error_reason, str)

        return data


    def add_rep_customer_ids(self, data: pd.DataFrame, 
            ref_columns: list, new_column: str = "map_rep_customer_id") -> pd.DataFrame:
        """
        adds a map_rep_customer id column by comparing the customer, city,
        and state columns named in ref_columns to respective columns in a derived
        reps-to-customer reference table
        """
        data_cols = data.columns.tolist()
        left_on_list = []
        for column in ref_columns:
            if isinstance(column,int):
                left_on_list.append(data_cols[column])
            elif isinstance(column,str) and column in data_cols:
                left_on_list.append(column)

        if not left_on_list:
            raise IndexError(f"one or more columns in {ref_columns} are not in the dataset")

        merged_w_reference = pd.merge(
            data, self.reps_to_cust_branch_ref,
            how="left", left_on=left_on_list,
            right_on=["customer_id","city_id","state_id"]
        )

        match_is_null = merged_w_reference["customer_id"].isnull()
        no_match_table = merged_w_reference[match_is_null]

        data[new_column] = merged_w_reference.loc[:,"map_rep_customer_id"].fillna(0).astype(int)

        error_reason = "Customer does not have a branch association with the city and state listed"
        self.record_mapping_errors(no_match_table, new_column, error_reason, int, 0)

        return data


    @staticmethod
    def processing_step_factory(step_description: str) -> ProcessingStep:
        return ProcessingStep(step_desctription=step_description)

    @staticmethod
    def error_factory(data: pd.DataFrame, field: str, reason: str,
            value_type, value_content: str = None) -> Error:

        for row_index, row_data in data.to_dict("index").items():
            if not value_content:
                value_content = row_data[field]
            error_obj = Error(
                row_index=row_index,
                field=field,
                value_type=value_type,
                value_content=0,
                reason=reason,
                row_data={row_index: row_data})

        return error_obj

    def process(self, submission: NewSubmission, manufacturer: Manufacturer):

        process_errors: List[Error] = []
        process_steps: List[ProcessingStep] = []

        
        result = self.fill_customer_ids(result, column="customer")
        process_steps.append(self.processing_step_factory(submission,"replaced customer names with customer ids where "
                "a customer reference name was found in the mapping in the database"))
        if submission.errors:
            process_steps.append(self.processing_step_factory(submission,"failures in customer name mapping logged"))
            num_errors = len(submission.errors)
        
        result = self.fill_city_ids(result, column="city")
        process_steps.append(self.processing_step_factory(submission,"replaced city names with city ids where "
                "a city reference name was found in the mapping in the database"))
        if len(submission.errors) > num_errors:
            process_steps.append(self.processing_step_factory(submission,"failures in city name mapping logged"))
            num_errors = len(submission.errors)
        
        result = self.fill_state_ids(result, column="state")
        process_steps.append(self.processing_step_factory(submission,"replaced state names with state ids where "
                "a state reference name was found in the mapping in the database"))
        if len(submission.errors) > num_errors:
            process_steps.append(self.processing_step_factory(submission,"failures in state name mapping logged"))
            num_errors = len(submission.errors)

        mask = result.all('columns')
        map_rep_col_name = "map_rep_customer_id"
        result = self.add_rep_customer_ids(result[mask], ref_columns=["customer", "city", "state"],
            new_column=map_rep_col_name)  # pared down to only customers with all values != 0
        process_steps.append(self.processing_step_factory(submission,"removed all rows that failed to map either a customer id, city id, or state id"))
        process_steps.append(self.processing_step_factory(submission,"added the rep-to-customer mapping id by looking up customer, "
                "city, and state ids in a reference table"))
        if len(submission.errors) > num_errors:
            process_steps.append(self.processing_step_factory(submission,"failures in rep-to-customer mapping logged"))

        mask = result.all('columns')
        result = result[mask]  # filter again for 0's. 0's have been recorded in the errors list
        process_steps.append(self.processing_step_factory(submission,"removed all rows that failed to map rep-to-customer id"))

        submission_id_col_name = "submission_id"
        result[submission_id_col_name] = submission.id
        result = result.loc[:,[submission_id_col_name,map_rep_col_name,"inv_amt","comm_amt"]]

        # update submission attrs
        submission.total_comm += result["comm_amt"].sum()
        process_steps.append(self.processing_step_factory(submission,f"total commissions successfully processed: ${submission.total_comm/100:.2f}"))
        submission.final_comm_data = pd.concat(
            [submission.final_comm_data, result]
        )




