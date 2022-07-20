"""Defines manufacturer and submission base classes"""
import os
import dotenv
from typing import Union
from datetime import datetime
from dataclasses import dataclass

import pandas as pd
from sqlalchemy import create_engine

import app.db.db_services as db_serv

dotenv.load_dotenv()

DB_URL = os.getenv("DATABASE_URL")
DB_ENGINE = create_engine(DB_URL)

database = db_serv.DatabaseServices(DB_ENGINE)


class Manufacturer:
    """
    defines manufacturer-specific attributes
    and handles report processing execution

    This is a base class for creating manufacturers
    """

    name: str = None # defined by subclasses, manufacturer's db name

    def __init__(self):
        self.mappings = {table: database.get_mappings(table) for table in database.get_mapping_tables()}
        self.id = database.get_manufacturer_id(self.name)
        self.customer_branches = database.get_customers_branches()
        self.reps_to_cust_branch_ref = database.get_reps_to_cust_branch_ref()
        

    def fill_customer_ids(self, data: pd.DataFrame, column: Union[str,int]) -> pd.DataFrame:

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


    def record_mapping_errors(
            self, data: pd.DataFrame, field: str, reason: str,
            value_type, value_content: str = None):

        for row_index, row_data in data.to_dict("index").items():
            if not value_content:
                value_content = row_data[field]
            error_obj = Error(
                submission_id=self.submission.id,
                row_index=row_index,
                field=field,
                value_type=value_type,
                value_content=0,
                reason=reason,
                row_data={row_index: row_data})

            self.submission.errors.append(error_obj)

    

class Submission:
    """
    handles report processing:
    tracks the state of submission attributes such as id,
    errors, processing steps, etc. to use in post-processing
    database operations

    takes a Manufacturer object in the constructor to access
    manufacturer attributes and report processing procedures
    """
    errors = []
    processing_steps = []
    final_comm_data = None
    total_comm = 0  # tracking cents, not dollars

    def __init__(self, rep_mon: int, rep_year: int, report_id: int, file: bytes) -> None:
        self.file = file
        self.report_month = rep_mon
        self.report_year = rep_year
        self.report_id = report_id
        self.submission_date = datetime.today()
        self.id = database.record_submission_metadata(
                    report_id=self.report_id,
                    data=pd.Series({
                        "submission_date": self.submission_date,
                        "reporting_month": self.report_month,
                        "reporting_year": self.report_year
                    })
                )


@dataclass
class Error:
    submission_id: int
    row_index: int
    field: str
    value_type: type
    value_content: str
    reason: str
    row_data: dict