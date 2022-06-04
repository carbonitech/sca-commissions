"""Collection of domain-level functions to be used by web workers to process API calls in the background"""
import pandas as pd
from app.db import db
from typing import Dict

# mappings
def get_mapping_tables(database: db.Database) -> set:

    with database as db_conn:
        db_conn: db.Database
        all_tables = db_conn.get_tables()

    mapping_tables = {record[0] for record in all_tables if record[0].split("_")[0] == "map"}
    return mapping_tables


def get_mapping(database: db.Database, table: str) -> pd.DataFrame:
    
    with database as db_conn:
        db_conn: db.Database
        mapping_data_columns = db_conn.select_records(table="INFORMATION_SCHEMA.COLUMNS", columns=["column_name"], constraints={"TABLE_NAME": table})
        mapping_data = db_conn.select_records(table=table)

    return pd.DataFrame.from_records(mapping_data, columns=[col[0] for col in mapping_data_columns])


def set_mapping(database: db.Database, table: int, data: pd.DataFrame) -> bool:
    ...

def del_mapping(database: db.Database, table: int, id: int) -> bool:
    ...

# final commission data
def get_final_data(database: db.Database) -> pd.DataFrame:
    ...

def record_final_data(database: db.Database, data: pd.DataFrame) -> bool:
    ...

# submission metadata
def get_submissions_metadata(database: db.Database, manufacturer_id: int) -> pd.DataFrame:
    ...

def del_submission(database: db.Database, submission_id: int) -> bool:
    ...

# original submission files
def get_submission_files(database: db.Database, manufacturer_id: int) -> Dict[int,str]:
    ...

def record_submission_file(database: db.Database, manufactuer_id: int, file: bytes) -> bool:
    ...

def del_submission_file(database: db.Database, id: int) -> bool:
    ...

# processing steps log
def get_processing_steps(database: db.Database, submission_id: int) -> pd.DataFrame:
    ...

def record_processing_steps(database: db.Database, submission_id: int, data: pd.DataFrame) -> bool:
    ...

def del_processing_steps(database: db.Database, submission_id: int) -> bool:
    ...

# errors
def get_errors(database: db.Database, submission_id: int) -> pd.DataFrame:
    ...

def record_errors(database: db.Database, submission_id: int, data: pd.DataFrame) -> bool:
    ...

def correct_error(database: db.Database, error_id: int, data: pd.DataFrame) -> bool:
    ...

def del_error(database: db.Database, error_id: int) -> bool:
    ...


### admin functions will be developed below here, but they are not needed for MVP ###
