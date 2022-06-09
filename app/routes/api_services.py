"""Collection of domain-level functions to be used by web workers to process API calls in the background"""
import pandas as pd
from sqlalchemy import select
import sqlalchemy
from sqlalchemy.engine.base import Engine
from app.db import db
from typing import Dict


def get_data(conn: Engine, table: db.Base) -> pd.DataFrame:
    """taking a SQL database and extracting all records. returns a pandas dataframe"""
    return pd.read_sql(select(table),conn)


def set_data(database: Engine, table: str, data: pd.DataFrame) -> bool: ...

# mappings
def get_mapping_tables(conn: Engine) -> set:
    return {table for table in sqlalchemy.inspect(conn).get_table_names() if table.split("_")[0] == "map"}
    
def get_mappings(*args, **kwargs) -> pd.DataFrame:
    return get_data(*args, **kwargs)

def set_mapping(database: Engine, table: str, data: pd.DataFrame) -> bool: ...
def del_mapping(database: Engine, table: str, id: int) -> bool: ...
# final commission data
def record_final_data(database: Engine, data: pd.DataFrame) -> bool: ...
def get_final_data(database: Engine) -> pd.DataFrame: ...
# submission metadata
def get_submissions_metadata(database: Engine, manufacturer_id: int) -> pd.DataFrame: ...
def del_submission(database: Engine, submission_id: int) -> bool: ...
# original submission files
def get_submission_files(database: Engine, manufacturer_id: int) -> Dict[int,str]: ...
def record_submission_file(database: Engine, manufactuer_id: int, file: bytes) -> bool: ...
def del_submission_file(database: Engine, id: int) -> bool: ...
# processing steps log
def get_processing_steps(database: Engine, submission_id: int) -> pd.DataFrame: ...
def record_processing_steps(database: Engine, submission_id: int, data: pd.DataFrame) -> bool: ...
def del_processing_steps(database: Engine, submission_id: int) -> bool: ...
# errors
def get_errors(database: Engine, submission_id: int) -> pd.DataFrame: ...
def record_errors(database: Engine, submission_id: int, data: pd.DataFrame) -> bool: ...
def correct_error(database: Engine, error_id: int, data: pd.DataFrame) -> bool: ...
def del_error(database: Engine, error_id: int) -> bool: ...

### admin functions will be developed below here, but they are not needed for MVP ###
