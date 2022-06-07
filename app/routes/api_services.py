"""Collection of domain-level functions to be used by web workers to process API calls in the background"""
import pandas as pd
from app.db import db
from typing import Dict


def get_data(database: db.SQLDatabase, table: str, columns: list = None) -> pd.DataFrame:
    """taking a SQL database and extracting all records. returns a pandas dataframe"""

    with database as db_conn:
        db_conn: db.SQLDatabase
        data = db_conn.select_records(table=table, columns = columns if columns else ['*'])

    return pd.DataFrame.from_records(data["records"], columns=data["columns"])


def set_data(database: db.SQLDatabase, table: str, data: pd.DataFrame) -> bool:
    try:    
        entries = data.to_dict(orient="records")

        with database as db_conn:
            db_conn: db.SQLDatabase
            for entry in entries:
                db_conn.create_record(table=table, data=entry)
    except:            ## <-- TODO: expand to handle errors unqiuely instead of simply returning False
        return False
    else:
        return True


# mappings
def get_mapping_tables(database: db.SQLDatabase) -> set:

    with database as db_conn:
        db_conn: db.SQLDatabase
        all_tables = db_conn.get_tables()

    mapping_tables = {record[0] for record in all_tables if record[0].split("_")[0] == "map"}
    return mapping_tables


def get_mappings(database: db.SQLDatabase, table: str) -> pd.DataFrame:
    return get_data(database=database, table=table)


def set_mapping(database: db.SQLDatabase, table: str, data: pd.DataFrame) -> bool:
    return set_data(database=database, table=table, data=data)


def del_mapping(database: db.SQLDatabase, table: str, id: int) -> bool:
    
    with database as db_conn:
        db_conn: db.SQLDatabase
        db_conn.delete_record(table=table,id=id)

    return True


# final commission data
def record_final_data(database: db.SQLDatabase, data: pd.DataFrame) -> bool:
    table = "final_commission_data"
    return set_data(database=database, table=table, data=data)


def get_final_data(database: db.SQLDatabase) -> pd.DataFrame:
    table = "final_commission_data"
    return get_data(database=database, table=table)
        

# submission metadata
def get_submissions_metadata(database: db.SQLDatabase, manufacturer_id: int) -> pd.DataFrame: ...
def del_submission(database: db.SQLDatabase, submission_id: int) -> bool: ...
# original submission files
def get_submission_files(database: db.SQLDatabase, manufacturer_id: int) -> Dict[int,str]: ...
def record_submission_file(database: db.SQLDatabase, manufactuer_id: int, file: bytes) -> bool: ...
def del_submission_file(database: db.SQLDatabase, id: int) -> bool: ...
# processing steps log
def get_processing_steps(database: db.SQLDatabase, submission_id: int) -> pd.DataFrame: ...
def record_processing_steps(database: db.SQLDatabase, submission_id: int, data: pd.DataFrame) -> bool: ...
def del_processing_steps(database: db.SQLDatabase, submission_id: int) -> bool: ...
# errors
def get_errors(database: db.SQLDatabase, submission_id: int) -> pd.DataFrame: ...
def record_errors(database: db.SQLDatabase, submission_id: int, data: pd.DataFrame) -> bool: ...
def correct_error(database: db.SQLDatabase, error_id: int, data: pd.DataFrame) -> bool: ...
def del_error(database: db.SQLDatabase, error_id: int) -> bool: ...

### admin functions will be developed below here, but they are not needed for MVP ###
