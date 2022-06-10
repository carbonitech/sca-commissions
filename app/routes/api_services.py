"""Collection of domain-level functions to be used by web workers to process API calls in the background"""
import pandas as pd
import sqlalchemy
from sqlalchemy.orm import Session
from sqlalchemy.engine.base import Engine
from app.db import db
from typing import Dict

# mappings
MAPPING_TABLES = {
    "map_customer_name": db.MapCustomerName,
    "map_city_names": db.MapCityName,
    "map_reps_customers": db.MapRepsToCustomer
}
def get_mapping_tables(conn: Engine) -> set:
    return {table for table in sqlalchemy.inspect(conn).get_table_names() if table.split("_")[0] == "map"}

def get_mappings(conn: Engine, table: str) -> pd.DataFrame:    
    return pd.read_sql(sqlalchemy.select(MAPPING_TABLES[table]),conn)

def set_mapping(database: Engine, table: str, data: pd.DataFrame) -> bool:
    rows_affected = data.to_sql(table, con=database, if_exists="append", index=False)
    if rows_affected and rows_affected > 0:
        return True
    else:
        False

def del_mapping(database: Engine, table: str, id: int) -> bool:
    with Session(database) as session:
        row = session.query(MAPPING_TABLES[table]).filter_by(id=id).first()
        session.delete(row)
        session.commit()
    return True


# final commission data
COMMISSION_DATA_TABLE = db.FinalCommissionData
def get_final_data(conn: Engine) -> pd.DataFrame:
    return pd.read_sql(sqlalchemy.select(COMMISSION_DATA_TABLE),conn)

def record_final_data(conn: Engine, data: pd.DataFrame) -> bool:
    rows_affected = data.to_sql(COMMISSION_DATA_TABLE.__table__.name, con=conn, if_exists="append", index=False)
    if rows_affected and rows_affected > 0:
        return True
    else:
        False

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
