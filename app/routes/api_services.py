"""Collection of domain-level functions to be used by web workers to process API calls in the background"""
import pandas as pd
import sqlalchemy
from sqlalchemy.orm import Session
from sqlalchemy.engine.base import Engine
from app.db import db
from typing import Dict, Union

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

# manufactuers tables
MANUFACTURER_TABLES = {
    "manufacturers": db.Manufacturer,
    "manufacturers_reports": db.ManufacturersReport
}
def get_manufacturers(conn: Engine, id: Union[int, None]=None): ...
def get_manufacturers_reports(conn: Engine, manufacturer_id: int) -> pd.DataFrame:
    table = "manufacturers_reports"
    table_obj = MANUFACTURER_TABLES[table]
    result = pd.read_sql(sqlalchemy.select(table_obj).where(table_obj.manufacturer_id==manufacturer_id), con=conn)
    return result

# submission metadata
SUBMISSIONS_META_TABLE = db.ReportSubmissionsLog
def get_submissions_metadata(conn: Engine, manufacturer_id: int) -> pd.DataFrame:
    """get a dataframe of all manufacturer's report submissions by manufacturer's id"""
    manufacturers_reports = get_manufacturers_reports(conn,manufacturer_id)
    report_ids = manufacturers_reports.loc[:,"id"].tolist()
    result = pd.read_sql(
        sqlalchemy.select(SUBMISSIONS_META_TABLE).where(SUBMISSIONS_META_TABLE.report_id.in_(report_ids)),
        con=conn)
    return result

def del_submission(database: Engine, submission_id: int) -> bool: ...


# original submission files
def get_submission_files(database: Engine, manufacturer_id: int) -> Dict[int,str]: ...
def record_submission_file(database: Engine, manufactuer_id: int, file: bytes) -> bool: ...
def del_submission_file(database: Engine, id: int) -> bool: ...


# processing steps log
PROCESS_STEPS_LOG = db.ReportProcessingStepsLog
def get_processing_steps(conn: Engine, submission_id: int) -> pd.DataFrame:
    """get all report processing steps for a commission report submission"""
    result = pd.read_sql(
        sqlalchemy.select(PROCESS_STEPS_LOG).where(PROCESS_STEPS_LOG.submission_id == submission_id),
        con=conn
    )
    return result

def record_processing_steps(engine: Engine, submission_id: int, data: pd.DataFrame) -> bool:
    """commit all report processing stesp for a commission report submission"""
    
    data["submission_id"] = submission_id
    records = data.to_dict("records")
    sql = sqlalchemy.insert(PROCESS_STEPS_LOG)
    with Session(bind=engine) as session:
        session.execute(sql,records)
        session.commit()
    return True

def del_processing_steps(database: Engine, submission_id: int) -> bool: ...

# errors
def get_errors(database: Engine, submission_id: int) -> pd.DataFrame: ...
def record_errors(database: Engine, submission_id: int, data: pd.DataFrame) -> bool: ...
def correct_error(database: Engine, error_id: int, data: pd.DataFrame) -> bool: ...
def del_error(database: Engine, error_id: int) -> bool: ...


### admin functions will be developed below here, but they are not needed for MVP ###
