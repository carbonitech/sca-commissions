"""Collection of domain-level functions to be used by web workers to process API calls in the background"""
import pandas as pd
from app.db import db
from typing import Dict

# mappings
def get_mapping_tables(db: db.Database) -> Dict[int,str]:
    ...

def get_mapping(db: db.Database, table: int) -> pd.DataFrame:
    ...

def set_mapping(db: db.Database, table: int, data: pd.DataFrame) -> bool:
    ...

def del_mapping(db: db.Database, table: int, id: int) -> bool:
    ...

# final commission data
def get_final_data(db: db.Database) -> pd.DataFrame:
    ...

def record_final_data(db: db.Database, data: pd.DataFrame) -> bool:
    ...

# submission metadata
def get_submissions_metadata(db: db.Database, manufacturer_id: int) -> pd.DataFrame:
    ...

def del_submission(db: db.Database, submission_id: int) -> bool:
    ...

# original submission files
def get_submission_files(db: db.Database, manufacturer_id: int) -> Dict[int,str]:
    ...

def record_submission_file(db: db.Database, manufactuer_id: int, file: bytes) -> bool:
    ...

def del_submission_file(db: db.Database, id: int) -> bool:
    ...

# processing steps log
def get_processing_steps(db: db.Database, submission_id: int) -> pd.DataFrame:
    ...

def record_processing_steps(db: db.Database, submission_id: int, data: pd.DataFrame) -> bool:
    ...

def del_processing_steps(db: db.Database, submission_id: int) -> bool:
    ...