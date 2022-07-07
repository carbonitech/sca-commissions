"""Defines manufacturer and submission base classes"""
import os
import dotenv
from dataclasses import dataclass
from sqlalchemy import create_engine
import app.db.db_services as db_serv

dotenv.load_dotenv()

DB_URL = os.getenv("SCA_DATABASE_URL")
DB_ENGINE = create_engine(DB_URL)

database = db_serv.DatabaseServices(DB_ENGINE)


class Manufacturer:
    """
    defines manufacturer-specific attributes
    and handles report processing execution

    This is a base class for creating manufacturers
    """

    name = None

    def __init__(self):
        self.mappings = {table: database.get_mappings(table) for table in database.get_mapping_tables()}
        self.id = database.get_manufacturer_id(self.name)
        
    

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

    def __init__(self, rep_mon: int, rep_year: int, file: bytes) -> None:
        self.file = file
        self.report_month = rep_mon
        self.report_year = rep_year
        self.id = ...


@dataclass
class Error:
    submission_id: int
    row_index: int
    field: str
    value_type: type
    value_content: str
    reason: str
    row_data: dict