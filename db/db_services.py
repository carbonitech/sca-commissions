"""Collection of domain-level functions to be used by web workers to process API calls in the background"""
from dotenv import load_dotenv
from os import getenv

import pandas as pd
import sqlalchemy
from sqlalchemy.orm import Session

from db import models
from entities.error import Error
from entities.submission import NewSubmission
from entities.processing_step import ProcessingStep

CUSTOMERS = models.Customer
BRANCHES = models.CustomerBranch
CITIES = models.City
STATES = models.State
REPS = models.Representative
CUSTOMER_NAME_MAP = models.MapCustomerName
CITY_NAME_MAP = models.MapCityName
STATE_NAME_MAP = models.MapStateName
REPS_CUSTOMERS_MAP = models.MapRepToCustomer
MANUFACTURERS = models.ManufacturerDTO
REPORTS = models.ManufacturersReport
COMMISSION_DATA_TABLE = models.FinalCommissionDataDTO
SUBMISSIONS_TABLE = models.SubmissionDTO
PROCESS_STEPS_LOG = models.ProcessingStepDTO
ERRORS_TABLE = models.ErrorDTO
MAPPING_TABLES = {
    "map_customer_name": models.MapCustomerName,
    "map_city_names": models.MapCityName,
    "map_reps_customers": models.MapRepToCustomer,
    "map_state_names": models.MapStateName
}

load_dotenv()

class DatabaseServices:

    engine = sqlalchemy.create_engine(getenv("DATABASE_URL"))

    def get_mappings(self, table: str) -> pd.DataFrame:
        return pd.read_sql(sqlalchemy.select(MAPPING_TABLES[table]),self.engine)

    def get_branches(self) -> pd.DataFrame:
        sql = sqlalchemy.select(BRANCHES)
        return pd.read_sql(sql,con=self.engine)

    def record_final_data(self, data: pd.DataFrame) -> None:
        data_records = data.to_dict(orient="records")
        with Session(bind=self.engine) as session:
            sql = sqlalchemy.insert(COMMISSION_DATA_TABLE)
            session.execute(sql, data_records) # for bulk insert per SQLAlchemy docs
            session.commit()
        return

    def record_submission(self, submission: NewSubmission) -> int:
        sql = sqlalchemy.insert(SUBMISSIONS_TABLE).returning(SUBMISSIONS_TABLE.id)\
                .values(**submission)
        with Session(bind=self.engine) as session:
            result = session.execute(sql)
            session.commit()
        return result.fetchone()[0]


    def record_processing_step(self, step_obj: ProcessingStep) -> bool:
        """commit all report processing stesp for a commission report submission"""
        sql = sqlalchemy.insert(PROCESS_STEPS_LOG).values(**step_obj)
        with Session(bind=self.engine) as session:
            session.execute(sql)
            session.commit()
        return True


    def record_error(self, error_obj: Error) -> None:
        """record errors into the current_errors table"""
        with Session(bind=self.engine) as session:
            sql = sqlalchemy.insert(ERRORS_TABLE).values(**error_obj)
            session.execute(sql)
            session.commit()
        return
        
    def get_reps_to_cust_branch_ref(self) -> pd.DataFrame:
        """generates a reference for matching the map_rep_customer id to
        an array of customer, city, and state ids"""
        branches = BRANCHES
        rep_mapping = REPS_CUSTOMERS_MAP
        sql = sqlalchemy \
            .select(rep_mapping.id, branches.customer_id,
                branches.city_id, branches.state_id) \
            .select_from(rep_mapping) \
            .join(branches) 

        result = pd.read_sql(sql, con=self.engine)
        result.columns = ["map_rep_customer_id", "customer_id", "city_id", 
                "state_id"]
        
        return result
