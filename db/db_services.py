"""Collection of domain-level functions to be used by web workers to process API calls in the background"""
from typing import Union
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
MANUFACTURERS = models.Manufacturer
REPORTS = models.ManufacturersReport
COMMISSION_DATA_TABLE = models.CommissionData
SUBMISSIONS_TABLE = models.Submission
PROCESS_STEPS_LOG = models.ProcessingStep
ERRORS_TABLE = models.Error
MAPPING_TABLES = {
    "map_customer_names": models.MapCustomerName,
    "map_city_names": models.MapCityName,
    "map_state_names": models.MapStateName
}

load_dotenv()

class DatabaseServices:

    engine = sqlalchemy.create_engine(getenv("DATABASE_URL").replace("postgres://","postgresql://"))

    def get_mappings(self, table: str) -> pd.DataFrame:
        return pd.read_sql(sqlalchemy.select(MAPPING_TABLES[table]),self.engine)

    def get_all_manufacturers(self) -> dict:
        sql = sqlalchemy.select(MANUFACTURERS.id,MANUFACTURERS.name).where(MANUFACTURERS.deleted == None)
        with self.engine.begin() as conn:
            query_result = conn.execute(sql).fetchall()
        return {id_: name_.lower() for id_, name_ in query_result}

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

    def last_step_num(self, submission_id: int) -> int:
        sql = sqlalchemy.select(sqlalchemy.func.max(PROCESS_STEPS_LOG.step_num))\
            .where(PROCESS_STEPS_LOG.submission_id == submission_id)
        with self.engine.begin() as conn:
            result, = conn.execute(sql).one()
        return result


    def record_error(self, error_obj: Error) -> None:
        """record errors into the current_errors table"""
        with Session(bind=self.engine) as session:
            sql = sqlalchemy.insert(ERRORS_TABLE).values(**error_obj)
            session.execute(sql)
            session.commit()
        return

    def delete_errors(self, record_ids: Union[int,list]):
        if isinstance(record_ids, int):
            record_ids = [record_ids]
        with self.engine.begin() as conn:
            for record_id in record_ids:
                record_id = int(record_id)
                sql = sqlalchemy.delete(ERRORS_TABLE).where(ERRORS_TABLE.id == record_id)
                conn.execute(sql)

        
    def get_report_name_by_id(self, report_id: int) -> str:
        sql = sqlalchemy.select(REPORTS.report_name).where(REPORTS.id == report_id)
        with self.engine.begin() as conn:
            result = conn.execute(sql).one_or_none()
        if result:
            return result[0]
        