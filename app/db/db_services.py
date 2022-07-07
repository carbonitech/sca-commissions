"""Collection of domain-level functions to be used by web workers to process API calls in the background"""
import pandas as pd
import sqlalchemy
from sqlalchemy.orm import Session
from sqlalchemy.engine.base import Engine
from app.db import models
from typing import Dict, Union

MAPPING_TABLES = {
    "map_customer_name": models.MapCustomerName,
    "map_city_names": models.MapCityName,
    "map_reps_customers": models.MapRepsToCustomer
}
MANUFACTURER_TABLES = {
    "manufacturers": models.Manufacturer,
    "manufacturers_reports": models.ManufacturersReport
}
COMMISSION_DATA_TABLE = models.FinalCommissionData
SUBMISSIONS_META_TABLE = models.ReportSubmissionsLog
PROCESS_STEPS_LOG = models.ReportProcessingStepsLog
ERRORS_TABLE = models.CurrentError


class DatabaseServices:

    def __init__(self, engine: Engine):
        self.engine = engine

    ## mappings
    def get_mapping_tables(self) -> set:
        return {table for table in sqlalchemy.inspect(self.engine).get_table_names() if table.split("_")[0] == "map"}

    def get_mappings(self, table: str) -> pd.DataFrame:
        return pd.read_sql(sqlalchemy.select(MAPPING_TABLES[table]),self.engine)

    def set_mapping(self, table: str, data: pd.DataFrame) -> bool:
        rows_affected = data.to_sql(table, con=self.engine, if_exists="append", index=False)
        if rows_affected and rows_affected > 0:
            return True
        else:
            False

    def del_mapping(self, table: str, id: int) -> bool:
        with Session(self.engine) as session:
            row = session.query(MAPPING_TABLES[table]).filter_by(id=id).first()
            session.delete(row)
            session.commit()
        return True


    ## final commission data
    def get_final_data(self) -> pd.DataFrame:
        return pd.read_sql(sqlalchemy.select(COMMISSION_DATA_TABLE),self.engine)

    def record_final_data(self, data: pd.DataFrame) -> bool:
        rows_affected = data.to_sql(COMMISSION_DATA_TABLE.__table__.name, con=self.engine, if_exists="append", index=False)
        if rows_affected and rows_affected > 0:
            return True
        else:
            False


    ## manufactuers tables
    def get_manufacturer_id(self, name: str) -> int:
        table = 'manufacturers'
        table_obj = MANUFACTURER_TABLES[table]
        sql = sqlalchemy.select(table_obj).where(table_obj.name==name)
        with Session(bind=self.engine) as session:
            manf_id = session.execute(sql).fetchone()[0]
        return manf_id

    def get_manufacturers_reports(self, manufacturer_id: int) -> pd.DataFrame:
        table = "manufacturers_reports"
        table_obj = MANUFACTURER_TABLES[table]
        result = pd.read_sql(sqlalchemy.select(table_obj).where(table_obj.manufacturer_id==manufacturer_id), con=self.engine)
        return result


    ## submission metadata
    def get_submissions_metadata(self, manufacturer_id: int) -> pd.DataFrame:
        """get a dataframe of all manufacturer's report submissions by manufacturer's id"""
        manufacturers_reports = self.get_manufacturers_reports(manufacturer_id)
        report_ids = manufacturers_reports.loc[:,"id"].tolist()
        result = pd.read_sql(
            sqlalchemy.select(SUBMISSIONS_META_TABLE).where(SUBMISSIONS_META_TABLE.report_id.in_(report_ids)),
            con=self.engine)
        return result

    def record_submission_metadata(self, report_id: int, data: pd.Series) -> int:
        """record a submission into the submissions log"""
        data = pd.concat([data,pd.Series({"report_id": report_id})])
        sql = sqlalchemy.insert(SUBMISSIONS_META_TABLE).returning(SUBMISSIONS_META_TABLE.id)\
                .values(data.to_dict())
        with Session(bind=self.engine) as session:
            result = session.execute(sql)
            session.commit()

        return result.fetchone()[0]

    def del_submission(self, submission_id: int) -> bool:
        """delete a submission using the submission id"""
        sql = sqlalchemy.delete(SUBMISSIONS_META_TABLE)\
                .where(SUBMISSIONS_META_TABLE.id == submission_id)

        with Session(bind=self.engine) as session:
            session.execute(sql)
            session.commit()

        return True

    ## processing steps log
    def get_processing_steps(self, submission_id: int) -> pd.DataFrame:
        """get all report processing steps for a commission report submission"""
        result = pd.read_sql(
            sqlalchemy.select(PROCESS_STEPS_LOG).where(PROCESS_STEPS_LOG.submission_id == submission_id),
            con=self.engine
        )
        return result

    def record_processing_steps(self, submission_id: int, data: pd.DataFrame) -> bool:
        """commit all report processing stesp for a commission report submission"""
        data_copy = data.copy()
        data_copy["submission_id"] = submission_id
        records = data_copy.to_dict("records")
        sql = sqlalchemy.insert(PROCESS_STEPS_LOG)
        with Session(bind=self.engine) as session:
            session.execute(sql,records)
            session.commit()
        return True

    def del_processing_steps(self, submission_id: int) -> bool:
        """delete processing steps entires by submission id"""
        sql = sqlalchemy.delete(PROCESS_STEPS_LOG).where(PROCESS_STEPS_LOG.submission_id == submission_id)
        with Session(bind=self.engine) as session:
            session.execute(sql)
            session.commit()
        return True


    ## errors
    def get_errors(self, submission_id: int) -> pd.DataFrame:
        """get all report processing errors for a commission report submission"""
        sql = sqlalchemy.select(ERRORS_TABLE).where(ERRORS_TABLE.submission_id == submission_id)
        result = pd.read_sql(sql, con=self.engine)
        return result

    def record_errors(self, submission_id: int, data: pd.DataFrame) -> bool:
        """record errors into the current_errors table"""
        data_copy = data.copy()
        data_copy["submission_id"] = submission_id
        records = data_copy.to_dict("records")
        with Session(bind=self.engine) as session:
            sql = sqlalchemy.insert(ERRORS_TABLE)
            session.execute(sql, records)
            session.commit()
        return True
        
    def del_error(self, error_id: int) -> bool:
        """delete errors from the errors table by error id"""
        sql = sqlalchemy.delete(ERRORS_TABLE).where(ERRORS_TABLE.id == error_id)
        with Session(bind=self.engine) as session:
            session.execute(sql)
            session.commit()
        return True

    # original submission files
    def get_submission_files(self, manufacturer_id: int) -> Dict[int,str]: ...
    def record_submission_file(self, manufactuer_id: int, file: bytes) -> bool: ...
    def del_submission_file(self, id: int) -> bool: ...
    ### admin functions will be developed below here, but they are not needed for MVP ###
