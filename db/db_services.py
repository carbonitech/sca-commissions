"""Collection of domain-level functions to be used by web workers to process API calls in the background"""
from typing import Dict
from os import getenv
import calendar

from dotenv import load_dotenv
import pandas as pd
import sqlalchemy
from sqlalchemy.orm import Session

from db import models
from entities.error import Error, ErrorType
from entities.submission import NewSubmission
from entities.processing_step import ProcessingStep

CUSTOMERS = {
    'customers': models.Customer,
    'customer_branches': models.CustomerBranch
}
LOCATIONS = {
    'cities': models.City,
    'states': models.State
}
REPS = models.Representative
MAPPING_TABLES = {
    "map_customer_name": models.MapCustomerName,
    "map_city_names": models.MapCityName,
    "map_reps_customers": models.MapRepToCustomer,
    "map_state_names": models.MapStateName
}
MANUFACTURER_TABLES = {
    "manufacturers": models.ManufacturerDTO,
    "manufacturers_reports": models.ManufacturersReport
}
COMMISSION_DATA_TABLE = models.FinalCommissionDataDTO
SUBMISSIONS_TABLE = models.SubmissionDTO
PROCESS_STEPS_LOG = models.ProcessingStepDTO
ERRORS_TABLE = models.ErrorDTO

load_dotenv()

class DatabaseServices:

    engine = sqlalchemy.create_engine(getenv("DATABASE_URL"))

    ## mappings
    def get_mapping_tables(self) -> set:
        return {table for table in sqlalchemy.inspect(self.engine).get_table_names() 
                if table.split("_")[0] == "map"}

    def get_mappings(self, table: str) -> pd.DataFrame:
        return pd.read_sql(sqlalchemy.select(MAPPING_TABLES[table]),self.engine)

    def set_mapping(self, table: str, data: pd.DataFrame) -> bool:

        col_list = data.columns.tolist()

        # make sure all text is capitalized
        for column in col_list:
            if data[column].dtype != 'object':
                continue
            data[column] = data.loc[:,column].apply(str.upper)

        # check for duplication and proceed with de-dupped data
        current_table = self.get_mappings(table)
        merged = pd.merge(data, current_table, how="left", on=col_list, indicator=True)
        uniques = merged[merged["_merge"]=="left_only"].loc[:,col_list]

        # execute if there's still data to commit and return a bool based on what happens
        if len(uniques) > 0:
            rows_affected = uniques.to_sql(table, con=self.engine, if_exists="append", index=False)

            if rows_affected:
                return True
            else:
                return False
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

    def record_final_data(self, data: pd.DataFrame) -> None:
        data_records = data.to_dict(orient="records")
        with Session(bind=self.engine) as session:
            sql = sqlalchemy.insert(COMMISSION_DATA_TABLE)
            session.execute(sql, data_records) # for bulk insert per SQLAlchemy docs
            session.commit()
        return


    ## manufactuers tables
    def get_manufacturer_id(self, name: str) -> int:
        table = 'manufacturers'
        table_obj = MANUFACTURER_TABLES[table]
        sql = sqlalchemy.select(table_obj).where(table_obj.name==name)
        with Session(bind=self.engine) as session:
            manf_id = session.execute(sql).fetchone()[0].id # returns a list of model instances with attrs - id accessed
        return manf_id

    def get_manufacturers_reports(self, manufacturer_id: int) -> pd.DataFrame:
        table = "manufacturers_reports"
        table_obj = MANUFACTURER_TABLES[table]
        result = pd.read_sql(
                    sqlalchemy.select(table_obj).where(table_obj.manufacturer_id==manufacturer_id),
                    con=self.engine
                )
        return result

    def get_report_id(self, manufacturer_id: int, report_name: str) -> int:
        table = "manufacturers_reports"
        table_obj = MANUFACTURER_TABLES[table]
        sql = sqlalchemy.select(table_obj.id) \
                        .where(sqlalchemy.and_(
                            table_obj.manufacturer_id == manufacturer_id,
                            table_obj.report_name == report_name
                        ))
        with Session(bind=self.engine) as session:
            report_id = session.execute(sql).fetchone()[0]
        
        return report_id


    ## submission
    def get_submissions(self, manufacturer_id: int) -> pd.DataFrame:
        """get a dataframe of all manufacturer's report submissions by manufacturer's id"""
        manufacturers_reports = self.get_manufacturers_reports(manufacturer_id)
        report_ids = manufacturers_reports.loc[:,"id"].tolist()
        result = pd.read_sql(
            sqlalchemy.select(SUBMISSIONS_TABLE).where(SUBMISSIONS_TABLE.report_id.in_(report_ids)),
            con=self.engine)
        return result

    def record_submission(self, submission: NewSubmission) -> int:
        sql = sqlalchemy.insert(SUBMISSIONS_TABLE).returning(SUBMISSIONS_TABLE.id)\
                .values(**submission)
        with Session(bind=self.engine) as session:
            result = session.execute(sql)
            session.commit()

        return result.fetchone()[0]

    def del_submission(self, submission_id: int) -> bool:
        """delete a submission using the submission id"""
        sql = sqlalchemy.delete(SUBMISSIONS_TABLE)\
                .where(SUBMISSIONS_TABLE.id == submission_id)

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

    def record_processing_step(self, step_obj: ProcessingStep) -> bool:
        """commit all report processing stesp for a commission report submission"""
        sql = sqlalchemy.insert(PROCESS_STEPS_LOG).values(**step_obj)
        with Session(bind=self.engine) as session:
            session.execute(sql)
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

    def record_error(self, error_obj: Error) -> None:
        """record errors into the current_errors table"""
        with Session(bind=self.engine) as session:
            sql = sqlalchemy.insert(ERRORS_TABLE).values(**error_obj)
            session.execute(sql)
            session.commit()
        return
        
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


    ## references
    def get_customers_branches(self) -> pd.DataFrame:
        sql = sqlalchemy.select(CUSTOMERS["customer_branches"])
        result = pd.read_sql(sql, con=self.engine)
        return result

    def get_reps_to_cust_branch_ref(self) -> pd.DataFrame:
        """generates a reference for matching the map_rep_customer id to
        an array of customer, city, and state ids"""
        branches = CUSTOMERS["customer_branches"]
        rep_mapping = MAPPING_TABLES["map_reps_customers"]
        sql = sqlalchemy \
            .select(rep_mapping.id, branches.customer_id,
                branches.city_id, branches.state_id) \
            .select_from(rep_mapping) \
            .join(branches) \

        result = pd.read_sql(sql, con=self.engine)
        result.columns = ["map_rep_customer_id", "customer_id", "city_id", 
                "state_id"]
        
        return result


class TableViews:

    engine = sqlalchemy.create_engine(getenv("DATABASE_URL"))

    @staticmethod
    def convert_cents_to_dollars(cent_amt: float) -> float:
        return round(cent_amt/100,2)

    @staticmethod
    def convert_month_from_number_to_name(month_num: int) -> str:
        return calendar.month_name[month_num]

    def commission_data_with_all_names(self) -> pd.DataFrame:
        """runs sql query to produce the commission table format used by SCA
        and converts month number to name and cents to dollars before return
        
        Returns: pd.DataFrame"""
        commission_data_raw = COMMISSION_DATA_TABLE
        submission_data = SUBMISSIONS_TABLE
        reports = MANUFACTURER_TABLES["manufacturers_reports"]
        manufacturers = MANUFACTURER_TABLES["manufacturers"]
        map_reps_to_customers = MAPPING_TABLES["map_reps_customers"]
        reps = REPS
        branches = CUSTOMERS["customer_branches"]
        customers = CUSTOMERS["customers"]
        cities = LOCATIONS["cities"]
        states = LOCATIONS["states"]
        sql = sqlalchemy.select(
            submission_data.reporting_year, submission_data.reporting_month,
            manufacturers.name, reps.initials, customers.name,
            cities.name, states.name, commission_data_raw.inv_amt,
            commission_data_raw.comm_amt
            ).select_from(commission_data_raw) \
            .join(submission_data)             \
            .join(reports)                     \
            .join(manufacturers)               \
            .join(map_reps_to_customers)       \
            .join(reps)                        \
            .join(branches)                    \
            .join(customers)                   \
            .join(cities)                      \
            .join(states)

        view_table = pd.read_sql(sql, con=self.engine)
        view_table.columns = ["Year","Month","Manufacturer","Salesman",
                "Customer Name","City","State","Inv Amt","Comm Amt"]
        view_table.loc[:,"Inv Amt"] = view_table.loc[:,"Inv Amt"].apply(self.convert_cents_to_dollars)
        view_table.loc[:,"Comm Amt"] = view_table.loc[:,"Comm Amt"].apply(self.convert_cents_to_dollars)
        view_table.loc[:,"Month"] = view_table.loc[:,"Month"].apply(self.convert_month_from_number_to_name).astype(str)
        return view_table


    def rep_to_customer_map_with_all_names(self) -> pd.DataFrame:
        reps = REPS
        map_reps_to_customers = MAPPING_TABLES["map_reps_customers"]
        branches = CUSTOMERS["customer_branches"]
        customers = CUSTOMERS["customers"]
        cities = LOCATIONS["cities"]
        states = LOCATIONS["states"]
        sql = sqlalchemy.select(
            reps.initials, customers.name,
            cities.name, states.name
            ).select_from(map_reps_to_customers) \
            .join(reps)                          \
            .join(branches)                      \
            .join(customers)                     \
            .join(cities)                        \
            .join(states)
        view_table = pd.read_sql(sql, con=self.engine)
        view_table.columns = ["Salesman","Customer","City","State"]
        return view_table

    def mapping_errors_view(self) -> pd.DataFrame:

        sql = sqlalchemy.select(ERRORS_TABLE)
        view_table = pd.read_sql(sql, con=self.engine)
        view_table.loc[:,"reason"] = view_table["reason"].apply(lambda enum_val: ErrorType(enum_val).name).astype(str)
        return view_table

