"""Collection of domain-level functions to be used by web workers to process API calls in the background"""
import json
import calendar
from typing import Tuple
from dotenv import load_dotenv
from os import getenv

import pandas as pd
import sqlalchemy
from sqlalchemy.orm import Session

from app import event
from db import models
from entities.error import Error, ErrorType
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

    def get_branches(self) -> pd.DataFrame:
        sql = sqlalchemy.select(BRANCHES)
        return pd.read_sql(sql,con=self.engine)


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
        sql = sqlalchemy.select(MANUFACTURERS).where(MANUFACTURERS.name==name)
        with Session(bind=self.engine) as session:
            manf_id = session.execute(sql).fetchone()[0].id # returns a list of model instances with attrs - id accessed
        return manf_id

    def get_manufacturers_reports(self, manufacturer_id: int) -> pd.DataFrame:
        result = pd.read_sql(
                    sqlalchemy.select(REPORTS).where(REPORTS.manufacturer_id==manufacturer_id),
                    con=self.engine
                )
        return result

    def get_report_id(self, manufacturer_id: int, report_name: str) -> int:
        sql = sqlalchemy.select(REPORTS.id) \
                        .where(sqlalchemy.and_(
                            REPORTS.manufacturer_id == manufacturer_id,
                            REPORTS.report_name == report_name
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
        result.row_data = result.row_data.apply(lambda json_str: json.loads(json_str))
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


    ## api
    def get_customers(self) -> pd.DataFrame:
        sql = sqlalchemy.select(CUSTOMERS)
        result = pd.read_sql(sql, con=self.engine)
        return result

    def new_customer(self, customer_fastapi: str) -> int:
        with Session(bind=self.engine) as session:
            sql = sqlalchemy.insert(CUSTOMERS).values(name=customer_fastapi) \
                .returning(CUSTOMERS.id)
            new_id = session.execute(sql).fetchone()[0]
            session.commit()
        return new_id

    def get_customer(self,cust_id: int) -> pd.DataFrame:
        sql = sqlalchemy.select(CUSTOMERS) \
                .where(CUSTOMERS.id == cust_id)
        result = pd.read_sql(sql, con=self.engine)
        return result

    def check_customer_exists_by_name(self, name: str) -> bool:
        sql = sqlalchemy.select(CUSTOMERS).where(CUSTOMERS.name == name)
        with Session(bind=self.engine) as session:
            result = session.execute(sql).fetchone()
        return True if result else False

    def modify_customer(self, customer_id: int, *args, **kwargs) -> None:
        sql = sqlalchemy.update(CUSTOMERS).values(**kwargs).where(CUSTOMERS.id == customer_id)
        with Session(bind=self.engine) as session:
            session.execute(sql)
            session.commit()
        return

    def get_branches_by_customer(self, customer_id: int) -> pd.DataFrame:
        branches = BRANCHES
        customers = CUSTOMERS
        cities = CITIES
        states = STATES
        rep_mapping = REPS_CUSTOMERS_MAP
        reps = REPS
        sql = sqlalchemy \
            .select(branches.id,customers.name,cities.name,states.name,reps.initials) \
            .select_from(branches).join(customers).join(cities).join(states).join(rep_mapping).join(reps) \
            .where(customers.id == customer_id)
        # filters for branches with rep mappings per inner joins
        result = pd.read_sql(sql, con=self.engine)
        result.columns = ["id", "Customer Name", "City", "State", "Salesman"]
        # append branches for this customer that don't have a rep assigned and return
        unmapped_branches = self.get_unmapped_branches_by_customer(customer_id=customer_id)
        return pd.concat([result,unmapped_branches])

    def get_unmapped_branches_by_customer(self, customer_id: int) -> pd.DataFrame:
        sql = sqlalchemy.select(BRANCHES.id, CUSTOMERS.name, CITIES.name, STATES.name) \
            .join(CUSTOMERS).join(CITIES).join(STATES).join(REPS_CUSTOMERS_MAP, isouter=True) \
            .where(sqlalchemy.and_(REPS_CUSTOMERS_MAP.id == None, BRANCHES.customer_id==customer_id))
        result = pd.read_sql(sql, con=self.engine)
        result.columns = ["id", "Customer Name", "City", "State"]
        result["Salesman"] = None
        return result

    def delete_a_branch_by_id(self, branch_id: int):
        sql = sqlalchemy.delete(BRANCHES).where(BRANCHES.id == branch_id)
        with Session(bind=self.engine) as session:
            session.execute(sql)
            session.commit()
        return

    def get_all_manufacturers(self) -> pd.DataFrame:
        sql = sqlalchemy.select(MANUFACTURERS)
        result = pd.read_sql(sql, con=self.engine)
        return result

    def get_manufacturer_by_id(self, manuf_id: int) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        manufs = MANUFACTURERS
        sql = sqlalchemy.select(manufs).where(manufs.id == manuf_id)
        submissions = self.get_submissions(manuf_id)
        reports = self.get_manufacturers_reports(manuf_id).drop(columns="manufacturer_id")
        manuf = pd.read_sql(sql, con=self.engine)
        return manuf, reports, submissions

    def get_all_reps(self) -> pd.DataFrame:
        sql = sqlalchemy.select(REPS)
        return pd.read_sql(sql, con=self.engine)

    def get_rep_and_branches(self, rep_id: int) -> pd.DataFrame:
        reps = REPS
        customers = CUSTOMERS
        branches = BRANCHES
        cities = CITIES
        states = STATES
        map_rep_to_customers = REPS_CUSTOMERS_MAP
        sql = sqlalchemy.select(map_rep_to_customers.id,customers.name,cities.name,states.name) \
                .select_from(map_rep_to_customers).join(reps).join(branches).join(customers) \
                .join(cities).join(states).where(map_rep_to_customers.rep_id == rep_id)
        result = pd.read_sql(sql, con=self.engine)
        result.columns = ["id", "Customer", "City", "State"]
        return result

    def get_all_submissions(self) -> pd.DataFrame:
        subs = SUBMISSIONS_TABLE
        reports = REPORTS
        manufs = MANUFACTURERS
        sql = sqlalchemy.select(subs.id,subs.submission_date,subs.reporting_month,subs.reporting_year,
                reports.report_name,reports.yearly_frequency, reports.POS_report,
                manufs.name).select_from(subs).join(reports).join(manufs)
        return pd.read_sql(sql, con=self.engine)

    def get_submission_by_id(self, submission_id: int) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        subs = SUBMISSIONS_TABLE
        reports = REPORTS
        manufs = MANUFACTURERS
        steps = PROCESS_STEPS_LOG
        submission_sql = sqlalchemy.select(subs.id,subs.submission_date,subs.reporting_month,subs.reporting_year,
                reports.report_name,reports.yearly_frequency, reports.POS_report,
                manufs.name).select_from(subs).join(reports).join(manufs).where(subs.id == submission_id)
        process_steps_sql = sqlalchemy.select(steps).where(steps.submission_id == submission_id)
        submission_data = pd.read_sql(submission_sql, con=self.engine)
        process_steps = pd.read_sql(process_steps_sql, con=self.engine)
        current_errors = self.get_errors(submission_id)

        return submission_data, process_steps, current_errors

    def get_customer_branches_raw(self, customer_id: int) -> pd.DataFrame:
        sql = sqlalchemy.select(BRANCHES).where(BRANCHES.customer_id == customer_id)
        return pd.read_sql(sql, con=self.engine)
    
    def set_new_customer_branch_raw(self, customer_id: int, city_id: int, state_id: int):
        sql = sqlalchemy.insert(BRANCHES) \
            .values(customer_id=customer_id,
                    city_id=city_id,
                    state_id=state_id)
        with Session(bind=self.engine) as session:
            session.execute(sql)
            session.commit()
        return

    def get_all_customer_name_mappings(self, customer_id: int=0) -> pd.DataFrame:
        sql = sqlalchemy.select(CUSTOMERS,CUSTOMER_NAME_MAP)\
                .select_from(CUSTOMERS).join(CUSTOMER_NAME_MAP)
        if customer_id:
            sql = sql.where(CUSTOMERS.id == customer_id)

        table = pd.read_sql(sql, con=self.engine)
        table.columns = ["Customer ID", "Customer Name", "Name Mapping ID", "Alias", "_"] 
        return table.iloc[:,:4]

    def get_all_city_name_mappings(self, city_id: int=0) -> pd.DataFrame:
        sql = sqlalchemy.select(CITIES,CITY_NAME_MAP)\
                .select_from(CITIES).join(CITY_NAME_MAP)
        if city_id:
            sql = sql.where(CITIES.id == city_id)

        table = pd.read_sql(sql, con=self.engine)
        table.columns = ["City ID", "City Name", "Name Mapping ID", "Alias", "_"] 
        return table.iloc[:,:4]

    def get_all_state_name_mappings(self, state_id: int=0) -> pd.DataFrame:
        sql = sqlalchemy.select(STATES,STATE_NAME_MAP)\
                .select_from(STATES).join(STATE_NAME_MAP)
        if state_id:
            sql = sql.where(STATES.id == state_id)

        table = pd.read_sql(sql, con=self.engine)
        table.columns = ["State ID", "State Name", "Name Mapping ID", "Alias", "_"] 
        return table.iloc[:,:4]

    def get_cities(self,city_id: int=0) -> pd.DataFrame:
        sql = sqlalchemy.select(CITIES)
        if city_id:
            sql = sql.where(CITIES.id==city_id)
        return pd.read_sql(sql, con=self.engine)

    def get_states(self,state_id: int=0) -> pd.DataFrame:
        sql = sqlalchemy.select(CITIES)
        if state_id:
            sql = sql.where(CITIES.id==state_id)
        return pd.read_sql(sql, con=self.engine)

    def get_rep_to_customer_full(self, customer_id: int) -> pd.DataFrame:
        sql = sqlalchemy.select(
            REPS_CUSTOMERS_MAP.id,
            BRANCHES.id,
            CUSTOMERS.id, CUSTOMERS.name,
            CITIES.id, CITIES.name,
            STATES.id,STATES.name,
            REPS.id, REPS.initials)\
            .select_from(REPS_CUSTOMERS_MAP).join(BRANCHES).join(CUSTOMERS).join(CITIES)\
            .join(STATES).join(REPS).where(CUSTOMERS.id == customer_id)
        table = pd.read_sql(sql, con=self.engine)
        table.columns = ["Rep to Customer ID", "Branch ID", "Customer ID", "Customer",
                "City ID", "City", "State ID", "State", "Rep ID", "Rep"]
        return table
        
    def set_customer_name_mapping(self, **kwargs):
        sql = sqlalchemy.insert(CUSTOMER_NAME_MAP).values(**kwargs)
        with Session(bind=self.engine) as session:
            session.execute(sql)
            session.commit()
        event.post_event("New Mapping Created",{CUSTOMER_NAME_MAP.__table__:kwargs})

    def set_city_name_mapping(self, **kwargs):
        sql = sqlalchemy.insert(CITY_NAME_MAP).values(**kwargs)
        with Session(bind=self.engine) as session:
            session.execute(sql)
            session.commit()
        event.post_event("New Mapping Created",{CITY_NAME_MAP.__table__:kwargs})
 
    def set_state_name_mapping(self, **kwargs):
        sql = sqlalchemy.insert(STATE_NAME_MAP).values(**kwargs)
        with Session(bind=self.engine) as session:
            session.execute(sql)
            session.commit()
        event.post_event("New Mapping Created",{STATE_NAME_MAP.__table__:kwargs})

    def set_rep_to_customer_mapping(self, **kwargs):
        sql = sqlalchemy.insert(REPS_CUSTOMERS_MAP).values(**kwargs)
        with Session(bind=self.engine) as session:
            session.execute(sql)
            session.commit()
        event.post_event("New Mapping Created",{REPS_CUSTOMERS_MAP.__table__:kwargs})

    def update_rep_to_customer_mapping(self, map_id: int, **kwargs):
        sql = sqlalchemy.update(REPS_CUSTOMERS_MAP).values(**kwargs).where(REPS_CUSTOMERS_MAP.id == map_id)
        with Session(bind=self.engine) as session:
            session.execute(sql)
            session.commit()
        event.post_event("Rep Mapping updated",{REPS_CUSTOMERS_MAP.__table__:kwargs})       

    ## references
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
        reports = REPORTS
        manufacturers = MANUFACTURERS
        map_reps_to_customers = REPS_CUSTOMERS_MAP
        reps = REPS
        branches = BRANCHES
        customers = CUSTOMERS
        cities = CITIES
        states = STATES
        sql = sqlalchemy.select(commission_data_raw.row_id,
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
        view_table.columns = ["ID","Year","Month","Manufacturer","Salesman",
                "Customer Name","City","State","Inv Amt","Comm Amt"]
        view_table.loc[:,"Inv Amt"] = view_table.loc[:,"Inv Amt"].apply(self.convert_cents_to_dollars)
        view_table.loc[:,"Comm Amt"] = view_table.loc[:,"Comm Amt"].apply(self.convert_cents_to_dollars)
        view_table.loc[:,"Month"] = view_table.loc[:,"Month"].apply(self.convert_month_from_number_to_name).astype(str)
        return view_table


    def rep_to_customer_map_with_all_names(self) -> pd.DataFrame:
        reps = REPS
        map_reps_to_customers = REPS_CUSTOMERS_MAP
        branches = BRANCHES
        customers = CUSTOMERS
        cities = CITIES
        states = STATES
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

