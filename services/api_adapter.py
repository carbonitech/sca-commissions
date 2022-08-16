from datetime import datetime
from typing import Tuple
from dotenv import load_dotenv
from os import getenv
import json

import pandas as pd
import sqlalchemy
from sqlalchemy.orm import Session

from app import event
from db import models

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

class ApiAdapter:

    engine = sqlalchemy.create_engine(getenv("DATABASE_URL"))

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
        table.columns = ["Customer ID", "Customer Name", "_", "Name Mapping ID", "Alias", "_"] # deleted, customer_id
        return table.loc[:,~table.columns.isin(["_"])]

    def get_all_city_name_mappings(self, city_id: int=0) -> pd.DataFrame:
        sql = sqlalchemy.select(CITIES,CITY_NAME_MAP)\
                .select_from(CITIES).join(CITY_NAME_MAP)
        if city_id:
            sql = sql.where(CITIES.id == city_id)

        table = pd.read_sql(sql, con=self.engine)
        table.columns = ["Customer ID", "Customer Name", "_", "Name Mapping ID", "Alias", "_"] # deleted, customer_id
        return table.loc[:,~table.columns.isin(["_"])]

    def get_all_state_name_mappings(self, state_id: int=0) -> pd.DataFrame:
        sql = sqlalchemy.select(STATES,STATE_NAME_MAP)\
                .select_from(STATES).join(STATE_NAME_MAP)
        if state_id:
            sql = sql.where(STATES.id == state_id)

        table = pd.read_sql(sql, con=self.engine)
        table.columns = ["Customer ID", "Customer Name", "_", "Name Mapping ID", "Alias", "_"] # deleted, customer_id
        return table.loc[:,~table.columns.isin(["_"])]

    def get_cities(self,city_id: int=0) -> pd.DataFrame:
        sql = sqlalchemy.select(CITIES)
        if city_id:
            sql = sql.where(CITIES.id==city_id)
        return pd.read_sql(sql, con=self.engine)

    def new_city(self,**kwargs):
        sql = sqlalchemy.insert(CITIES) \
            .values(**kwargs)
        with Session(bind=self.engine) as session:
            session.execute(sql)
            session.commit()
        event.post_event("New City Created", {CITIES:kwargs})

    def modify_city(self, city_id:int, **kwargs):
        sql = sqlalchemy.update(CITIES).values(**kwargs) \
            .where(CITIES.id == city_id)
        with Session(bind=self.engine) as session:
            session.execute(sql)
            session.commit()
        event.post_event("City Modified", {CITIES:kwargs})

    def delete_city_by_id(self, city_id: int):
        sql = sqlalchemy.update(CITIES).values(deleted=datetime.now().isoformat())\
            .where(CITIES.id == city_id)
        with self.engine.begin() as conn:
            conn.execute(sql)

    def get_states(self,state_id: int=0) -> pd.DataFrame:
        sql = sqlalchemy.select(CITIES)
        if state_id:
            sql = sql.where(CITIES.id==state_id)
        return pd.read_sql(sql, con=self.engine)

    def new_state(self,**kwargs):
        sql = sqlalchemy.insert(STATES) \
            .values(**kwargs)
        with Session(bind=self.engine) as session:
            session.execute(sql)
            session.commit()
        event.post_event("New City Created", {STATES:kwargs})

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
        event.post_event("New Mapping Created",{CUSTOMER_NAME_MAP:kwargs})

    def set_city_name_mapping(self, **kwargs):
        sql = sqlalchemy.insert(CITY_NAME_MAP).values(**kwargs)
        with Session(bind=self.engine) as session:
            session.execute(sql)
            session.commit()
        event.post_event("New Mapping Created",{CITY_NAME_MAP:kwargs})
 
    def set_state_name_mapping(self, **kwargs):
        sql = sqlalchemy.insert(STATE_NAME_MAP).values(**kwargs)
        with Session(bind=self.engine) as session:
            session.execute(sql)
            session.commit()
        event.post_event("New Mapping Created",{STATE_NAME_MAP:kwargs})

    def set_rep_to_customer_mapping(self, **kwargs):
        sql = sqlalchemy.insert(REPS_CUSTOMERS_MAP).values(**kwargs)
        with Session(bind=self.engine) as session:
            session.execute(sql)
            session.commit()
        event.post_event("New Mapping Created",{REPS_CUSTOMERS_MAP:kwargs})

    def update_rep_to_customer_mapping(self, map_id: int, **kwargs):
        sql = sqlalchemy.update(REPS_CUSTOMERS_MAP).values(**kwargs).where(REPS_CUSTOMERS_MAP.id == map_id)
        with Session(bind=self.engine) as session:
            session.execute(sql)
            session.commit()
        event.post_event("Rep Mapping updated",{REPS_CUSTOMERS_MAP:kwargs})

    def get_processing_steps(self, submission_id: int) -> pd.DataFrame:
        """get all report processing steps for a commission report submission"""
        result = pd.read_sql(
            sqlalchemy.select(PROCESS_STEPS_LOG).where(PROCESS_STEPS_LOG.submission_id == submission_id),
            con=self.engine
        )
        return result

    def get_errors(self, submission_id: int) -> pd.DataFrame:
        """get all report processing errors for a commission report submission"""
        sql = sqlalchemy.select(ERRORS_TABLE).where(ERRORS_TABLE.submission_id == submission_id)
        result = pd.read_sql(sql, con=self.engine)
        result.loc['row_data'] = result.loc['row_data'].apply(lambda json_str: json.loads(json_str))
        return result