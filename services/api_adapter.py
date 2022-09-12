from datetime import datetime
from typing import Tuple
import calendar
from dotenv import load_dotenv
from os import getenv
import json

import pandas as pd
import sqlalchemy
from sqlalchemy.orm import Session, sessionmaker

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

def hyphenate_name(table_name: str) -> str:
    return table_name.replace("_","-")

class ApiAdapter:

    engine = sqlalchemy.create_engine(getenv("DATABASE_URL").replace("postgres://","postgresql://"))
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

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
        kwargs = {"id": new_id, "name": customer_fastapi}
        event.post_event("New Record", CUSTOMERS, **kwargs)
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
        event.post_event("Record Updated", CUSTOMERS, customer_id, **kwargs)
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
        sql = sqlalchemy.update(BRANCHES) \
            .values(deleted = datetime.now()) \
            .where(BRANCHES.id == branch_id)
        with Session(bind=self.engine) as session:
            session.execute(sql)
            session.commit()
        return

    def get_all_manufacturers(self) -> pd.DataFrame:
        sql = sqlalchemy.select(MANUFACTURERS)
        result = pd.read_sql(sql, con=self.engine)
        return result

    def get_manufacturer_by_id(self, manuf_id: int):
        manuf_sql = sqlalchemy.select(MANUFACTURERS).where(MANUFACTURERS.id == manuf_id)
        submissions_sql = sqlalchemy.select(SUBMISSIONS_TABLE)\
            .join(REPORTS).where(REPORTS.manufacturer_id == manuf_id)
        manuf = pd.read_sql(manuf_sql, con=self.engine)
        submissions = pd.read_sql(submissions_sql, con=self.engine)
        return manuf, submissions

    def get_all_reps(self) -> pd.DataFrame:
        sql = sqlalchemy.select(REPS)
        return pd.read_sql(sql, con=self.engine)

    def get_a_rep(self, rep_id: int) -> pd.DataFrame:
        sql = sqlalchemy.select(REPS).where(REPS.id == rep_id)
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
                reports.id.label("report_id"),reports.report_name,reports.yearly_frequency, reports.POS_report,
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

    def submission_exists(self, submission_id: int) -> bool:
        sql = sqlalchemy.select(SUBMISSIONS_TABLE).where(SUBMISSIONS_TABLE.id == submission_id)
        with self.engine.begin() as conn:
            result = conn.execute(sql).fetchone()
        return True if result else False

    def get_customer_branches_raw(self, customer_id: int) -> pd.DataFrame:
        sql = sqlalchemy.select(BRANCHES).where(BRANCHES.customer_id == customer_id)
        return pd.read_sql(sql, con=self.engine)
    
    def set_new_customer_branch_raw(self, customer_id: int, city_id: int, state_id: int):
        values = {
            "customer_id": customer_id,
            "city_id": city_id,
            "state_id": state_id
        }
        sql = sqlalchemy.insert(BRANCHES).values(**values)
        with Session(bind=self.engine) as session:
            session.execute(sql)
            session.commit()
        event.post_event("New Record", BRANCHES, **values)
        return

    def get_all_customer_name_mappings(self, customer_id: int=0) -> pd.DataFrame:
        sql = sqlalchemy.select(CUSTOMERS,CUSTOMER_NAME_MAP)\
                .select_from(CUSTOMERS).join(CUSTOMER_NAME_MAP)
        if customer_id:
            sql = sql.where(CUSTOMERS.id == customer_id)

        table = pd.read_sql(sql, con=self.engine)
        table.columns = ["customer_id", "customer", "mapping_id", "alias", "_"] # deleted, customer_id
        return table.loc[:,~table.columns.isin(["_"])]

    def get_all_city_name_mappings(self, city_id: int=0) -> pd.DataFrame:
        sql = sqlalchemy.select(CITIES,CITY_NAME_MAP)\
                .select_from(CITIES).join(CITY_NAME_MAP)
        if city_id:
            sql = sql.where(CITIES.id == city_id)

        table = pd.read_sql(sql, con=self.engine)
        table.columns = ["city_id", "city", "_", "mapping_id", "alias", "_"] # deleted, customer_id
        return table.loc[:,~table.columns.isin(["_"])]

    def get_all_state_name_mappings(self, state_id: int=0) -> pd.DataFrame:
        sql = sqlalchemy.select(STATES,STATE_NAME_MAP)\
                .select_from(STATES).join(STATE_NAME_MAP)
        if state_id:
            sql = sql.where(STATES.id == state_id)

        table = pd.read_sql(sql, con=self.engine)
        table.columns = ["state_id", "state", "_", "mapping_id", "alias", "_"] # deleted, customer_id
        return table.loc[:,~table.columns.isin(["_"])]

    def get_cities(self,city_id: int=0) -> pd.DataFrame:
        sql = sqlalchemy.select(CITIES)
        if city_id:
            sql = sql.where(CITIES.id==city_id)
        return pd.read_sql(sql, con=self.engine)

    def new_city(self,**kwargs):
        sql = sqlalchemy.insert(CITIES) \
            .values(**kwargs).returning(CITIES.id)
        with Session(bind=self.engine) as session:
            new_id = session.execute(sql).one()[0]
            session.commit()
        kwargs.update({"id": new_id})
        event.post_event("New Record", CITIES, **kwargs)

    def modify_city(self, city_id:int, **kwargs):
        sql = sqlalchemy.update(CITIES).values(**kwargs) \
            .where(CITIES.id == city_id)
        with Session(bind=self.engine) as session:
            session.execute(sql)
            session.commit()
        kwargs.update({"id": city_id})
        event.post_event("Record Updated", CITIES, **kwargs)

    def delete_city_by_id(self, city_id: int):
        sql = sqlalchemy.update(CITIES).values(deleted=datetime.now().isoformat())\
            .where(CITIES.id == city_id)
        with self.engine.begin() as conn:
            conn.execute(sql)

    def get_states(self,state_id: int=0) -> pd.DataFrame:
        sql = sqlalchemy.select(STATES)
        if state_id:
            sql = sql.where(STATES.id==state_id)
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
        table.columns = ["rep_customer_id", "branch_id", "customer_id", "customer",
                "city_id", "city", "state_id", "state", "rep_id", "rep"]
        return table
        
    def set_customer_name_mapping(self, **kwargs):
        sql = sqlalchemy.insert(CUSTOMER_NAME_MAP).values(**kwargs)
        with Session(bind=self.engine) as session:
            session.execute(sql)
            session.commit()
        event.post_event("New Record", CUSTOMER_NAME_MAP, **kwargs)

    def set_city_name_mapping(self, **kwargs):
        sql = sqlalchemy.insert(CITY_NAME_MAP).values(**kwargs)
        with Session(bind=self.engine) as session:
            session.execute(sql)
            session.commit()
        event.post_event("New Record", CITY_NAME_MAP, **kwargs)
 
    def set_state_name_mapping(self, **kwargs):
        sql = sqlalchemy.insert(STATE_NAME_MAP).values(**kwargs)
        with Session(bind=self.engine) as session:
            session.execute(sql)
            session.commit()
        event.post_event("New Record", STATE_NAME_MAP, **kwargs)

    def set_rep_to_customer_mapping(self, **kwargs):
        sql = sqlalchemy.insert(REPS_CUSTOMERS_MAP).values(**kwargs)
        with Session(bind=self.engine) as session:
            session.execute(sql)
            session.commit()
        event.post_event("New Record", REPS_CUSTOMERS_MAP, **kwargs)

    def update_rep_to_customer_mapping(self, map_id: int, **kwargs):
        sql = sqlalchemy.update(REPS_CUSTOMERS_MAP).values(**kwargs).where(REPS_CUSTOMERS_MAP.id == map_id)
        with Session(bind=self.engine) as session:
            session.execute(sql)
            session.commit()
        event.post_event("Record Updated", REPS_CUSTOMERS_MAP, **kwargs)

    def get_processing_steps(self, submission_id: int) -> pd.DataFrame:
        """get all report processing steps for a commission report submission"""
        result = pd.read_sql(
            sqlalchemy.select(PROCESS_STEPS_LOG).where(PROCESS_STEPS_LOG.submission_id == submission_id),
            con=self.engine
        )
        return result

    def get_errors(self, submission_id: int=0) -> pd.DataFrame:
        """get all report processing errors for a commission report submission"""
        sql = sqlalchemy.select(ERRORS_TABLE)
        if submission_id:
            sql = sql.where(ERRORS_TABLE.submission_id == submission_id)
        result = pd.read_sql(sql, con=self.engine)
        if result.empty:
            return result
        result.loc[:,'row_data'] = result.loc[:,'row_data'].apply(lambda json_str: json.loads(json_str))
        return result

    def rep_customer_id_exists(self, id_: int) -> bool:
        sql = sqlalchemy.select(REPS_CUSTOMERS_MAP).where(REPS_CUSTOMERS_MAP.id == id_)
        with self.engine.begin() as conn:
            result = conn.execute(sql)
        return True if result else False

    def set_new_commission_data_entry(self, **kwargs) -> int:
        sql = sqlalchemy.insert(COMMISSION_DATA_TABLE)\
            .values(**kwargs).returning(COMMISSION_DATA_TABLE.row_id)
        with self.engine.begin() as conn:
            result = conn.execute(sql).one()[0]
        return result

    @staticmethod
    def convert_cents_to_dollars(cent_amt: float) -> float:
        return round(cent_amt/100,2)

    @staticmethod
    def convert_month_from_number_to_name(month_num: int) -> str:
        return calendar.month_name[month_num]

    def commission_data_with_all_names(self, submission_id:int = 0) -> pd.DataFrame:
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
            .join(states)                      \
            .order_by(
                submission_data.reporting_year.desc(),
                submission_data.reporting_month.desc(),
                customers.name.asc(),
                cities.name.asc(),
                states.name.asc()
            )

        if submission_id:
            sql = sql.where(commission_data_raw.submission_id == submission_id)
        view_table = pd.read_sql(sql, con=self.engine)
        view_table.columns = ["ID","Year","Month","Manufacturer","Salesman",
                "Customer Name","City","State","Inv Amt","Comm Amt"]
        view_table.loc[:,"Inv Amt"] = view_table.loc[:,"Inv Amt"].apply(self.convert_cents_to_dollars)
        view_table.loc[:,"Comm Amt"] = view_table.loc[:,"Comm Amt"].apply(self.convert_cents_to_dollars)
        view_table.loc[:,"Month"] = view_table.loc[:,"Month"].apply(self.convert_month_from_number_to_name).astype(str)
        return view_table

    def get_commission_data_by_row(self, row_id: int) -> COMMISSION_DATA_TABLE:
        sql = sqlalchemy.select(COMMISSION_DATA_TABLE) \
            .where(COMMISSION_DATA_TABLE.row_id == row_id)
        with self.engine.begin() as conn:
            return conn.execute(sql).fetchone()

    def modify_commission_data_row(self, row_id: int, **kwargs):
        sql = sqlalchemy.update(COMMISSION_DATA_TABLE) \
                .values(**kwargs).where(COMMISSION_DATA_TABLE.row_id == row_id)
        with self.engine.begin() as conn:
            conn.execute(sql)
        return

    def delete_commission_data_line(self, row_id: int):
        sql = sqlalchemy.delete(COMMISSION_DATA_TABLE)\
            .where(COMMISSION_DATA_TABLE.row_id == row_id)
        with self.engine.begin() as conn:
            conn.execute(sql)
        return

    def delete_customer(self, customer_id: int):
        sql = sqlalchemy.update(CUSTOMERS)\
            .values(deleted = datetime.now())\
            .where(CUSTOMERS.id==customer_id)
        with self.engine.begin() as conn:
            conn.execute(sql)
        return

    def delete_manufacturer(self, manufacturer_id: int):
        sql = sqlalchemy.update(MANUFACTURERS)\
            .values(deleted = datetime.now())\
            .where(MANUFACTURERS.id==manufacturer_id)
        with self.engine.begin() as conn:
            conn.execute(sql)
        return

    def set_new_manufacturer(self, **kwargs) -> int:  
        sql = sqlalchemy.insert(MANUFACTURERS).values(**kwargs)\
            .returning(MANUFACTURERS.id)
        with self.engine.begin() as conn:
            new_id = conn.execute(sql).one()[0]
        return new_id

    def delete_rep(self, rep_id:int):
        sql = sqlalchemy.update(REPS)\
            .values(deleted = datetime.now())\
            .where(REPS.id==rep_id)
        with self.engine.begin() as conn:
            conn.execute(sql)
        return

    def modify_rep(self, rep_id:int, **kwargs):
        sql = sqlalchemy.update(REPS) \
                .values(**kwargs).where(REPS.id == rep_id)
        with self.engine.begin() as conn:
            conn.execute(sql)
        return

    def set_new_rep(self, **kwargs) -> int:
        sql = sqlalchemy.insert(REPS).values(**kwargs)\
            .returning(REPS.id)
        with self.engine.begin() as conn:
            new_id = conn.execute(sql).one()[0]
        return new_id

    def set_new_state(self, **kwargs) -> int:
        sql = sqlalchemy.insert(STATES).values(**kwargs)\
            .returning(STATES.id)
        with self.engine.begin() as conn:
            new_id = conn.execute(sql).one()[0]
        kwargs.update({"id": new_id})
        event.post_event("New Record", STATES, **kwargs)
        return new_id

    def modify_state(self, state_id:int, **kwargs):
        sql = sqlalchemy.update(STATES) \
                .values(**kwargs).where(STATES.id == state_id)
        with self.engine.begin() as conn:
            conn.execute(sql)
        event.post_event("Record Updated", STATES, state_id, **kwargs)
        return

    def delete_state(self, state_id:int):
        sql = sqlalchemy.update(STATES)\
            .values(deleted = datetime.now())\
            .where(STATES.id==state_id)
        with self.engine.begin() as conn:
            conn.execute(sql)
        return

    def delete_customer_name_mapping(self, mapping_id: int):
        sql = sqlalchemy.delete(CUSTOMER_NAME_MAP).where(CUSTOMER_NAME_MAP.id == mapping_id)
        with self.engine.begin() as conn:
            conn.execute(sql)
        return


    def delete_customer_rep_mapping(self, mapping_id: int):
        sql = sqlalchemy.update(REPS_CUSTOMERS_MAP)\
            .values(orphaned = datetime.now())\
            .where(REPS_CUSTOMERS_MAP.id == mapping_id)
        with self.engine.begin() as conn:
            conn.execute(sql)
        return

    def modify_city_name_mapping(self, mapping_id: int, **kwargs):
        sql = sqlalchemy.update(CITY_NAME_MAP).values(**kwargs).where(CITY_NAME_MAP.id == mapping_id)
        with self.engine.begin() as conn:
            conn.execute(sql)
        event.post_event("Record Updated", CITY_NAME_MAP, **kwargs)
        
    def modify_state_name_mapping(self, mapping_id: int, **kwargs):
        sql = sqlalchemy.update(STATE_NAME_MAP).values(**kwargs).where(STATE_NAME_MAP.id == mapping_id)
        with self.engine.begin() as conn:
            conn.execute(sql)
        event.post_event("Record Updated", STATE_NAME_MAP, **kwargs)

    def delete_submission(self, submission_id: int):
        sql_errors = sqlalchemy.delete(ERRORS_TABLE).where(ERRORS_TABLE.submission_id == submission_id)
        sql_submission = sqlalchemy.delete(SUBMISSIONS_TABLE).where(SUBMISSIONS_TABLE.id == submission_id)
        sql_commission = sqlalchemy.delete(COMMISSION_DATA_TABLE).where(COMMISSION_DATA_TABLE.submission_id == submission_id)
        sql_processing_steps = sqlalchemy.delete(PROCESS_STEPS_LOG).where(PROCESS_STEPS_LOG.submission_id == submission_id)
        with self.engine.begin() as conn:
            conn.execute(sql_commission)
            conn.execute(sql_processing_steps)
            conn.execute(sql_errors)
            conn.execute(sql_submission)
        
    def modify_submission_metadata(self, submission_id:int, **kwargs):
        sql = sqlalchemy.update(SUBMISSIONS_TABLE).values(**kwargs).where(SUBMISSIONS_TABLE.id == submission_id)
        with self.engine.begin() as conn:
            conn.execute(sql)  

    def reactivate_branch(self, branch_id: int):
        sql = sqlalchemy.update(BRANCHES).values(deleted=None).where(BRANCHES.id == branch_id)
        with self.engine.begin() as conn:
            conn.execute(sql)

    def reactivate_city(self, city_id: int):
        sql = sqlalchemy.update(CITIES).values(deleted=None).where(CITIES.id == city_id)
        with self.engine.begin() as conn:
            conn.execute(sql)


    # JSON:API implementation - passing in a db session instead of creating one
    def get_customer_jsonapi(self, db: Session, cust_id: int, query: dict) -> dict:
        model_name = hyphenate_name(CUSTOMERS.__tablename__)
        return models.serializer.get_resource(db,query,model_name,cust_id)

    def get_many_customers_jsonapi(self, db: Session, query: dict) -> dict:
        model_name = hyphenate_name(CUSTOMERS.__tablename__)
        return models.serializer.get_collection(db,query,model_name)

    def get_related(self, db: Session, primary: str, id_: int, secondary: str) -> dict:
        return models.serializer.get_related(db,{},primary,id_,secondary)

    def get_many_cities_jsonapi(self, db: Session, query: dict):
        model_name = hyphenate_name(CITIES.__tablename__)
        return models.serializer.get_collection(db,query,model_name)

    def get_city_jsonapi(self, db: Session, city_id: int, query: dict) -> dict:
        model_name = hyphenate_name(CITIES.__tablename__)
        return models.serializer.get_resource(db,query,model_name,city_id)
