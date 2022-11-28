import os
import re
import json
import calendar
from datetime import datetime
from dotenv import load_dotenv
from os import getenv
import requests
from dataclasses import dataclass

import pandas as pd
from sqlalchemy.exc import IntegrityError
import sqlalchemy
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy_jsonapi.serializer import JSONAPIResponse
from fastapi import Request, HTTPException

from app import event
from app.jsonapi import jsonapi_error_handling
from db import models
from entities.submission import NewSubmission
from entities.processing_step import ProcessingStep
from entities.error import Error

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
DOWNLOADS = models.FileDownloads
FORM_FIELDS = models.ReportFormFields
USERS = models.User
USER_COMMISSIONS = models.UserCommissionRate
COMMISSION_SPLITS = models.CommissionSplit

load_dotenv()

class UserMisMatch(Exception):
    pass

@dataclass
class User:
    nickname: str
    name: str
    email: str
    verified: bool

    def domain(self) -> str:
        return re.search(r"(.*)@(.*)",self.email)[2] if self.verified else None

    def id(self, db: Session) -> int:
        return db.execute("SELECT id FROM users WHERE company_domain = :domain", {"domain": self.domain()}).scalar_one_or_none()


def get_user(request: Request) -> User:
    url = os.getenv("AUTH0_DOMAIN") + "/userinfo"
    auth0_user_body: dict =  requests.get(url, headers={"Authorization": request.headers.get("Authorization")}).json()
    match auth0_user_body:
        case {"nickname": a, "name": b, "email": c, "email_verified": d, **other}:
            return User(nickname=a, name=b, email=c, verified=d)
        case _:
            raise HTTPException(status=400, detail={"user could not be verified"})

def hyphenate_name(table_name: str) -> str:
    return table_name.replace("_","-")

def hyphenated_name(table_obj) -> str:
    return table_obj.__tablename__.replace("_","-")

def hyphenate_attribute_keys(json_data: dict) -> dict:
    json_data["data"]["attributes"] = {hyphenate_name(k):v for k,v in json_data["data"]["attributes"].items()}
    return json_data

def get_db():
    db = ApiAdapter().SessionLocal()
    try:
        yield db
    finally:
        db.close()

class ApiAdapter:

    engine = sqlalchemy.create_engine(getenv("DATABASE_URL").replace("postgres://","postgresql://"))
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    def submission_exists(self, submission_id: int) -> bool:
        sql = sqlalchemy.select(SUBMISSIONS_TABLE).where(SUBMISSIONS_TABLE.id == submission_id)
        with self.engine.begin() as conn:
            result = conn.execute(sql).fetchone()
        return True if result else False


    def set_city_name_mapping(self, **kwargs):
        sql = sqlalchemy.insert(CITY_NAME_MAP).values(**kwargs)
        with Session(bind=self.engine) as session:
            session.execute(sql)
            session.commit()
        event.post_event("New Record", CITY_NAME_MAP, **kwargs, session=kwargs.get("db"))
 
    def set_state_name_mapping(self, **kwargs):
        sql = sqlalchemy.insert(STATE_NAME_MAP).values(**kwargs)
        with Session(bind=self.engine) as session:
            session.execute(sql)
            session.commit()
        event.post_event("New Record", STATE_NAME_MAP, **kwargs, session=kwargs.get("db"))


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

    def commission_data_with_all_names(self, submission_id:int = 0, **kwargs) -> pd.DataFrame:
        """runs sql query to produce the commission table format used by SCA
        and converts month number to name and cents to dollars before return
        
        Returns: pd.DataFrame"""
        commission_data_raw = COMMISSION_DATA_TABLE
        submission_data = SUBMISSIONS_TABLE
        reports = REPORTS
        manufacturers = MANUFACTURERS
        reps = REPS
        branches = BRANCHES
        customers = CUSTOMERS
        cities = CITIES
        states = STATES
        sql = sqlalchemy.select(commission_data_raw.id,
            submission_data.reporting_year, submission_data.reporting_month,
            manufacturers.name, reps.initials, customers.name,
            cities.name, states.name, commission_data_raw.inv_amt,
            commission_data_raw.comm_amt
            ).select_from(commission_data_raw) \
            .join(submission_data)             \
            .join(reports)                     \
            .join(manufacturers)               \
            .join(branches)                    \
            .join(reps)                        \
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

        if (start_date := kwargs.get("startDate")):
            try:
                start_date = datetime.fromisoformat(start_date)
            except ValueError:
                start_date = datetime.fromisoformat(start_date.replace("Z",""))
            except Exception as e:
                print(e)
            if isinstance(start_date, datetime):
                sql = sql.where(sqlalchemy.and_(
                    submission_data.reporting_year >= start_date.year,
                    submission_data.reporting_month >= start_date.month))
        if (end_date := kwargs.get("endDate")):
            try:
                end_date = datetime.fromisoformat(end_date)
            except ValueError:
                end_date = datetime.fromisoformat(end_date.replace("Z",""))
            except Exception as e:
                print(e)
            if isinstance(end_date, datetime):
                sql = sql.where(sqlalchemy.and_(
                    submission_data.reporting_year <= end_date.year,
                    submission_data.reporting_month <= end_date.month))
        if(manufacturer := kwargs.get("manufacturer_id")):
            sql = sql.where(manufacturers.id == manufacturer)
        if(customer := kwargs.get("customer_id")):
            sql = sql.where(customers.id == customer)
        if(city := kwargs.get("city_id")):
            sql = sql.where(cities.id == city)
        if(state := kwargs.get("state_id")):
            sql = sql.where(states.id == state)
        if(representative := kwargs.get("representative_id")):
            sql = sql.where(reps.id == representative)

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


    def delete_submission(self, submission_id: int, session: Session):
        sql_errors = sqlalchemy.delete(ERRORS_TABLE).where(ERRORS_TABLE.submission_id == submission_id)
        sql_commission = sqlalchemy.delete(COMMISSION_DATA_TABLE).where(COMMISSION_DATA_TABLE.submission_id == submission_id)
        sql_processing_steps = sqlalchemy.delete(PROCESS_STEPS_LOG).where(PROCESS_STEPS_LOG.submission_id == submission_id)
        sql_submission = sqlalchemy.delete(SUBMISSIONS_TABLE).where(SUBMISSIONS_TABLE.id == submission_id)
        session.execute(sql_commission)
        session.execute(sql_processing_steps)
        session.execute(sql_errors)
        session.execute(sql_submission)
        session.commit()
        
    def get_mappings(self, db: Session, table: str, user_id: int) -> pd.DataFrame:
        if table == "map_customer_names":
            sql = sqlalchemy.select(CUSTOMER_NAME_MAP).join(CUSTOMERS).where(CUSTOMERS.user_id == user_id)
        elif table == "map_city_names":
            sql = sqlalchemy.select(CITY_NAME_MAP).join(CITIES).where(CITIES.user_id == user_id)
        elif table == "map_state_names":
            sql = sqlalchemy.select(STATE_NAME_MAP).join(STATES).where(STATES.user_id == user_id)
        return pd.read_sql(sql,db.get_bind())

    def get_all_manufacturers(self, db: Session) -> dict:
        sql = sqlalchemy.select(MANUFACTURERS.id,MANUFACTURERS.name).where(MANUFACTURERS.deleted == None)
        query_result = db.execute(sql).fetchall()
        return {id_: name_.lower().replace(" ","_") for id_, name_ in query_result}

    def get_branches(self, db: Session, user_id: int) -> pd.DataFrame:
        sql = sqlalchemy.select(BRANCHES).join(CUSTOMERS).where(CUSTOMERS.user_id == user_id)
        data = pd.read_sql(sql,con=db.get_bind())
        def _try_convert_number(value: str) -> str:
            try:
                return str(int(float(value)))
            except:
                return value
        data["store_number"] = data["store_number"].apply(_try_convert_number)
        return data

    def record_final_data(self, db: Session, data: pd.DataFrame) -> None:
        data_records = data.to_dict(orient="records")
        sql = sqlalchemy.insert(COMMISSION_DATA_TABLE)
        db.execute(sql, data_records) # for bulk insert per SQLAlchemy docs
        db.commit()
        return

    def record_submission(self, db: Session, submission: NewSubmission) -> int:
        sql = sqlalchemy.insert(SUBMISSIONS_TABLE).returning(SUBMISSIONS_TABLE.id)\
                .values(**submission)
        result = db.execute(sql).fetchone()[0]
        db.commit()
        return result


    def record_processing_step(self, db: Session, step_obj: ProcessingStep) -> bool:
        """commit all report processing stesp for a commission report submission"""
        sql = sqlalchemy.insert(PROCESS_STEPS_LOG).values(**step_obj)
        db.execute(sql)
        db.commit()
        return True

    def last_step_num(self, db: Session, submission_id: int) -> int:
        sql = sqlalchemy.select(sqlalchemy.func.max(PROCESS_STEPS_LOG.step_num))\
            .where(PROCESS_STEPS_LOG.submission_id == submission_id)
        if result := db.execute(sql).one()[0]:
            return result
        else:
            return 0

    def record_error(self, db: Session, error_obj: Error) -> None:
        """record errors into the current_errors table"""
        sql = sqlalchemy.insert(ERRORS_TABLE).values(**error_obj)
        db.execute(sql)
        db.commit()
        return

    def delete_errors(self, db: Session, record_ids: int|list):
        if isinstance(record_ids, int):
            record_ids = [record_ids]
        for record_id in record_ids:
            record_id = int(record_id)
            sql = sqlalchemy.delete(ERRORS_TABLE).where(ERRORS_TABLE.id == record_id)
            db.execute(sql)
        db.commit()

        
    def get_report_name_by_id(self, db: Session, report_id: int) -> str:
        sql = sqlalchemy.select(REPORTS.report_name).where(REPORTS.id == report_id)
        result = db.execute(sql).one_or_none()
        if result:
            return result[0]
        

    # JSON:API implementation - passing in a db session instead of creating one

    def generate_file_record(self, db: Session, record: dict):
        sql = sqlalchemy.insert(DOWNLOADS).values(**record)
        with db as session:
            session.execute(sql)
            session.commit()


    def download_file_lookup(self, db: Session, hash: str):
        sql = sqlalchemy.select(DOWNLOADS).where(DOWNLOADS.hash == hash)
        with db as session:
            return session.execute(sql).one_or_none()


    def mark_file_downloaded(self, db: Session, hash: str):
        sql = sqlalchemy.update(DOWNLOADS).values(downloaded = True).where(DOWNLOADS.hash == hash)
        with db as session:
            session.execute(sql)
            session.commit()
            
    @staticmethod
    def matched_user(user: User, model, reference_id: int, db: Session) -> bool:
        return user.id(db) == db.query(model.user_id).filter(model.id == reference_id).scalar()
    
    @jsonapi_error_handling
    def get_related(self, db: Session, primary: str, id_: int, secondary: str, user: User) -> JSONAPIResponse:
        user_id: int = user.id(db=db)
        return models.serializer.get_related(db,{},primary,id_,secondary)
    
    @jsonapi_error_handling
    def get_relationship(self, db: Session, primary: str, id_: int, secondary: str, user: User) -> JSONAPIResponse:
        user_id: int = user.id(db=db)
        return models.serializer.get_relationship(db,{},primary,id_,secondary)
    

    @jsonapi_error_handling
    def get_customer_jsonapi(self, db: Session, cust_id: int, query: dict, user: User) -> JSONAPIResponse:
        if not self.matched_user(user, CUSTOMERS, cust_id, db):
            raise UserMisMatch()
        model_name = hyphenate_name(CUSTOMERS.__tablename__)
        return models.serializer.get_resource(db,query,model_name,cust_id, obj_only=True)
    
    @jsonapi_error_handling
    def get_many_customers_jsonapi(self, db: Session, query: dict, user: User) -> JSONAPIResponse:
        user_id: int = user.id(db=db)
        return models.serializer.get_collection(db,query,CUSTOMERS,user_id)

    @jsonapi_error_handling
    def get_reports(self, db: Session, query: dict, user: User, report_id: int=0) -> JSONAPIResponse:
        if report_id:
            if not self.matched_user(user, REPORTS, report_id, db):
                raise UserMisMatch()
            return models.serializer.get_resource(db, query, hyphenated_name(REPORTS), report_id, obj_only=True)
        else:
            user_id: int = user.id(db=db)
            return models.serializer.get_collection(db, query, REPORTS, user_id)
    
    @jsonapi_error_handling
    def get_many_cities_jsonapi(self, db: Session, query: dict, user: User) -> JSONAPIResponse:
        user_id: int = user.id(db=db)
        return models.serializer.get_collection(db,query,CITIES, user_id)
    
    @jsonapi_error_handling
    def get_city_jsonapi(self, db: Session, city_id: int, query: dict, user: User) -> JSONAPIResponse:
        if not self.matched_user(user, CITIES, city_id, db):
            raise UserMisMatch()
        model_name = hyphenate_name(CITIES.__tablename__)
        return models.serializer.get_resource(db,query,model_name,city_id, obj_only=True)
    
    @jsonapi_error_handling
    def get_state_jsonapi(self, db: Session, state_id: int, query: dict, user: User) -> JSONAPIResponse:
        if not self.matched_user(user, STATES, state_id, db):
            raise UserMisMatch()
        model_name = hyphenate_name(STATES.__tablename__)
        return models.serializer.get_resource(db,query,model_name,state_id, obj_only=True)
    
    @jsonapi_error_handling
    def get_many_states_jsonapi(self, db: Session, query: dict, user: User) -> JSONAPIResponse:
        user_id: int = user.id(db=db)
        return models.serializer.get_collection(db,query,STATES, user_id)
    
    @jsonapi_error_handling
    def get_rep_jsonapi(self, db: Session, rep_id: int, query: dict, user: User) -> JSONAPIResponse:
        if not self.matched_user(user, REPS, rep_id, db):
            raise UserMisMatch()
        model_name = hyphenate_name(REPS.__tablename__)
        return models.serializer.get_resource(db,query,model_name,rep_id, obj_only=True)
    
    @jsonapi_error_handling
    def get_many_reps_jsonapi(self, db: Session, query: dict, user: User) -> JSONAPIResponse:
        user_id: int = user.id(db=db)
        return models.serializer.get_collection(db,query,REPS, user_id)
    
    @jsonapi_error_handling
    def get_submission_jsonapi(self, db: Session, submission_id: int, query: dict, user: User) -> JSONAPIResponse:
        if not self.matched_user(user, SUBMISSIONS_TABLE, submission_id, db):
            raise UserMisMatch()
        model_name = hyphenate_name(SUBMISSIONS_TABLE.__tablename__)
        return models.serializer.get_resource(db,query,model_name,submission_id, obj_only=True)
    
    @jsonapi_error_handling
    def get_many_submissions_jsonapi(self, db: Session, query: dict, user: User) -> JSONAPIResponse:
        user_id: int = user.id(db=db)
        return models.serializer.get_collection(db,query,SUBMISSIONS_TABLE, user_id)
    
    @jsonapi_error_handling
    def get_manufacturer_jsonapi(self, db: Session, manuf_id: int, query: dict, user: User) -> JSONAPIResponse:
        if not self.matched_user(user, MANUFACTURERS, manuf_id, db):
            raise UserMisMatch()
        model_name = hyphenate_name(MANUFACTURERS.__tablename__)
        return models.serializer.get_resource(db,query,model_name,manuf_id, obj_only=True)
    
    @jsonapi_error_handling
    def get_many_manufacturers_jsonapi(self, db: Session, query: dict, user: User) -> JSONAPIResponse:
        user_id: int = user.id(db=db)
        return models.serializer.get_collection(db,query,MANUFACTURERS, user_id)
    
    @jsonapi_error_handling
    def get_commission_data_by_id_jsonapi(self, db: Session, row_id: int, query: dict, user: User) -> JSONAPIResponse:
        if not self.matched_user(user, COMMISSION_DATA_TABLE, row_id, db):
            raise UserMisMatch()
        model_name = hyphenate_name(COMMISSION_DATA_TABLE.__tablename__)
        return models.serializer.get_resource(db,query,model_name,row_id, obj_only=True)
    
    @jsonapi_error_handling
    def get_all_commission_data_jsonapi(self, db: Session, query: dict, user: User) -> JSONAPIResponse:
        user_id: int = user.id(db=db)
        return models.serializer.get_collection(db,query,COMMISSION_DATA_TABLE, user_id)
    
    @jsonapi_error_handling   
    def get_all_customer_name_mappings(self, db: Session, query: dict, user: User) -> JSONAPIResponse:
        user_id: int = user.id(db=db)
        return models.serializer.get_collection(db,query,CUSTOMER_NAME_MAP, user_id)
    
    @jsonapi_error_handling
    def get_customer_name_mapping_by_id(self, db: Session, mapping_id: int, query: dict, user: User) -> JSONAPIResponse:
        if not self.matched_user(user, CUSTOMER_NAME_MAP, mapping_id, db):
            raise UserMisMatch()
        model_name = hyphenate_name(CUSTOMER_NAME_MAP.__tablename__)
        return models.serializer.get_resource(db,query,model_name,mapping_id,obj_only=True)

    @jsonapi_error_handling
    def get_many_branches_jsonapi(self, db: Session, query: dict, user: User) -> JSONAPIResponse:
        user_id: int = user.id(db=db)
        return models.serializer.get_collection(db,query,BRANCHES, user_id)
    
    @jsonapi_error_handling
    def get_branch(self, db: Session, branch_id: int, query: dict, user: User) -> JSONAPIResponse:
        if not self.matched_user(user, BRANCHES, branch_id, db):
            raise UserMisMatch()
        model_name = hyphenated_name(BRANCHES)
        return models.serializer.get_resource(db,query,model_name,branch_id,obj_only=True)

    @jsonapi_error_handling
    def create_customer_name_mapping(self, db: Session, json_data: dict, user: User) -> JSONAPIResponse:
        # TODO use user in actual post_collection
        model_name = hyphenated_name(CUSTOMER_NAME_MAP)
        hyphenate_attribute_keys(json_data)
        result = models.serializer.post_collection(db,json_data,model_name).data
        event.post_event("New Record", CUSTOMER_NAME_MAP, session=db, user=user)
        return result

    @jsonapi_error_handling
    def modify_customer_jsonapi(self, db: Session, customer_id: int, json_data: dict, user: User) -> JSONAPIResponse:
        if not self.matched_user(user, CUSTOMERS, customer_id, db):
            raise UserMisMatch()
        model_name = hyphenated_name(CUSTOMERS)
        hyphenate_attribute_keys(json_data)
        result = models.serializer.patch_resource(db, json_data, model_name, customer_id).data
        event.post_event(
            "Record Updated",
            CUSTOMERS,
            id_=customer_id,
            db=db,
            **json_data["data"]["attributes"],
            session=db,
            user=user)
        return result

    @jsonapi_error_handling
    def modify_branch(self, db: Session, branch_id: int, json_data: dict) -> JSONAPIResponse:
        model_name = hyphenated_name(BRANCHES)
        hyphenate_attribute_keys(json_data)
        return models.serializer.patch_resource(db, json_data, model_name, branch_id).data

    @jsonapi_error_handling
    def delete_map_customer_name(self, db: Session, id_: int):
        model_name = hyphenated_name(CUSTOMER_NAME_MAP)
        return models.serializer.delete_resource(db,{},model_name,id_) # data param is unused
    
    @jsonapi_error_handling
    def delete_a_branch(self, db: Session, branch_id: int):
        _now = datetime.utcnow()
        db.execute("UPDATE customer_branches SET deleted = :current_time WHERE id = :branch_id", {"branch_id": branch_id, "current_time": _now})
        db.commit()
        return

    def set_customer_name_mapping(self, db: Session, **kwargs):
        sql = sqlalchemy.insert(CUSTOMER_NAME_MAP).values(**kwargs)
        try:
            db.execute(sql)
        except IntegrityError:
            db.rollback()
        else:
            db.commit()
            event.post_event("New Record", CUSTOMER_NAME_MAP, **kwargs, session=db)

    def create_new_customer_branch_bulk(self, db: Session, records: list[dict]):
        sql_default_rep = sqlalchemy.select(REPS.id).where(REPS.initials == "sca")
        default_rep = db.execute(sql_default_rep).scalar()
        for record in records:
            record["rep_id"]=default_rep
            record["in_territory"]=True
            db.add(BRANCHES(**record))
        db.commit()
        return

    def get_errors(self, db: Session, submission_id: int=0) -> pd.DataFrame:
        """get all report processing errors for a commission report submission"""
        sql = sqlalchemy.select(ERRORS_TABLE)
        if submission_id:
            sql = sql.where(ERRORS_TABLE.submission_id == submission_id)
        result = pd.read_sql(sql, con=db.get_bind())
        if result.empty:
            return result
        result.loc[:,'row_data'] = result.loc[:,'row_data'].apply(lambda json_str: json.loads(json_str))
        return result


    def get_all_submissions(self, db: Session) -> pd.DataFrame:
        subs = SUBMISSIONS_TABLE
        reports = REPORTS
        manufs = MANUFACTURERS
        sql = sqlalchemy.select(subs.id,subs.submission_date,subs.reporting_month,subs.reporting_year,
                reports.id.label("report_id"),reports.report_name,reports.yearly_frequency, reports.pos_report,
                manufs.name).select_from(subs).join(reports).join(manufs)
        return pd.read_sql(sql, con=db.get_bind())


    def get_commission_rate(self, db: Session, manufacturer_id: int, user_id: int) -> float:
        sql = sqlalchemy.select(USER_COMMISSIONS.commission_rate)\
            .where(
                sqlalchemy.and_(
                    USER_COMMISSIONS.manufacturer_id == manufacturer_id,
                    USER_COMMISSIONS.user_id == user_id
            ))
        result = db.execute(sql).scalar()
        return result

    
    def get_split(self, db: Session, report_id: int, user_id: int) -> float:
        sql = sqlalchemy.select(COMMISSION_SPLITS.split_proportion)\
            .where(
                sqlalchemy.and_(
                    COMMISSION_SPLITS.report_id == report_id,
                    COMMISSION_SPLITS.user_id == user_id
            ))
        result = db.execute(sql).scalar()
        return result


