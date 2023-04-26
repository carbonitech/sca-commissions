import os
import re
import json
import time
import calendar
import requests
from os import getenv
from datetime import datetime
from dotenv import load_dotenv
from dataclasses import dataclass

import pandas as pd
import sqlalchemy
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy_jsonapi.serializer import JSONAPIResponse
from fastapi import Request, HTTPException
from jose.jwt import get_unverified_claims

from app import event
from db import models
from entities.error import Error
from entities.submission import NewSubmission
from app.jsonapi import jsonapi_error_handling

CUSTOMERS = models.Customer
BRANCHES = models.CustomerBranch
REPS = models.Representative
MANUFACTURERS = models.Manufacturer
REPORTS = models.ManufacturersReport
COMMISSION_DATA_TABLE = models.CommissionData
SUBMISSIONS_TABLE = models.Submission
ERRORS_TABLE = models.Error
DOWNLOADS = models.FileDownloads
FORM_FIELDS = models.ReportFormFields
USERS = models.User
USER_COMMISSIONS = models.UserCommissionRate
COMMISSION_SPLITS = models.CommissionSplit
ID_STRINGS = models.IDStringMatch
LOCATIONS = models.Location

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

    access_token: str = request.headers.get("Authorization")
    access_token_bare = access_token.replace("Bearer ","")
    if user_details := preverified(access_token_bare):
        return User(**user_details)

    url = os.getenv("AUTH0_DOMAIN") + "/userinfo"
    auth0_user_body: dict = requests.get(url, headers={"Authorization":access_token}).json()
    match auth0_user_body:
        case {"nickname": a, "name": b, "email": c, "email_verified": d, **other}:
            cache_token(access_token_bare, nickname=a, name=b, email=c, verified=d)
            return User(nickname=a, name=b, email=c, verified=d)
        case _:
            raise HTTPException(status_code=400, detail="user could not be verified")

def preverified(access_token: str) -> dict | None:
    session = next(get_db())
    parameters = {"access_token": access_token, "current_time": int(time.time())}
    # without DISTINCT, may run into multiple of the same token cached
    sql = "SELECT DISTINCT nickname, name, email, verified FROM user_tokens WHERE access_token = :access_token and expires_at > :current_time"
    return session.execute(sql, parameters).one_or_none()

def cache_token(access_token: str, nickname: str, name: str, email: str, verified: bool) -> None:
    session = next(get_db())
    if session.execute("select id from user_tokens where access_token = :access_token", {"access_token": access_token}).fetchone():
        session.close()
        return
    sql = "INSERT INTO user_tokens (access_token, nickname, name, email, verified, expires_at)"\
        "VALUES (:access_token, :nickname, :name, :email, :verified, :expires_at)"
    parameters = {
        "access_token": access_token,
        "nickname": nickname,
        "name": name,
        "email": email,
        "verified": verified,
        "expires_at": int(get_unverified_claims(access_token)["exp"]) 
    }
    session.execute(sql, parameters)
    session.commit()
    session.close()

def hyphenate_name(table_name: str) -> str:
    return table_name.replace("_","-")

def hyphenated_name(table_obj) -> str:
    return table_obj.__tablename__.replace("_","-")

def hyphenate_json_obj_keys(json_data: dict) -> dict:
    for hi_level in json_data["data"].keys():
        if not isinstance(json_data["data"][hi_level], dict):
            continue
        json_data["data"][hi_level] = {hyphenate_name(k):v for k,v in json_data["data"][hi_level].items()}
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

    def set_new_commission_data_entry(self, **kwargs) -> int:
        sql = sqlalchemy.insert(COMMISSION_DATA_TABLE)\
            .values(**kwargs).returning(COMMISSION_DATA_TABLE.row_id)
        with self.engine.begin() as conn:
            result = conn.execute(sql).one()[0]
        return result

    @staticmethod
    def convert_cents_to_dollars(cent_amt: float) -> float:
        return cent_amt/100

    @staticmethod
    def convert_month_from_number_to_name(month_num: int) -> str:
        return calendar.month_name[month_num]

    def commission_data_with_all_names(self, db: Session, submission_id: int=0, **kwargs) -> pd.DataFrame:
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
        locations = LOCATIONS
        sql = sqlalchemy.select(commission_data_raw.id, commission_data_raw.submission_id,
            submission_data.reporting_year, submission_data.reporting_month,
            manufacturers.name, reps.initials, customers.name,
            locations.city, locations.state, commission_data_raw.inv_amt,
            commission_data_raw.comm_amt
            ).select_from(commission_data_raw) \
            .join(submission_data, commission_data_raw.submission_id == submission_data.id)\
            .join(reports)                      \
            .join(manufacturers, reports.manufacturer_id == manufacturers.id)\
            .join(branches, commission_data_raw.customer_branch_id == branches.id)\
            .join(reps)                        \
            .join(customers)                   \
            .join(locations)                      \
            .where(commission_data_raw.user_id == kwargs.get("user_id"))\
            .order_by(
                submission_data.reporting_year.desc(),
                submission_data.reporting_month.desc(),
                customers.name.asc(),
                locations.city.asc(),
                locations.state.asc()
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
                sql = sql.where(sqlalchemy.or_(
                    sqlalchemy.and_(
                        submission_data.reporting_year == start_date.year,
                        submission_data.reporting_month >= start_date.month),
                    submission_data.reporting_year > start_date.year))
        if (end_date := kwargs.get("endDate")):
            try:
                end_date = datetime.fromisoformat(end_date)
            except ValueError:
                end_date = datetime.fromisoformat(end_date.replace("Z",""))
            except Exception as e:
                print(e)
            if isinstance(end_date, datetime):
                sql = sql.where(sqlalchemy.or_(
                    sqlalchemy.and_(
                        submission_data.reporting_year == end_date.year,
                        submission_data.reporting_month <= end_date.month),
                    submission_data.reporting_year < end_date.year))
        if(manufacturer := kwargs.get("manufacturer_id")):
            sql = sql.where(manufacturers.id == manufacturer)
        if(customer := kwargs.get("customer_id")):
            sql = sql.where(customers.id == customer)
        if(city := kwargs.get("city_id")):
            sql = sql.where(locations.city == city)
        if(state := kwargs.get("state_id")):
            sql = sql.where(locations.state == state)
        if(representative := kwargs.get("representative_id")):
            sql = sql.where(reps.id == representative)

        view_table = pd.read_sql(sql, con=db.get_bind())

        view_table.columns = ["ID","Submission","Year","Month","Manufacturer","Salesman",
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


    def delete_submission(self, submission_id: int, session: Session, user: User):
        if not self.matched_user(user, SUBMISSIONS_TABLE, submission_id, session):
            raise UserMisMatch()
        sql_errors = sqlalchemy.delete(ERRORS_TABLE).where(ERRORS_TABLE.submission_id == submission_id)
        sql_commission = sqlalchemy.delete(COMMISSION_DATA_TABLE).where(COMMISSION_DATA_TABLE.submission_id == submission_id)
        sql_submission = sqlalchemy.delete(SUBMISSIONS_TABLE).where(SUBMISSIONS_TABLE.id == submission_id)
        session.execute(sql_commission)
        session.execute(sql_errors)
        session.execute(sql_submission)
        session.commit()
        
    def get_all_manufacturers(self, db: Session) -> dict:
        sql = sqlalchemy.select(MANUFACTURERS.id,MANUFACTURERS.name).where(MANUFACTURERS.deleted == None)
        query_result = db.execute(sql).fetchall()
        return {id_: name_.lower().replace(" ","_").replace("-","_") for id_, name_ in query_result}

    def get_branches(self, db: Session, user_id: int) -> pd.DataFrame:
        return self.get_all_by_user_id(db, BRANCHES, user_id)
    
    def get_id_string_matches(self, db: Session, user_id: int) -> pd.DataFrame:
        return self.get_all_by_user_id(db, ID_STRINGS, user_id).loc[:,["match_string","report_id","customer_branch_id","id"]]
    
    def generate_string_match_supplement(self, db: Session, user_id: int) -> pd.DataFrame:
        branches_expanded_sql = sqlalchemy.select(CUSTOMERS.name, LOCATIONS.city, LOCATIONS.state, BRANCHES.id)\
            .select_from(BRANCHES).join(CUSTOMERS).join(LOCATIONS).where(BRANCHES.user_id == user_id)
        result = pd.read_sql(branches_expanded_sql, con=db.get_bind())
        # create the match_string from customer name, city, and state
        result.loc[:,"match_string"] = result[["name", "city", "state"]].apply(
            lambda row: '_'.join(row.values.astype(str)), axis=1
        )
        result = result.rename(columns={"id": "customer_branch_id"})
        return result.loc[:,["match_string", "customer_branch_id"]]
    
    def record_auto_matched_strings(self, db: Session, user_id: int, data: pd.DataFrame) -> None:
        """
        record id string matches in the database from auto-matching
        Return a DataFrame of the inserted values with their id's
        """
        data_cp = data.copy().drop_duplicates()
        data_cp = data_cp.rename(columns={"id_string": "match_string"})
        data_cp.loc[:, "auto_matched"] = True
        data_cp.loc[:, "user_id"] = user_id
        data_cp.loc[:, "created_at"] = datetime.utcnow()
        # table should have the match_string, report_id, customer_branch_id, auto_matched, user_id, created_at, and match_score
        data_records = data_cp.to_dict(orient="records")
        insert_stmt = sqlalchemy.insert(ID_STRINGS)\
            .values(data_records)\
            .returning(ID_STRINGS.id, ID_STRINGS.match_string, ID_STRINGS.report_id)
        return_results = db.execute(insert_stmt).mappings().all()
        db.commit()
        return pd.DataFrame(return_results).rename(columns={"id": "report_branch_ref"})

    
    def report_id_by_submission(self, db: Session, user_id: int, sub_ids: list):
        all_subs = self.get_all_by_user_id(db, SUBMISSIONS_TABLE, user_id)
        target_subs = all_subs.loc[all_subs["id"].isin(sub_ids),["id", "report_id"]]
        target_subs.columns = ["submission_id", "report_id"]
        return target_subs
    
    def get_all_by_user_id(self, db: Session, table: models.Base, user_id: int) -> pd.DataFrame:
        sql = sqlalchemy.select(table).where(table.user_id == user_id)
        return pd.read_sql(sql,con=db.get_bind())      

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
        try:
            return user.id(db) == db.query(model.user_id).filter(model.id == reference_id).scalar()
        except:
            return False
    
    @jsonapi_error_handling
    def get_related(self, db: Session, primary: str, id_: int, secondary: str, user: User) -> JSONAPIResponse:
        return models.serializer.get_related(db,{},primary,id_,secondary)
    
    @jsonapi_error_handling
    def get_relationship(self, db: Session, primary: str, id_: int, secondary: str, user: User) -> JSONAPIResponse:
        return models.serializer.get_relationship(db,{},primary,id_,secondary)
    
    @jsonapi_error_handling
    def __get_X(self, db: Session, query: dict, user: User, model: models.Base, _id: int=0) -> JSONAPIResponse:
        if not _id:
            user_id: int = user.id(db=db)
            return models.serializer.get_collection(db,query,model,user_id)
        else:
            if not self.matched_user(user, model, _id, db):
                raise UserMisMatch()
            return models.serializer.get_resource(db, query, hyphenated_name(model), _id, obj_only=True)
        

    @jsonapi_error_handling
    def get_customers(self, db: Session, query: dict, user: User, cust_id: int=0) -> JSONAPIResponse:
        return self.__get_X(db, query, user, CUSTOMERS, cust_id)
    
    @jsonapi_error_handling
    def get_reports(self, db: Session, query: dict, user: User, report_id: int=0) -> JSONAPIResponse:
        return self.__get_X(db, query, user, REPORTS, report_id)
    
    @jsonapi_error_handling
    def get_reps(self, db: Session, query: dict, user: User, rep_id: int=0) -> JSONAPIResponse:
        return self.__get_X(db, query, user, REPS, rep_id)
    
    @jsonapi_error_handling
    def get_submissions(self, db: Session, query: dict, user: User, submission_id: int=0) -> JSONAPIResponse:
        return self.__get_X(db, query, user, SUBMISSIONS_TABLE, submission_id)
    
    @jsonapi_error_handling
    def get_mappings(self, db: Session, query: dict, user: User, _id: int=0) -> JSONAPIResponse:
        return self.__get_X(db, query, user, ID_STRINGS, _id)

    @jsonapi_error_handling
    def get_manufacturers(self, db: Session, query: dict, user: User, manuf_id: int=0) -> JSONAPIResponse:
        return self.__get_X(db, query, user, MANUFACTURERS, manuf_id)
    
    @jsonapi_error_handling
    def get_commission_data(self, db: Session, query: dict, user: User, row_id: int=0) -> JSONAPIResponse:
        return self.__get_X(db, query, user, COMMISSION_DATA_TABLE, row_id)
    
    @jsonapi_error_handling
    def get_branch(self, db: Session, query: dict, user: User, branch_id: int=0) -> JSONAPIResponse:
        return self.__get_X(db, query, user, BRANCHES, branch_id)

    @jsonapi_error_handling
    def get_location(self, db: Session, query: dict, user: User, location_id: int=0) -> JSONAPIResponse:
        return self.__get_X(db, query, user, LOCATIONS, location_id)


    @jsonapi_error_handling
    def __create_X(self, db: Session, json_data: dict, user: User, model: models.Base) -> JSONAPIResponse:
        model_name = hyphenated_name(model)
        hyphenate_json_obj_keys(json_data)
        result = models.serializer.post_collection(db,json_data,model_name,user.id(db=db)).data
        event.post_event(
            "New Record",
            model,
            db=db,
            user=user,
            id_=result["data"]["id"]
        )
        return result

    @jsonapi_error_handling
    def create_customer(self, db: Session, json_data: dict, user: User) -> JSONAPIResponse:
        new_name: str = json_data["data"]["attributes"]["name"]
        json_data["data"]["attributes"]["name"] = new_name.upper().strip()
        return self.__create_X(db, json_data, user, CUSTOMERS)
    
    def create_branch(self, db: Session, json_data: dict, user: User) -> JSONAPIResponse:
        return self.__create_X(db, json_data, user, BRANCHES)
    
    def create_mapping(self, db: Session, json_data: dict, user: User) -> JSONAPIResponse:
        return self.__create_X(db, json_data, user, ID_STRINGS)
    

    @jsonapi_error_handling
    def modify_customer_jsonapi(self, db: Session, customer_id: int, json_data: dict, user: User) -> JSONAPIResponse:
        if not self.matched_user(user, CUSTOMERS, customer_id, db):
            raise UserMisMatch()
        model_name = hyphenated_name(CUSTOMERS)
        hyphenate_json_obj_keys(json_data)
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
    def modify_branch(self, db: Session, branch_id: int, json_data: dict, user: User) -> JSONAPIResponse:
        if not self.matched_user(user, BRANCHES, branch_id, db):
            raise UserMisMatch()
        model_name = hyphenated_name(BRANCHES)
        hyphenate_json_obj_keys(json_data)
        return models.serializer.patch_resource(db, json_data, model_name, branch_id).data

    @jsonapi_error_handling
    def delete_a_branch(self, db: Session, branch_id: int):
        _now = datetime.utcnow()
        db.execute("UPDATE customer_branches SET deleted = :current_time WHERE id = :branch_id", {"branch_id": branch_id, "current_time": _now})
        db.commit()
        return
    
    @jsonapi_error_handling
    def delete_mapping(self, db: Session, mapping_id: int) -> None:
        sql = sqlalchemy.delete(ID_STRINGS).where(ID_STRINGS.id == mapping_id)
        db.execute(sql)
        db.commit()
        return

    def get_errors(self, db: Session, user: User, submission_id: int=0) -> pd.DataFrame:
        """get all report processing errors for all submissions by user, or a specific submission"""
        sql = sqlalchemy.select(ERRORS_TABLE).where(ERRORS_TABLE.user_id == user.id(db=db))
        if submission_id:
            sql = sql.where(ERRORS_TABLE.submission_id == submission_id)
        result = pd.read_sql(sql, con=db.get_bind())
        if result.empty:
            return result
        result.loc[:,'row_data'] = result.loc[:,'row_data'].apply(lambda json_str: json.loads(json_str))
        return result


    def get_all_submissions(self, db: Session, user: User) -> pd.DataFrame:
        subs = SUBMISSIONS_TABLE
        reports = REPORTS
        manufs = MANUFACTURERS
        sql = sqlalchemy.select(subs.id,subs.submission_date,subs.reporting_month,subs.reporting_year,
                reports.id.label("report_id"),reports.report_name,reports.yearly_frequency, reports.pos_report,
                manufs.name).select_from(subs).join(reports).join(manufs).where(subs.user_id == user.id(db=db))
        return pd.read_sql(sql, con=db.get_bind())


    def get_commission_rate(self, db: Session, manufacturer_id: int, user_id: int) -> float|None:
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

    def alter_sub_status(self, db: Session, submission_id: int, status: str) -> bool:
        sql = sqlalchemy.update(SUBMISSIONS_TABLE).values(status=status).where(SUBMISSIONS_TABLE.id==submission_id)
        try:
            db.execute(sql)
            db.commit()
        except:
            return False
        else:
            return True
