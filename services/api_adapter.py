import calendar
from os import getenv
from datetime import datetime
from dotenv import load_dotenv

import pandas as pd
import sqlalchemy
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy_jsonapi.serializer import JSONAPIResponse

from app import event
from db import models
from entities.error import Error
from entities.submission import NewSubmission
from jsonapi.jsonapi import jsonapi_error_handling

from services.utils import *
from entities.user import User
load_dotenv()

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

    def modify_commission_data_row(self, row_id: int, **kwargs):
        sql = sqlalchemy.update(COMMISSION_DATA_TABLE) \
                .values(**kwargs).where(COMMISSION_DATA_TABLE.row_id == row_id)
        with self.engine.begin() as conn:
            conn.execute(sql)
        return

def delete_submission(submission_id: int, session: Session, user: User) -> None:
    if not matched_user(user, SUBMISSIONS_TABLE, submission_id, session):
        raise UserMisMatch()
    sql_errors = sqlalchemy.delete(ERRORS_TABLE).where(ERRORS_TABLE.submission_id == submission_id)
    sql_commission = sqlalchemy.delete(COMMISSION_DATA_TABLE).where(COMMISSION_DATA_TABLE.submission_id == submission_id)
    sql_submission = sqlalchemy.delete(SUBMISSIONS_TABLE).where(SUBMISSIONS_TABLE.id == submission_id)
    session.execute(sql_commission)
    session.execute(sql_errors)
    session.execute(sql_submission)
    session.commit()
    return

@jsonapi_error_handling
def modify_submission(session: Session, submission_id: int, submission_obj: dict, user: User):
    if not matched_user(user, SUBMISSIONS_TABLE, submission_id, session):
        raise UserMisMatch()
    model_name = hyphenated_name(SUBMISSIONS_TABLE)
    hyphenate_json_obj_keys(submission_obj)
    return models.serializer.patch_resource(session, submission_obj, model_name, submission_id).data


def record_auto_matched_strings(db: Session, user_id: int, data: pd.DataFrame) -> pd.DataFrame:
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
        .returning(
            ID_STRINGS.id,
            ID_STRINGS.match_string,
            ID_STRINGS.report_id,
            ID_STRINGS.customer_branch_id
        )
    return_results = db.execute(insert_stmt).mappings().all()
    db.commit()
    return pd.DataFrame(return_results).rename(columns={
            "id": "report_branch_ref",
            "match_string": "id_string"
        })

def record_final_data(db: Session, data: pd.DataFrame) -> None:
    data_records = data.to_dict(orient="records")
    sql = sqlalchemy.insert(COMMISSION_DATA_TABLE)
    db.execute(sql, data_records) # for bulk insert per SQLAlchemy docs
    db.commit()
    return

def record_submission(db: Session, submission: NewSubmission) -> int:
    sql = sqlalchemy.insert(SUBMISSIONS_TABLE).returning(SUBMISSIONS_TABLE.id)\
            .values(**submission)
    result = db.execute(sql).fetchone()[0]
    db.commit()
    return result

def record_error(db: Session, error_obj: Error) -> None:
    """record errors into the current_errors table"""
    sql = sqlalchemy.insert(ERRORS_TABLE).values(**error_obj)
    db.execute(sql)
    db.commit()
    return

def delete_errors(db: Session, record_ids: int|list):
    if isinstance(record_ids, int):
        record_ids = [record_ids]
    for record_id in record_ids:
        record_id = int(record_id)
        sql = sqlalchemy.delete(ERRORS_TABLE).where(ERRORS_TABLE.id == record_id)
        db.execute(sql)
    db.commit()

def generate_file_record(db: Session, record: dict):
    sql = sqlalchemy.insert(DOWNLOADS).values(**record)
    with db as session:
        session.execute(sql)
        session.commit()

def mark_file_downloaded(db: Session, hash: str):
    sql = sqlalchemy.update(DOWNLOADS).values(downloaded = True).where(DOWNLOADS.hash == hash)
    with db as session:
        session.execute(sql)
        session.commit()

@jsonapi_error_handling
def __create_X(db: Session, json_data: dict, user: User, model: models.Base) -> JSONAPIResponse:
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
def create_customer(db: Session, json_data: dict, user: User) -> JSONAPIResponse:
    new_name: str = json_data["data"]["attributes"]["name"]
    json_data["data"]["attributes"]["name"] = new_name.upper().strip()
    return __create_X(db, json_data, user, CUSTOMERS)


@jsonapi_error_handling
def create_branch(db: Session, json_data: dict, user: User) -> JSONAPIResponse:
    return __create_X(db, json_data, user, BRANCHES)


@jsonapi_error_handling
def create_mapping(db: Session, json_data: dict, user: User) -> JSONAPIResponse:
    return __create_X(db, json_data, user, ID_STRINGS)


@jsonapi_error_handling
def create_manufacturer(db: Session, json_data: dict, user: User) -> JSONAPIResponse:
    return __create_X(db, json_data, user, MANUFACTURERS)

@jsonapi_error_handling
def create_representative(db: Session, json_data: dict, user: User) -> JSONAPIResponse:
    return __create_X(db, json_data, user, REPS)

@jsonapi_error_handling
def modify_customer_jsonapi(db: Session, customer_id: int, json_data: dict, user: User) -> JSONAPIResponse:
    if not matched_user(user, CUSTOMERS, customer_id, db):
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
def modify_branch(db: Session, branch_id: int, json_data: dict, user: User) -> JSONAPIResponse:
    if not matched_user(user, BRANCHES, branch_id, db):
        raise UserMisMatch()
    model_name = hyphenated_name(BRANCHES)
    hyphenate_json_obj_keys(json_data)
    return models.serializer.patch_resource(db, json_data, model_name, branch_id).data

@jsonapi_error_handling
def modify_rep(db: Session, rep_id: int, json_data: dict, user: User) -> JSONAPIResponse:
    if not matched_user(user, REPS, rep_id, db):
        raise UserMisMatch()
    model_name = hyphenated_name(REPS)
    hyphenate_json_obj_keys(json_data)
    return models.serializer.patch_resource(db, json_data, model_name, rep_id).data

@jsonapi_error_handling
def delete_a_branch(db: Session, branch_id: int) -> None:
    _now = datetime.utcnow()
    db.execute("UPDATE customer_branches SET deleted = :current_time WHERE id = :branch_id", {"branch_id": branch_id, "current_time": _now})
    db.commit()
    return

@jsonapi_error_handling
def delete_mapping(db: Session, mapping_id: int) -> None:
    # TODO should be soft delete if an id string will ultimately become the link between customer info and commission_data
    sql = sqlalchemy.delete(ID_STRINGS).where(ID_STRINGS.id == mapping_id)
    db.execute(sql)
    db.commit()
    return

@jsonapi_error_handling
def delete_customer(db: Session, customer_id: int) -> None:
    current_time = datetime.utcnow()
    sql = """UPDATE customers SET deleted = :current_time WHERE id = :customer_id;"""
    db.execute(sql, {"current_time": current_time, "customer_id": customer_id})
    db.commit()
    return

@jsonapi_error_handling
def delete_manufacturer(db: Session, manuf_id: int, user: User) -> None:
    return __soft_delete(db=db, table=MANUFACTURERS, _id=manuf_id, user=user)

@jsonapi_error_handling
def delete_representative(db: Session, rep_id: int, user: User) -> None:
    return __soft_delete(db=db, table=REPS, _id=rep_id, user=user)

@jsonapi_error_handling
def delete_commission_data_line(db: Session, row_id: int, user: User) -> None:
    if not matched_user(user, COMMISSION_DATA_TABLE, row_id, db):
        raise UserMisMatch()
    sql = sqlalchemy.delete(COMMISSION_DATA_TABLE).where(COMMISSION_DATA_TABLE.row_id == row_id)
    db.execute(sql)
    db.commit()
    return

def __soft_delete(db: Session, table: models.Base, _id: int, user: User):
    if not matched_user(user, table, _id, db):
        raise UserMisMatch()
    current_time = datetime.utcnow()
    sql = f"""UPDATE {table.__tablename__} SET deleted = :current_time WHERE id = :_id;"""
    db.execute(sql, {"current_time": current_time, "_id": _id})
    db.commit()



def alter_sub_status(db: Session, submission_id: int, status: str) -> bool:
    sql = sqlalchemy.update(SUBMISSIONS_TABLE).values(status=status).where(SUBMISSIONS_TABLE.id==submission_id)
    try:
        db.execute(sql)
        db.commit()
    except:
        return False
    else:
        return True
