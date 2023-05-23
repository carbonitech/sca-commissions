"""Contains all post/insert methods for use by the higher level methods to
add records to a database"""

from services.utils import *
from jsonapi.jsonapi import jsonapi_error_handling, JSONAPIResponse
from sqlalchemy.orm import Session
import sqlalchemy
from app import event
import pandas as pd
from datetime import datetime
from entities.error import Error
from entities.submission import NewSubmission

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
def customer(db: Session, json_data: dict, user: User) -> JSONAPIResponse:
    new_name: str = json_data["data"]["attributes"]["name"]
    json_data["data"]["attributes"]["name"] = new_name.upper().strip()
    return __create_X(db, json_data, user, CUSTOMERS)

@jsonapi_error_handling
def branch(db: Session, json_data: dict, user: User) -> JSONAPIResponse:
    return __create_X(db, json_data, user, BRANCHES)

@jsonapi_error_handling
def mapping(db: Session, json_data: dict, user: User) -> JSONAPIResponse:
    return __create_X(db, json_data, user, ID_STRINGS)

@jsonapi_error_handling
def manufacturer(db: Session, json_data: dict, user: User) -> JSONAPIResponse:
    return __create_X(db, json_data, user, MANUFACTURERS)

@jsonapi_error_handling
def representative(db: Session, json_data: dict, user: User) -> JSONAPIResponse:
    return __create_X(db, json_data, user, REPS)

def file_record(db: Session, record: dict):
    sql = sqlalchemy.insert(DOWNLOADS).values(**record)
    with db as session:
        session.execute(sql)
        session.commit()

def auto_matched_strings(db: Session, user_id: int, data: pd.DataFrame) -> pd.DataFrame:
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

def final_data(db: Session, data: pd.DataFrame) -> None:
    data_records = data.to_dict(orient="records")
    sql = sqlalchemy.insert(COMMISSION_DATA_TABLE)
    db.execute(sql, data_records) # for bulk insert per SQLAlchemy docs
    db.commit()
    return

def submission(db: Session, submission: NewSubmission) -> int:
    sql = sqlalchemy.insert(SUBMISSIONS_TABLE).returning(SUBMISSIONS_TABLE.id)\
            .values(**submission)
    result = db.execute(sql).fetchone()[0]
    db.commit()
    return result

def error(db: Session, error_obj: Error) -> None:
    """record errors into the current_errors table"""
    sql = sqlalchemy.insert(ERRORS_TABLE).values(**error_obj)
    db.execute(sql)
    db.commit()
    return

def set_new_commission_data_entry(db: Session, **kwargs) -> int:
    sql = sqlalchemy.insert(COMMISSION_DATA_TABLE)\
        .values(**kwargs).returning(COMMISSION_DATA_TABLE.row_id)
    result = db.execute(sql).scalar_one()
    db.commit()
    return result