"""Contains all patch/update methods for use by the higher level methods to
modify data in a database"""

from services.utils import *
import sqlalchemy
from jsonapi.jsonapi import jsonapi_error_handling, JSONAPIResponse
from app import event


@jsonapi_error_handling
def submission(session: Session, submission_id: int, submission_obj: dict, user: User):
    if not matched_user(user, SUBMISSIONS_TABLE, submission_id, session):
        raise UserMisMatch()
    model_name = hyphenated_name(SUBMISSIONS_TABLE)
    hyphenate_json_obj_keys(submission_obj)
    return models.serializer.patch_resource(session, submission_obj, model_name, submission_id).data

def file_downloads(db: Session, hash: str):
    sql = sqlalchemy.update(DOWNLOADS).values(downloaded = True).where(DOWNLOADS.hash == hash)
    with db as session:
        session.execute(sql)
        session.commit()

@jsonapi_error_handling
def customer(db: Session, customer_id: int, json_data: dict, user: User) -> JSONAPIResponse:
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
def branch(db: Session, branch_id: int, json_data: dict, user: User) -> JSONAPIResponse:
    if not matched_user(user, BRANCHES, branch_id, db):
        raise UserMisMatch()
    model_name = hyphenated_name(BRANCHES)
    hyphenate_json_obj_keys(json_data)
    return models.serializer.patch_resource(db, json_data, model_name, branch_id).data

@jsonapi_error_handling
def representative(db: Session, rep_id: int, json_data: dict, user: User) -> JSONAPIResponse:
    if not matched_user(user, REPS, rep_id, db):
        raise UserMisMatch()
    model_name = hyphenated_name(REPS)
    hyphenate_json_obj_keys(json_data)
    return models.serializer.patch_resource(db, json_data, model_name, rep_id).data

def sub_status(db: Session, submission_id: int, status: str) -> bool:
    sql = sqlalchemy.update(SUBMISSIONS_TABLE).values(status=status).where(SUBMISSIONS_TABLE.id==submission_id)
    try:
        db.execute(sql)
        db.commit()
    except:
        return False
    else:
        return True

@jsonapi_error_handling
def commission_data_row(db: Session, row_id: int, **kwargs):
    sql = sqlalchemy.update(COMMISSION_DATA_TABLE) \
            .values(**kwargs).where(COMMISSION_DATA_TABLE.row_id == row_id)
    db.execute(sql)
    db.commit()
    return

@jsonapi_error_handling
def mapping(db: Session, mapping_id: int, json_data: dict, user: User) -> JSONAPIResponse:
    if not matched_user(user, BRANCHES, mapping_id, db):
        raise UserMisMatch()
    model_name = hyphenated_name(ID_STRINGS)
    hyphenate_json_obj_keys(json_data)
    return models.serializer.patch_resource(db, json_data, model_name, mapping_id).data


def change_commission_data_customer_branches(db: Session, report_branch_ref_id: int, customer_branch_id: int) -> None:
    sql = (
        sqlalchemy.update(COMMISSION_DATA_TABLE)
        .values(customer_branch_id = customer_branch_id)
        .where(COMMISSION_DATA_TABLE.report_branch_ref == report_branch_ref_id)
    )
    db.execute(sql)
    db.commit()
    return