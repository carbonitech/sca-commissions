"""Contains all delete methods for use by the higher level methods to
remove or soft-delete data (actually an UPDATE) from a database"""

from services.utils import *
from jsonapi.jsonapi import jsonapi_error_handling
from datetime import datetime
import sqlalchemy

@jsonapi_error_handling
def submission(submission_id: int, session: Session, user: User) -> None:
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
def errors(db: Session, record_ids: int|list):
    if isinstance(record_ids, int):
        record_ids = [record_ids]
    for record_id in record_ids:
        record_id = int(record_id)
        sql = sqlalchemy.delete(ERRORS_TABLE).where(ERRORS_TABLE.id == record_id)
        db.execute(sql)
    db.commit()

@jsonapi_error_handling
def branch(db: Session, branch_id: int) -> None:
    _now = datetime.utcnow()
    db.execute("UPDATE customer_branches SET deleted = :current_time WHERE id = :branch_id", {"branch_id": branch_id, "current_time": _now})
    db.commit()
    return

@jsonapi_error_handling
def mapping(db: Session, mapping_id: int) -> None:
    # TODO should be soft delete if an id string will ultimately become the link between customer info and commission_data
    sql = sqlalchemy.delete(ID_STRINGS).where(ID_STRINGS.id == mapping_id)
    db.execute(sql)
    db.commit()
    return

@jsonapi_error_handling
def customer(db: Session, customer_id: int) -> None:
    current_time = datetime.utcnow()
    sql = """UPDATE customers SET deleted = :current_time WHERE id = :customer_id;"""
    db.execute(sql, {"current_time": current_time, "customer_id": customer_id})
    db.commit()
    return

@jsonapi_error_handling
def manufacturer(db: Session, manuf_id: int, user: User) -> None:
    return __soft_delete(db=db, table=MANUFACTURERS, _id=manuf_id, user=user)

@jsonapi_error_handling
def representative(db: Session, rep_id: int, user: User) -> None:
    return __soft_delete(db=db, table=REPS, _id=rep_id, user=user)

@jsonapi_error_handling
def commission_data_line(db: Session, row_id: int, user: User) -> None:
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
