from typing import List
from pandas import DataFrame

from app import event
from entities import error
from db import db_services


def error_factory(error_data: DataFrame, type_: int) -> List[error.Error]:
    result_list = []
    for row_index, row_data in error_data.to_dict("index").items():
        row_index: int
        row_data: dict
        error_obj = error.Error(
            submission_id=row_data["submission_id"],
            row_index=row_index,
            reason=error.ErrorType(type_),
            row_data=row_data)
        result_list.append(error_obj)
    return result_list

def no_customer_error(data_affected: DataFrame) -> None:
    if data_affected.empty: return
    errors = error_factory(data_affected,1)
    db = db_services.DatabaseServices()
    for error in errors:
        db.record_error(error)
    sub_id = errors[0].row_data["submission_id"]
    event.post_event("Errors Recorded", errors, sub_id)


def no_city_error(data_affected: DataFrame) -> None:
    if data_affected.empty: return
    errors = error_factory(data_affected,2)
    db = db_services.DatabaseServices()
    for error in errors:
        db.record_error(error)
    sub_id = errors[0].row_data["submission_id"]
    event.post_event("Errors Recorded", errors, sub_id)

def no_state_error(data_affected: DataFrame) -> None:
    if data_affected.empty: return
    errors = error_factory(data_affected,3)
    db = db_services.DatabaseServices()
    for error in errors:
        db.record_error(error)
    sub_id = errors[0].row_data["submission_id"]
    event.post_event("Errors Recorded", errors, sub_id)

def no_branch_error(data_affected: DataFrame) -> None:
    if data_affected.empty: return
    errors = error_factory(data_affected,4)
    db = db_services.DatabaseServices()
    for error in errors:
        db.record_error(error)
    sub_id = errors[0].row_data["submission_id"]
    event.post_event("Errors Recorded", errors, sub_id)

def no_rep_assigned_error(data_affected: DataFrame) -> None:
    if data_affected.empty: return
    errors = error_factory(data_affected,5)
    db = db_services.DatabaseServices()
    for error in errors:
        db.record_error(error)
    sub_id = errors[0].row_data["submission_id"]
    event.post_event("Errors Recorded", errors, sub_id)


def setup_error_event_handlers():
    event.subscribe(error.ErrorType(1), no_customer_error)
    event.subscribe(error.ErrorType(2), no_city_error)
    event.subscribe(error.ErrorType(3), no_state_error)
    event.subscribe(error.ErrorType(4), no_branch_error)
    event.subscribe(error.ErrorType(5), no_rep_assigned_error)