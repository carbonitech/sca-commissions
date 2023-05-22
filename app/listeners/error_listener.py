from typing import List
from pandas import DataFrame

from app import event
from entities import error
from services.api_adapter import ApiAdapter


def error_factory(error_data: DataFrame, type_: int, submission_id: int, user_id: int) -> List[error.Error]:
    result_list = []
    for row_data in error_data.to_dict("index").values():
        row_data: dict
        error_obj = error.Error(
            submission_id=submission_id,
            reason=error.ErrorType(type_),
            row_data=row_data,
            user_id=user_id)
        result_list.append(error_obj)
    return result_list

def no_customer_error(data_affected: DataFrame, submission_id: int, *args, **kwargs) -> None:
    return error_handler(data_affected, submission_id, *args, error_type=1, **kwargs)

def no_branch_error(data_affected: DataFrame, submission_id: int, *args, **kwargs) -> None:
    return error_handler(data_affected, submission_id, *args, error_type=4, **kwargs)

def no_rep_assigned_error(data_affected: DataFrame, submission_id: int, *args, **kwargs) -> None:
    return error_handler(data_affected, submission_id, *args, error_type=5, **kwargs)

def error_handler(data_affected: DataFrame, submission_id: int, *args, **kwargs):
    if not isinstance(data_affected, DataFrame):
        return
    if data_affected.empty:
        return
    session = kwargs.get("session")
    user_id = kwargs.get("user_id")
    errors = error_factory(data_affected,kwargs.get("error_type"),submission_id, user_id)
    api = ApiAdapter()
    for error in errors:
        api.record_error(session, error)
    


def setup_error_event_handlers():
    event.subscribe(error.ErrorType(1), no_customer_error)
    event.subscribe(error.ErrorType(4), no_branch_error)
    event.subscribe(error.ErrorType(5), no_rep_assigned_error)