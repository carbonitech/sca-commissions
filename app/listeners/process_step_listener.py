from typing import List
from pandas import DataFrame
from sqlalchemy.orm import Session
from app import event
from entities import error
from entities.processing_step import ProcessingStep
from services.api_adapter import ApiAdapter

api = ApiAdapter()

def handle_errors_recorded(error_list: List[error.Error], submission_id: int, user_id: int, *args, **kwargs) -> None:
    num_rows = len(error_list)
    desc_str = f"{num_rows} rows failed to match to standard references due to {error_list[0].reason.name}."
    if start_step := kwargs.get("start_step"):
        ProcessingStep.set_total_step_num(start_step)
        
    session: Session = kwargs.get("session")
    api.record_processing_step(session,ProcessingStep(desc_str, sub_id=submission_id, user_id=user_id))


def handle_rows_removed(data_removed: DataFrame, submission_id: int, user_id: int, *args, **kwargs) -> None:
    if data_removed.empty: return
    num_rows = len(data_removed)
    sum_inv_removed = data_removed.loc[:,"inv_amt"].sum()/100
    sum_comm_removed = data_removed.loc[:,"comm_amt"].sum()/100

    desc_str = f"{num_rows} rows dropped."\
            f" Total Inv_amt excluded: ${sum_inv_removed:,.2f} and Comm_amt excluded: ${sum_comm_removed:,.2f}"

    if start_step := kwargs.get("start_step"):
        ProcessingStep.set_total_step_num(start_step)

    session: Session = kwargs.get("session")
    api.record_processing_step(session,ProcessingStep(desc_str,submission_id, user_id=user_id))


def handle_data_formatting(msg: str, submission_id: int, user_id: int, *args, **kwargs) -> None:
    if start_step := kwargs.get("start_step"):
        ProcessingStep.set_total_step_num(start_step)
    
    session: Session = kwargs.get("session")
    api.record_processing_step(session,ProcessingStep(msg,submission_id, user_id=user_id))


def handle_comm_data_recorded(data: DataFrame, submission_id: int, user_id: int, *args, **kwargs) -> None:
    num_rows = len(data)
    sum_inv = data.loc[:,"inv_amt"].sum()/100
    sum_comm = data.loc[:,"comm_amt"].sum()/100

    desc_str = f"{num_rows} rows written to commission data table."\
            f" Total Sales added: ${sum_inv:,.2f}"\
            f" Total Commissions added = ${sum_comm:,.2f}"
    
    if start_step := kwargs.get("start_step"):
        ProcessingStep.set_total_step_num(start_step)
        
    session: Session = kwargs.get("session")
    api.record_processing_step(session,ProcessingStep(desc_str,submission_id, user_id=user_id))


def handle_mapping_table_updated(data, submission_id, user_id: int, *args, **kwargs) -> None:
    if start_step := kwargs.get("start_step"):
        ProcessingStep.set_total_step_num(start_step)
    session: Session = kwargs.get("session")
    api.record_processing_step(session,ProcessingStep(data,submission_id, user_id=user_id))


def setup_processing_step_handlers():
    event.subscribe("Errors Recorded", handle_errors_recorded)
    event.subscribe("Rows Removed", handle_rows_removed)
    event.subscribe("Formatting", handle_data_formatting)
    event.subscribe("Data Recorded", handle_comm_data_recorded)
    event.subscribe("Reprocessing", handle_mapping_table_updated)


