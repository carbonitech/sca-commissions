from typing import List
from pandas import DataFrame
from app import event
from entities import error
from entities.processing_step import ProcessingStep
from db.db_services import DatabaseServices

def handle_errors_recorded(error_list: List[error.Error], sub_id: int, *args, **kwargs) -> None:
    num_rows = len(error_list)
    row_index_list = [err.row_index for err in error_list]

    desc_str = f"{num_rows} rows failed to match to standard references due to {error_list[0].reason.name}.\n" \
                f"Rows indices: {row_index_list}"

    if start_step := kwargs.get("start_step"):
        ProcessingStep.set_total_step_num(start_step)
        
    db = DatabaseServices()
    db.record_processing_step(ProcessingStep(desc_str, sub_id=sub_id))


def handle_rows_removed(data_removed: DataFrame, sub_id: int, *args, **kwargs) -> None:
    if data_removed.empty: return
    num_rows = len(data_removed)
    row_index_list = data_removed.index.to_list()
    sum_inv_removed = data_removed.loc[:,"inv_amt"].sum()/100
    sum_comm_removed = data_removed.loc[:,"comm_amt"].sum()/100

    desc_str = f"{num_rows} rows removed from commission data.\n"\
            f"Rows removed: {row_index_list}\n" \
            f"Total Inv_amt reduced by ${sum_inv_removed:,.2f} and Comm_amt reduced by ${sum_comm_removed:,.2f}"

    if start_step := kwargs.get("start_step"):
        ProcessingStep.set_total_step_num(start_step)

    db = DatabaseServices()
    db.record_processing_step(ProcessingStep(desc_str,sub_id))


def handle_data_formatting(msg: str, sub_id: int, *args, **kwargs) -> None:
    if start_step := kwargs.get("start_step"):
        ProcessingStep.set_total_step_num(start_step)
    
    db = DatabaseServices()
    db.record_processing_step(ProcessingStep(msg,sub_id))


def handle_comm_data_recorded(data: DataFrame, sub_id: int, *args, **kwargs) -> None:
    num_rows = len(data)
    sum_inv_removed = data.loc[:,"inv_amt"].sum()/100
    sum_comm_removed = data.loc[:,"comm_amt"].sum()/100

    desc_str = f"{num_rows} rows written to the database.\n"\
            f"Total Inv_amt = ${sum_inv_removed:,.2f}\n"\
            f"Comm_amt = ${sum_comm_removed:,.2f}"
    
    if start_step := kwargs.get("start_step"):
        ProcessingStep.set_total_step_num(start_step)
        
    db = DatabaseServices()
    db.record_processing_step(ProcessingStep(desc_str,sub_id))


def handle_mapping_table_updated(data) -> None:
    ...


def setup_processing_step_handlers():
    event.subscribe("Errors Recorded", handle_errors_recorded)
    event.subscribe("Rows Removed", handle_rows_removed)
    event.subscribe("Formatting", handle_data_formatting)
    event.subscribe("Data Recorded", handle_comm_data_recorded)
