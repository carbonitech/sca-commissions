from typing import List
from pandas import DataFrame
from app import event
from entities import error
from entities.processing_step import ProcessingStep
from db.db_services import DatabaseServices

def handle_errors_recorded(error_list: List[error.Error]) -> None:
    num_rows = len(error_list)
    row_index_list = [err.row_index for err in error_list]

    desc_str = f"{num_rows} rows failed to match to standard references due to {error_list[0].reason.name}.\n" \
                f"Rows indices: {row_index_list}"

    db = DatabaseServices()
    db.record_processing_step(ProcessingStep(desc_str))

def handle_rows_removed(data_removed: DataFrame) -> None:
    if data_removed.empty: return
    num_rows = len(data_removed)
    row_index_list = data_removed.index.to_list()
    sum_inv_removed = data_removed.loc[:,"inv_amt"].sum()/100
    sum_comm_removed = data_removed.loc[:,"comm_amt"].sum()/100

    desc_str = f"{num_rows} rows removed from commission data.\n"\
            f"Rows removed: {row_index_list}\n" \
            f"Total Inv_amt reduced by ${sum_inv_removed:,.2f} and Comm_amt reduced by ${sum_comm_removed:,.2f}"
    
    db = DatabaseServices()
    db.record_processing_step(ProcessingStep(desc_str))

def handle_data_formatting(msg: str) -> None:
    db = DatabaseServices()
    db.record_processing_step(ProcessingStep(msg))  


def setup_processing_step_handlers():
    event.subscribe("Errors Recorded", handle_errors_recorded)
    event.subscribe("Rows Removed", handle_rows_removed)
    event.subscribe("Formatting", handle_data_formatting)
