from datetime import datetime
from typing import Hashable, Union, Type
import pandas as pd
from sqlalchemy.orm import Session

from app import event
from entities.preprocessor import AbstractPreProcessor
from entities.commission_data import PreProcessedData
from entities.submission import NewSubmission
from entities.error import ErrorType
from services import api_adapter


class EmptyTableException(Exception):
    pass

class FileProcessingError(Exception):
    def __init__(self, *args: object, **kwargs) -> None:
        super().__init__(*args)
        self.submission_id: int = kwargs.get("submission_id")

class ReportProcessor:
    """
    Handles processing of data delivered through a preprocessor, which itself recieves the file
    and does manufacturer-specific preprocessing steps. The preprocessor is expected to return the same format 
    for all manufacturers.

    Data Processing is achieved by gathering name mappings (Customer, City, State, and Branches)
    and using those mappings to find id numbers in the database. On the happy path, commission 
    data (invoiced and commission amounts) are recorded in the database with an id and a branch id.
    If a mapping match isn't found for names (Customer, City, State), the data is placed in an 'errors'
    table as-is for reprocessing attempts later. If a row maps all names but the ids aren't associated with one
    another as a branch, a new branch is added to the database without a rep assignment. (While this 
    does need user attention to add a rep assignment, it's not going to the error's table)

    """
    def __init__(
            self,
            session: Session,
            user: api_adapter.User,
            preprocessor: Type[AbstractPreProcessor] = None,
            submission: NewSubmission = None,
            target_err: ErrorType = None,
            error_table: pd.DataFrame = pd.DataFrame(),
            submission_id: int|None = None
        ):

        self.skip = False
        self.reintegration = False
        self.inter_warehouse_transfer = False
        self.session = session
        self.api = api_adapter.ApiAdapter()
        self.user_id = user.id(self.session) if user.verified else None
        self.submission_id = submission_id

        if not submission_id:
            del self.submission_id
        
        if preprocessor and submission:
            if issubclass(preprocessor, AbstractPreProcessor) and isinstance(submission, NewSubmission):
                self.submission = submission
                self.preprocessor = preprocessor
                self.report_id = submission.report_id
                self.standard_commission_rate = self.api.get_commission_rate(session, submission.manufacturer_id, user_id=self.user_id)
                self.split = self.api.get_split(session, submission.report_id, user_id=self.user_id)

        elif target_err and isinstance(error_table, pd.DataFrame):
            if isinstance(target_err, ErrorType):
                self.reintegration = True
                self.target_err = target_err
                self.error_table: pd.DataFrame = pd.concat(   # expand row_data into dataframe columns
                    [
                        error_table, 
                        pd.json_normalize(error_table.pop("row_data"),max_level=1)
                    ], 
                    axis=1)
                self.error_table = self.error_table.reset_index(drop=True)
        else:
            self.skip = True
            return

        self.branches = self.api.get_branches(session, user_id=self.user_id)
        self.id_string_matches = self.api.get_id_string_matches(session, user_id=self.user_id)

    def _send_event_by_submission(self, data: pd.DataFrame, event_: Hashable) -> None:
        
        for sub_id in data["submission_id"].unique().tolist():
            mask = data["submission_id"] == sub_id
            sub_id_table = data.loc[mask,:]
            sub_id_table = sub_id_table.loc[:,~sub_id_table.columns.isin(['submission_id','id','reason','user_id'])]
            event.post_event(
                event_,
                sub_id_table,
                submission_id=sub_id,
                user_id=self.user_id,
                session=self.session
            )


    def _filter_for_existing_records_with_target_error_type(self) -> 'ReportProcessor':
        mask = self.error_table["reason"] == self.target_err.value
        table_target_errors = self.error_table.loc[mask]
        self.error_table = table_target_errors.reset_index(drop=True) # fixes for id merging strategy
        self.staged_data = self.error_table.copy()
        if table_target_errors.empty:
            raise EmptyTableException
        return self

    def remove_error_db_entries(self) -> 'ReportProcessor':
        self.api.delete_errors(db=self.session, record_ids=self.staged_data["id"].to_list())
        return self

    def add_branch_id(self, data=pd.DataFrame(), pipe=True) -> Union['ReportProcessor',pd.DataFrame]:
        """
        Adds the customer's branch id, if the assignment exists.
        Un-matched rows are added to the customer_branches table

        """
        new_column: str = "customer_branch_id"

        if not pipe:
            operating_data = data
        else:
            operating_data = self.staged_data

        if operating_data.empty:
            operating_data["customer_branch_id"] = None
            return self if pipe else operating_data

        merged_with_branches = pd.merge(
                operating_data, self.id_string_matches[self.id_string_matches["report_id"] == self.report_id],
                how="left", left_on="id_string",
                right_on="match_string",
                suffixes=(None,"_ref_table")
        ) 
        
        new_col_values = merged_with_branches.loc[:,"customer_branch_id"].fillna(0).astype(int).to_list()

        operating_data.loc[:, new_column] = new_col_values

        if pipe:
            operating_data = self._filter_out_any_rows_unmapped(operating_data)
            self.staged_data = operating_data
            return self
        else:
            return operating_data

    def assign_value_by_transfer_direction(self) -> 'ReportProcessor':
        """
        if receiver (+)
        if sender (-)
        """
        for money_col in ["inv_amt", "comm_amt"]:
            self.staged_data.loc[:,money_col] = self.staged_data.apply(
                lambda row: row[money_col] if row["direction"] == "receiving" else -row[money_col],
                axis=1
            )
        return self


    def _filter_out_any_rows_unmapped(self, data: pd.DataFrame, error_type: ErrorType) -> pd.DataFrame:
        if data.empty:
            return data
        mask = data.loc[:,~data.columns.isin(["submission_id","inv_amt","comm_amt","row_index"])].all('columns')
        data_remaining = data[mask]
        data_removed = data[~mask]
        self._send_event_by_submission(data_removed, error_type)
        return data_remaining


    def drop_extra_columns(self) -> 'ReportProcessor':
        self.staged_data = self.staged_data.loc[:,["submission_id","customer_branch_id","inv_amt","comm_amt","user_id"]]
        return self

    def register_commission_data(self) -> 'ReportProcessor':
        if self.staged_data.empty:
            # my method for removing rows checks for existing rows with falsy values.
            # Avoid writing a blank row in the database from an empty dataframe
            return self
        else:
            self.staged_data = self.staged_data.dropna() # just in case
        self.api.record_final_data(db=self.session, data=self.staged_data)
        return self

    def set_switches(self) -> 'ReportProcessor':
        if self.reintegration:
            col_list = self.staged_data.columns.to_list()
        else:
            col_list = self.ppdata.data.columns.to_list()

        match col_list:
            case [*other_cols, "direction"]:
                self.inter_warehouse_transfer = True
        
        return self

    def preprocess(self) -> 'ReportProcessor':
        report_name = self.api.get_report_name_by_id(db=self.session, report_id=self.submission.report_id)
        sub_id = self.submission_id
        file = self.submission.file
        preprocessor: AbstractPreProcessor = self.preprocessor(report_name, sub_id, file)
        optional_params = {
            "total_freight_amount": self.submission.total_freight_amount,
            "total_commission_amount": self.submission.total_commission_amount,
            "additional_file_1": self.submission.additional_file_1,
            "standard_commission_rate": self.standard_commission_rate,
            "split": self.split,
        }
        try:
            ppdata: PreProcessedData = preprocessor.preprocess(**optional_params)
        except Exception:
            raise FileProcessingError("There was an error attempting to process the file", submission_id=sub_id)

        self.ppdata = ppdata
        self.staged_data = ppdata.data.copy()
        return self

    def insert_submission_id(self) -> 'ReportProcessor':
        self.staged_data.insert(0,"submission_id",self.submission_id)
        return self

    def insert_recorded_at_column(self) -> 'ReportProcessor':
        self.staged_data["recorded_at"] = datetime.utcnow()
        return self

    def insert_user_id(self) -> 'ReportProcessor':
        self.staged_data["user_id"] = self.user_id
        return self

    def set_submission_status(self, status: str) -> 'ReportProcessor':
        if self.reintegration:
            table = self.staged_data
            for sub_id in table["submission_id"].unique().tolist():
                if self.session.execute("SELECT * FROM errors WHERE submission_id = :sub_id", {"sub_id": sub_id}).fetchone():
                    continue
                self.api.alter_sub_status(db=self.session, submission_id=sub_id, status=status)
            return self

        self.api.alter_sub_status(db=self.session, submission_id=self.submission_id, status=status)
        return self


    def process_and_commit(self) -> int|None:
        """
        Taking preprocessed data, use reference tables from the database
        to map customer names, city names, state names, and reps
        by id numbers

        Effects: commits the submission data, final commission data, errors, and processing steps
                to the database 
        """
        if self.skip:
            return
        try:
            if not self.reintegration:
                self.set_submission_status("PROCESSING")\
                    .preprocess()\
                    .insert_submission_id()\
                    .insert_user_id()
            else:
                self._filter_for_existing_records_with_target_error_type()\
                    .remove_error_db_entries()
            self.set_switches()
            if self.inter_warehouse_transfer:
                self.assign_value_by_transfer_direction()
            else:
                    self.add_branch_id()
                    print(self.staged_data)
        except EmptyTableException:
            self.set_submission_status("NEEDS_ATTENTION")
        except Exception as err:
            self.set_submission_status("FAILED")
            import traceback
            # BUG background task conversion up-stack means now I'm losing the capture of this traceback
            # so while this error is raised, nothing useful is happening with it currently
            # this print is so I can see it in heroku logs
            print(f"from print: {traceback.format_exc()}")
            raise FileProcessingError(err, submission_id=self.submission_id if self.submission_id else None)
        else:
            self.drop_extra_columns()\
                .insert_recorded_at_column()\
                .register_commission_data()\
                .set_submission_status("COMPLETE")
        return self.submission_id if not self.reintegration else None