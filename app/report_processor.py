from datetime import datetime
from typing import Hashable, Union, Type
import pandas as pd
from sqlalchemy.orm import Session
from Levenshtein import ratio, jaro_winkler

from app import event
from entities.preprocessor import AbstractPreProcessor
from entities.commission_data import PreProcessedData
from entities.submission import NewSubmission
from entities.error import ErrorType
from services import api_adapter


AUTO_MATCH_THRESHOLD = 0.75     # based on the eye-ball test
PREFIX_WEIGHT = 0.3             # ditto

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

    Alternatively, this class is used to attempt to integrate data that was previously kicked out due to no
    existing mapping.

    Output: Submission ID or None
            Either commission data, failed matches, or both, are written to a database

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
        self.id_sting_match_supplement = self.api.generate_string_match_supplement(session, user_id=self.user_id)
        self.id_string_matches = self.api.get_id_string_matches(session, user_id=self.user_id)

    def _send_event_by_submission(self, data: pd.DataFrame, event_: Hashable) -> None:
        """
        Seperates data by submission ID before posting an event specific to that submission
        """
        for sub_id in data["submission_id"].unique().tolist():
            mask = data["submission_id"] == sub_id
            sub_id_table = data.loc[mask, data.columns.isin(
                    ['customer', 'city', 'state', 'inv_amt', 'comm_amt', 'id_string', 'direction']
                )]
            event.post_event(
                event_,
                sub_id_table,
                submission_id=sub_id,
                user_id=self.user_id,
                session=self.session
            )


    def _filter_for_existing_records_with_target_error_type(self) -> 'ReportProcessor':
        mask = self.error_table["reason"] == self.target_err.value
        table_target_errors = self.error_table.loc[mask,:]
        self.error_ids = table_target_errors["id"].to_list()
        self.error_table = table_target_errors.reset_index(drop=True) # fixes for id merging strategy
        self.staged_data = self.error_table.copy()
        self.staged_data = self.staged_data.loc[:,
                                self.staged_data.columns.isin(["submission_id", "user_id", "id_string", "inv_amt", "comm_amt", "direction"])
                            ]
        if table_target_errors.empty:
            raise EmptyTableException
        self.report_id_by_submission = self.api.report_id_by_submission(
                self.session,
                user_id=self.user_id,
                sub_ids=self.staged_data.loc[:,"submission_id"].unique().tolist()
            )
        return self

    def remove_error_db_entries(self) -> 'ReportProcessor':
        self.api.delete_errors(db=self.session, record_ids=self.error_ids)
        return self

    def add_branch_id(self, data=pd.DataFrame(), pipe=True) -> Union['ReportProcessor',pd.DataFrame]:
        """
        Adds the customer's branch id, if the assignment exists.
        Un-matched rows are added to the customer_branches table

        """
        new_column_cb_id: str = "customer_branch_id"
        new_column_id_string_id: str = "report_branch_ref"
        combined_new_cols = [new_column_cb_id, new_column_id_string_id]

        if not pipe:
            operating_data = data
        else:
            operating_data = self.staged_data

        if operating_data.empty:
            operating_data["customer_branch_id"] = None
            return self if pipe else operating_data

        merged_with_branches = pd.merge(
                operating_data, self.id_string_matches,
                how="left", left_on=["id_string", "report_id"],
                right_on=["match_string", "report_id"],
                suffixes=(None,"_ref_table")
        ) 
        new_column_cb_id_values = merged_with_branches.loc[:,"customer_branch_id"].fillna(0).astype(int).to_list()
        operating_data.loc[:, new_column_cb_id] = new_column_cb_id_values

        # 'id' is the id column of id_string_matches
        new_column_id_string_id_values = merged_with_branches.loc[:,"id"].fillna(0).astype(int).to_list()
        operating_data.loc[:, new_column_id_string_id] = new_column_id_string_id_values
        # attempt to auto-match the unmatched values
        unmatched_id_strings = operating_data.loc[operating_data["customer_branch_id"] == 0, ["id_string","customer_branch_id","report_id"]]
        if not unmatched_id_strings.empty:
            auto_matched = self.attempt_auto_matching(unmatched_rows=unmatched_id_strings)
            if not auto_matched.empty:
                auto_matched_index = auto_matched.index
                # fill the data with auto_matched values
                operating_data.loc[auto_matched_index, combined_new_cols] = auto_matched.loc[auto_matched_index, combined_new_cols]

        if pipe:
            operating_data = self._filter_out_any_rows_unmapped(operating_data, error_type = ErrorType(4))
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
                lambda row: row[money_col] if row["direction"] == "RECEIVING" else -row[money_col],
                axis=1
            )
        return self


    def _filter_out_any_rows_unmapped(self, data: pd.DataFrame, error_type: ErrorType) -> pd.DataFrame:
        if data.empty:
            return data
        mask = data.loc[:,~data.columns.isin(["submission_id","inv_amt","comm_amt"])].all('columns')
        data_remaining = data[mask]
        data_removed: pd.DataFrame = data.loc[~mask, ~data.columns.isin(['customer_branch_id'])]
        self._send_event_by_submission(data_removed, error_type)
        return data_remaining


    def drop_extra_columns(self) -> 'ReportProcessor':
        self.staged_data = self.staged_data.loc[:,["submission_id","customer_branch_id","inv_amt","comm_amt","user_id","report_branch_ref"]]
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
    
    def insert_report_id(self) -> 'ReportProcessor':
        if self.reintegration:
            new_col = self.staged_data.loc[:,["submission_id"]].merge(self.report_id_by_submission, how="left", on="submission_id").loc[:,["report_id"]]
            self.staged_data["report_id"] = new_col
        else:
            self.staged_data.insert(0,"report_id", self.report_id)

    def insert_recorded_at_column(self) -> 'ReportProcessor':
        self.staged_data["recorded_at"] = datetime.utcnow()
        return self

    def insert_user_id(self) -> 'ReportProcessor':
        self.staged_data["user_id"] = self.user_id
        return self

    def set_submission_status(self, status: str) -> 'ReportProcessor':
        """
        sets the status of the submission to an enum value.
        When an attempt is made to change the status, checks are made based on
        the processing scheme and status value provided to prevent erroneously assigned
        status values. Primary concern is that a submission will be labeled "COMPLETE" while
        processing errors are still found in the database
        """
        if self.reintegration:
            table = self.staged_data
            for sub_id in table["submission_id"].unique().tolist():
                if self.session.execute("SELECT * FROM errors WHERE submission_id = :sub_id", {"sub_id": sub_id}).fetchone():
                    continue
                self.api.alter_sub_status(db=self.session, submission_id=sub_id, status=status)
            return self
        elif status == 'COMPLETE':
            if self.session.execute("SELECT * FROM errors WHERE submission_id = :sub_id", {"sub_id": self.submission_id}).fetchone():
                return self
            else:
                self.api.alter_sub_status(db=self.session, submission_id=self.submission_id, status=status)
                return self
        else:
            self.api.alter_sub_status(db=self.session, submission_id=self.submission_id, status=status)
            return self
    def attempt_auto_matching(self, unmatched_rows: pd.DataFrame) -> pd.DataFrame:
        """automatically matches unmatched id_string values
            using a combination of methods with equal weights
            
            - normalized indel similarity in the range [0, 1], as (1 - normalized_distance)
            - jaro-winkler
            - jaro-winkler (reversed)
            
            Successfully matched values are registed in the database reference table
            Customer_branch_id's are returned as a pd.Series"""
        
        # columns in data: ["id_string","customer_branch_id","report_id"]
        
        # create reference table using a combination of existing match strings
        # and strings generated from the existing customer branches
        # both tables have matching columns for a union-like join
        ref_table = pd.concat([self.id_string_matches, self.id_sting_match_supplement], ignore_index=True)

        def score_unmatched(unmatched_value: str, *args, **kwargs) -> pd.Series:
            """Used for the apply function to score each unmatched row value against
                the reference table"""
            
            ref_table.loc[:,"indel"] = ref_table["match_string"].apply(lambda val: ratio(val,unmatched_value))
            ref_table.loc[:,"jaro_winkler"] = ref_table["match_string"].apply(lambda val: jaro_winkler(val,unmatched_value,prefix_weight=PREFIX_WEIGHT))
            ref_table.loc[:,"reverse_jaro_winkler"] = ref_table["match_string"].apply(lambda val: jaro_winkler(val[::-1],unmatched_value[::-1],prefix_weight=PREFIX_WEIGHT))
            # multiply first_n and full_string scores to calculate composite score
            ref_table["match_score"] = ref_table["indel"] * ref_table["jaro_winkler"] * ref_table["reverse_jaro_winkler"]

            # grab the customer_branch_id with the highest match_score and return the string
            over_threshold = ref_table["match_score"] > AUTO_MATCH_THRESHOLD
            max_value = ref_table["match_score"] == ref_table["match_score"].max()


            top_scoring_branch = ref_table.loc[
                    (over_threshold) & (max_value),
                    ["customer_branch_id", "match_score"],
                ]
            # if more than one match (likely duplicated data), take the lowest customer_branch_id
            top_scoring_branch = top_scoring_branch.loc[top_scoring_branch["customer_branch_id"] == top_scoring_branch["customer_branch_id"].min(),:]
            if top_scoring_branch.empty:
                return pd.Series([0,0])
            else:
                return pd.Series(top_scoring_branch.iloc[0].to_list())
            
        unmatched_rows[["customer_branch_id","match_score"]] = unmatched_rows["id_string"].apply(score_unmatched, result_type="expand")
        # columns in data: ["id_string","customer_branch_id","report_id", "match_score"]  -- customer_branch_id has been updated
        matched_rows = unmatched_rows[unmatched_rows["customer_branch_id"] > 0]
        if not matched_rows.empty:
            # columns in this data: ["report_branch_ref"/id, "id_string"/"match_string", "report_id", "customer_branch_id"]
            matches_w_ids = self.api.record_auto_matched_strings(db=self.session, user_id=self.user_id, data=matched_rows) # returns unique rows with id nums
            matched_rows = matched_rows\
                .reset_index()\
                .merge(matches_w_ids[["report_branch_ref","id_string","report_id"]],
                                        on=["id_string", "report_id"])
            # recover index after merge
            result = matched_rows.set_index("index")
            return result
        return pd.DataFrame()



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
                    .insert_user_id()\
                    .insert_report_id()
            else:
                self._filter_for_existing_records_with_target_error_type()\
                    .insert_report_id()
            self.set_switches()
            if self.inter_warehouse_transfer:
                self.assign_value_by_transfer_direction()
            else:
                self.add_branch_id()
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
            if self.reintegration:
                self.remove_error_db_entries()
            self.drop_extra_columns()\
                .insert_recorded_at_column()\
                .register_commission_data()\
                .set_submission_status("NEEDS_ATTENTION")\
                .set_submission_status("COMPLETE")
        return self.submission_id if not self.reintegration else None