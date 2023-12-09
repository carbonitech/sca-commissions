from datetime import datetime
from typing import Hashable, Type
import pandas as pd
from sqlalchemy.orm import Session
from Levenshtein import ratio, jaro_winkler

from app import event
from entities.preprocessor import AbstractPreProcessor
from entities.commission_data import PreProcessedData
from entities.submission import NewSubmission
from entities.error import ErrorType
from entities.user import User
from services import get, post, patch, delete


AUTO_MATCH_THRESHOLD = 0.75     # based on the eye-ball test
PREFIX_WEIGHT = 0.3             # ditto

class EmptyTableException(Exception):
    pass

class FileProcessingError(Exception):
    def __init__(self, *args: object, **kwargs) -> None:
        super().__init__(*args)
        self.submission_id: int = kwargs.get("submission_id")

class Processor:
    """
    Handles processing of data delivered through a preprocessor, which itself recieves the file
    and does manufacturer-specific preprocessing steps. The preprocessor is expected to return the same format 
    for all manufacturers.

    Alternatively, this class is used to attempt to integrate data that was previously kicked out due to no
    existing mapping.

    This class is extended by inheritance, once for a new report flow, and another for integration attempts on
    the values that were filtered out into errors from prior submissions
    """
    skip: bool
    inter_warehouse_transfer: bool
    session: Session
    user_id: int|None
    submission_id: int|None
    submission: NewSubmission
    preprocessor = Type[AbstractPreProcessor]
    report_id: int
    standard_commission_rate: float|None
    split: float
    target_err: ErrorType
    error_table: pd.DataFrame
    branches: pd.DataFrame
    id_sting_match_supplement: pd.DataFrame
    id_string_matches: pd.DataFrame
    territory: list[str]
    customer_branch_proportions: pd.DataFrame
    specified_customer: tuple[int,str]

    def __init__(self, session: Session, user_id: int):
        self.branches = get.branches(session, user_id=user_id)
        self.id_sting_match_supplement = get.string_match_supplement(session, user_id=user_id)
        self.id_string_matches = get.id_string_matches(session, user_id=user_id)

    def insert_report_id(self) -> 'Processor':
        pass

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


    def _filter_for_existing_records_with_target_error_type(self) -> 'Processor':
        mask = self.error_table["reason"] == self.target_err.value
        table_target_errors = self.error_table.loc[mask,:]
        self.error_ids = table_target_errors["id"].to_list()
        self.error_table = table_target_errors.reset_index(drop=True) # fixes for id merging strategy
        self.staged_data = self.error_table.copy()
        self.staged_data: pd.DataFrame = self.staged_data.loc[
                :,
                self.staged_data.columns.isin(["submission_id", "user_id", "id_string", "inv_amt", "comm_amt"])
            ]
        if table_target_errors.empty:
            raise EmptyTableException
        self.report_id_by_submission = get.report_id_by_submission(
                self.session,
                user_id=self.user_id,
                sub_ids=self.staged_data.loc[:,"submission_id"].unique().tolist()
            )
        return self

    def remove_error_db_entries(self) -> 'Processor':
        delete.errors(db=self.session, record_ids=self.error_ids)
        return self

    def add_branch_id(self, do_auto_match: bool=True) -> 'Processor':
        new_column_cb_id: str = "customer_branch_id"
        new_column_id_string_id: str = "report_branch_ref"
        combined_new_cols = [new_column_cb_id, new_column_id_string_id]

        operating_data = self.staged_data.copy()

        if operating_data.empty:
            operating_data["customer_branch_id"] = None
            return self

        merged_with_branches = pd.merge(
                operating_data, self.id_string_matches,
                how="left", left_on=["id_string", "report_id"],
                right_on=["match_string", "report_id"],
                suffixes=(None,"_ref_table")
        ) 
        print(merged_with_branches)
        new_column_cb_id_values = merged_with_branches.loc[:,"customer_branch_id"].fillna(0).astype(int).to_list()
        print(new_column_cb_id_values)
        operating_data.loc[:, new_column_cb_id] = new_column_cb_id_values
        print(operating_data)

        # 'id' is the id column of id_string_matches
        new_column_id_string_id_values = merged_with_branches.loc[:,"id"].fillna(0).astype(int).to_list()
        operating_data.loc[:, new_column_id_string_id] = new_column_id_string_id_values
        # attempt to auto-match the unmatched values
        unmatched_id_strings = operating_data.loc[operating_data["customer_branch_id"] == 0, ["id_string","customer_branch_id","report_id"]]
        if not unmatched_id_strings.empty and do_auto_match:
            auto_matched = self.attempt_auto_matching(unmatched_rows=unmatched_id_strings)
            if not auto_matched.empty:
                auto_matched_index = auto_matched.index
                # fill the data with auto_matched values
                operating_data.loc[auto_matched_index, combined_new_cols] = auto_matched.loc[auto_matched_index, combined_new_cols]
        operating_data = self._filter_out_any_rows_unmapped(operating_data, error_type = ErrorType(4))
        self.staged_data = operating_data
        print(self.staged_data)
        return self


    def _filter_out_any_rows_unmapped(self, data: pd.DataFrame, error_type: ErrorType) -> pd.DataFrame:
        if data.empty:
            return data
        mask = data.loc[:,~data.columns.isin(["submission_id","inv_amt","comm_amt"])].all('columns')
        data_remaining = data[mask]
        data_removed: pd.DataFrame = data.loc[~mask, ~data.columns.isin(['customer_branch_id'])]
        self._send_event_by_submission(data_removed, error_type)
        return data_remaining


    def drop_extra_columns(self) -> 'Processor':
        self.staged_data = self.staged_data.loc[:,["submission_id","customer_branch_id","inv_amt","comm_amt","user_id","report_branch_ref"]]
        return self

    def register_commission_data(self) -> 'Processor':
        if self.staged_data.empty:
            # my method for removing rows checks for existing rows with falsy values.
            # Avoid writing a blank row in the database from an empty dataframe
            return self
        else:
            self.staged_data = self.staged_data.dropna() # just in case
        post.final_data(db=self.session, data=self.staged_data)
        return self

    def set_switches(self) -> 'Processor':
        col_list = self.ppdata.data.columns.to_list()

        if "direction" in col_list:
            self.inter_warehouse_transfer = True
        
        return self

    def preprocess(self) -> 'Processor':
        report_name = get.report_name_by_id(db=self.session, report_id=self.submission.report_id)
        sub_id = self.submission_id
        file = self.submission.file
        preprocessor: AbstractPreProcessor = self.preprocessor(report_name, sub_id, file)
        optional_params = {
            "total_freight_amount": self.submission.total_freight_amount,
            "total_rebate_credits": self.submission.total_rebate_credits,
            "total_commission_amount": self.submission.total_commission_amount,
            "additional_file_1": self.submission.additional_file_1,
            "standard_commission_rate": self.standard_commission_rate,
            "split": self.split,
            "territory": self.territory,
            "specified_customer": self.specified_customer,
            "customer_proportions_by_state": self.customer_branch_proportions,
        }
        try:
            ppdata: PreProcessedData = preprocessor.preprocess(**optional_params)
        except Exception:
            raise FileProcessingError("There was an error attempting to process the file", submission_id=sub_id)

        self.ppdata = ppdata
        self.staged_data = ppdata.data.copy()
        return self

    def insert_submission_id(self) -> 'Processor':
        self.staged_data.insert(0,"submission_id",self.submission_id)
        return self
    
    def insert_recorded_at_column(self) -> 'Processor':
        self.staged_data["recorded_at"] = datetime.utcnow()
        return self

    def insert_user_id(self) -> 'Processor':
        self.staged_data["user_id"] = self.user_id
        return self

    def set_submission_status(self, status: str) -> 'Processor':
        pass


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
            matches_w_ids = post.auto_matched_strings(db=self.session, user_id=self.user_id, data=matched_rows) # returns unique rows with id nums
            matched_rows = matched_rows\
                .reset_index()\
                .merge(matches_w_ids[["report_branch_ref","id_string","report_id"]],
                                        on=["id_string", "report_id"])
            # recover index after merge
            result = matched_rows.set_index("index")
            return result
        return pd.DataFrame()
    
    def process_and_commit(self) -> int|None:
        pass



class NewReportStrategy(Processor):
    def __init__(
        self,
        session: Session,
        user: User,
        preprocessor: Type[AbstractPreProcessor],
        submission: NewSubmission,
        submission_id: int
    ):

        self.skip = False
        self.session = session
        self.user_id = user.id(self.session) if user.verified else None
        self.submission_id = submission_id
        self.submission = submission
        self.preprocessor = preprocessor
        self.report_id = submission.report_id
        self.standard_commission_rate = get.commission_rate(session, submission.manufacturer_id, user_id=self.user_id)
        self.split = get.split(session, submission.report_id, user_id=self.user_id)
        self.territory = get.territory(session, user_id=self.user_id, manf_id=self.submission.manufacturer_id)
        self.specified_customer = get.customer_id_and_name_from_report(session, user_id=self.user_id, report_id=self.report_id)
        self.customer_branch_proportions = get.customer_location_proportions_by_state(
            db=session, user_id=self.user_id,
            customer_id=self.specified_customer[0],
            territory=self.territory
        ) if self.specified_customer else None
        super().__init__(session=session, user_id=self.user_id)

    def insert_report_id(self) -> 'Processor':
        self.staged_data.insert(0,"report_id", self.report_id)
        return self


    def set_submission_status(self, status: str) -> 'Processor':
        """
        sets the status of the submission to an enum value.
        When an attempt is made to change the status, checks are made based on
        the processing scheme and status value provided to prevent erroneously assigned
        status values. Primary concern is that a submission will be labeled "COMPLETE" while
        processing errors are still found in the database
        """
        if status == 'COMPLETE':
            if self.session.execute("SELECT * FROM errors WHERE submission_id = :sub_id LIMIT 1;", {"sub_id": self.submission_id}).fetchone():
                return self
            else:
                patch.sub_status(db=self.session, submission_id=self.submission_id, status=status)
                return self
        else:
            patch.sub_status(db=self.session, submission_id=self.submission_id, status=status)
            return self


    def process_and_commit(self) -> int:
        try:
            (
            self.set_submission_status("PROCESSING")
                .preprocess()
                .insert_submission_id()
                .insert_user_id()
                .insert_report_id()
                .add_branch_id()
            )
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
            (
            self.drop_extra_columns()
                .insert_recorded_at_column()
                .register_commission_data()
                .set_submission_status("NEEDS_ATTENTION")
                .set_submission_status("COMPLETE")
            )
        return self.submission_id
    
class ErrorReintegrationStrategy(Processor):
    """when mapping errors to new mappings, no attempt is made to auto-match other values""" 
    def __init__(
        self,
        session: Session,
        user: User,
        new_mapping_id: int,
        target_err: ErrorType = None,
        error_table: pd.DataFrame = pd.DataFrame(),
    ):

        self.skip = False
        self.session = session
        self.user_id = user.id(self.session) if user.verified else None
        self.new_mapping_id = new_mapping_id
        self.target_err = target_err
        # expand row_data into dataframe columns
        self.error_table: pd.DataFrame = pd.concat(   
            [
                error_table, 
                pd.json_normalize(error_table.pop("row_data"),max_level=1)
            ], 
            axis=1)
        self.error_table = self.error_table.reset_index(drop=True)
        super().__init__(session=session, user_id=self.user_id)


    def insert_report_id(self) -> 'Processor':
        new_col = self.staged_data.loc[:,["submission_id"]].merge(self.report_id_by_submission, how="left", on="submission_id").loc[:,["report_id"]]
        self.staged_data["report_id"] = new_col
        return self

    def set_submission_status(self, status: str) -> 'Processor':
        """
        sets the status of the submission to an enum value.
        When an attempt is made to change the status, checks are made based on
        the processing scheme and status value provided to prevent erroneously assigned
        status values. Primary concern is that a submission will be labeled "COMPLETE" while
        processing errors are still found in the database
        """
        table = self.staged_data
        for sub_id in table["submission_id"].unique().tolist():
            if self.session.execute("SELECT * FROM errors WHERE submission_id = :sub_id LIMIT 1;", {"sub_id": sub_id}).fetchone():
                continue
            patch.sub_status(db=self.session, submission_id=sub_id, status=status)
        return self


    def process_and_commit(self) -> None:
        try:
            (
            self._filter_for_existing_records_with_target_error_type()
                .insert_report_id()
                .add_branch_id(do_auto_match=False)
            )
        except EmptyTableException:
            pass
        except Exception as err:
            self.set_submission_status("FAILED")
            import traceback
            # BUG background task conversion up-stack means now I'm losing the capture of this traceback
            # so while this error is raised, nothing useful is happening with it currently
            # this print is so I can see it in heroku logs
            print(f"from print: {traceback.format_exc()}")
            raise FileProcessingError(err, submission_id=None)
        else:
            (
            self.remove_error_db_entries()
                .drop_extra_columns()
                .insert_recorded_at_column()
                .register_commission_data()
                .set_submission_status("COMPLETE")
            )
        return