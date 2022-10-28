from datetime import datetime
from typing import Hashable
import pandas as pd
from sqlalchemy.orm import Session

from app import event
from entities.preprocessor import AbstractPreProcessor
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
            preprocessor: AbstractPreProcessor = None, # a class obj, not an instance
            submission: NewSubmission = None,
            target_err: ErrorType = None,
            error_table: pd.DataFrame = pd.DataFrame()
        ):

        self.skip = False
        self.reintegration = False
        self.use_store_numbers = False
        self.inter_warehouse_transfer = False
        
        if preprocessor and submission:
            if issubclass(preprocessor, AbstractPreProcessor) and isinstance(submission, NewSubmission):
                self.submission = submission
                self.preprocessor = preprocessor

        elif target_err and isinstance(error_table, pd.DataFrame):
            if isinstance(target_err, ErrorType):
                self.reintegration = True
                self.target_err = target_err
                self.error_table = pd.concat(   # expand row_data into dataframe columns
                    [
                        error_table, 
                        pd.json_normalize(error_table.pop("row_data"),max_level=1)
                    ], 
                    axis=1).reset_index(drop=True)
        else:
            self.skip = True
            return

        self.session = session
        self.api = api_adapter.ApiAdapter()
        self.map_customer_names = self.api.get_mappings(session, "map_customer_names")
        self.map_city_names = self.api.get_mappings(session, "map_city_names")
        self.map_state_names = self.api.get_mappings(session, "map_state_names")
        self.branches = self.api.get_branches(session)

    def reset_branch_ref(self):
        self.branches = self.api.get_branches(db=self.session)

    def total_commissions(self) -> int:
        total_comm = self.staged_data.loc[:,"comm_amt"].sum()
        return round(total_comm)

    def total_sales(self) -> int:
        total_sales = self.staged_data.loc[:,"inv_amt"].sum()
        return round(total_sales)

    def _send_event_by_submission(self, indecies: list, event_: Hashable, msg: str=None) -> None:
        if self.reintegration:
            table = self.error_table.iloc[indecies,:]
        else:
            table = self.ppdata.data.iloc[indecies,:]
            table.insert(0,"submission_id",self.submission_id)

        for sub_id in table["submission_id"].unique().tolist():
            mask = table["submission_id"] == sub_id
            sub_id_table = table.loc[mask,:]
            if "row_index" in table.columns.tolist():
                sub_id_table = sub_id_table.set_index("row_index")
            sub_id_table = sub_id_table.loc[:,~sub_id_table.columns.isin(['submission_id','id','reason'])]
            if event_ == "Formatting" and msg:
                event.post_event(
                    event_,
                    msg,
                    submission_id=sub_id,
                    start_step=self.api.last_step_num(db=self.session, submission_id=sub_id)+1,
                    session=self.session
                )
            else:
                event.post_event(
                    event_,
                    sub_id_table,
                    submission_id=sub_id,
                    start_step=self.api.last_step_num(db=self.session, submission_id=sub_id)+1,
                    session=self.session
                )


    def _filter_for_existing_records_with_target_error_type(self) -> 'ReportProcessor':
        mask = self.error_table["reason"] == self.target_err.value
        table_target_errors = self.error_table.loc[mask]
        if table_target_errors.empty:
            raise EmptyTableException
        self.error_table = table_target_errors.reset_index(drop=True) # fixes for for id merging strategy
        self.staged_data = self.error_table.copy()
        for sub_id in self.staged_data["submission_id"].unique().tolist():
            rows_affected = len(self.error_table[self.error_table["submission_id"]==sub_id])    
            msg = f"reprocessing of errors initiated by reassessment of {self.target_err.name} errors. {rows_affected} rows affected"
            event.post_event(
                "Reprocessing",
                data_ = msg,
                submission_id = sub_id,
                start_step=self.api.last_step_num(db=self.session, submission_id=sub_id)+1,
                session=self.session
            )
        return self

    def remove_error_db_entries(self) -> 'ReportProcessor':
        self.api.delete_errors(db=self.session, record_ids=self.staged_data["id"].to_list())
        return self

    def fill_customer_ids(self) -> 'ReportProcessor':
        """converts customer column customer id #s using the map_customer_name reference table"""
        left_on_name = "customer"
            
        merged_with_name_map = pd.merge(
                self.staged_data, self.map_customer_names,
                how="left", left_on=left_on_name, right_on="recorded_name"
            )
        
        # customer column is going from a name string to an id integer
        self.staged_data[left_on_name] = merged_with_name_map.loc[:,"customer_id"].fillna(0).astype(int).to_list()

        no_match_indices = self.staged_data.loc[self.staged_data[left_on_name] == 0].index.to_list()
        self._send_event_by_submission(no_match_indices,ErrorType(1))
        return self


    def fill_city_ids(self) -> 'ReportProcessor':
        """converts city column city id #s using the map_city_names reference table"""
        left_on_name = "city"
            
        merged_w_cities_map = pd.merge(
                self.staged_data, self.map_city_names,
                how="left", left_on=left_on_name, right_on="recorded_name"
        )

        # city column is going from a name string to an id integer
        self.staged_data[left_on_name] = merged_w_cities_map.loc[:,"city_id"].fillna(0).astype(int).to_list()
        no_match_indices = self.staged_data.loc[self.staged_data[left_on_name] == 0].index.to_list()
        self._send_event_by_submission(no_match_indices,ErrorType(2))
        return self


    def fill_state_ids(self) -> 'ReportProcessor':
        """converts column supplied in the args to id #s using the map_state_names reference table"""
        left_on_name = "state"

        merged_w_states_map = pd.merge(
                self.staged_data, self.map_state_names,
                how="left", left_on=left_on_name, right_on="recorded_name"
            )

        # state column is going from a name string to an id integer
        self.staged_data[left_on_name] = merged_w_states_map.loc[:,"state_id"].fillna(0).astype(int).to_list()
        
        no_match_indices = self.staged_data.loc[self.staged_data[left_on_name] == 0].index.to_list()
        self._send_event_by_submission(no_match_indices,ErrorType(3))
        return self


    def add_branch_id(self) -> 'ReportProcessor':
        """
        Adds the customer's branch id, if the assignment exists.
        Un-matched rows are added to the customer_branches table without a rep assigned

        """
        new_column: str = "customer_branch_id"
        left_on_list = ["customer","city","state"]

        merged_with_branches = pd.merge(
                self.staged_data, self.branches,
                how="left", left_on=left_on_list,
                right_on=["customer_id","city_id","state_id"],
                suffixes=(None,"_ref_table")
        ) 
        try:
            new_col_values = merged_with_branches.loc[:,"id_ref_table"].fillna(0).astype(int).to_list()
        except KeyError:
            new_col_values = merged_with_branches.loc[:,"id"].fillna(0).astype(int).to_list()

        self.staged_data.loc[:, new_column] = new_col_values

        no_match_table = self.staged_data.loc[self.staged_data[new_column]==0,left_on_list]
        if not no_match_table.empty:
            no_match_table.columns = ["customer_id","city_id","state_id"]
            no_match_records = no_match_table.drop_duplicates()
            self.api.create_new_customer_branch_bulk(self.session,no_match_records.to_dict(orient="records"))
            self._send_event_by_submission(
                no_match_records.index.to_list(),
                "Formatting",f"added {len(no_match_records)} branches to the branches table without a rep assignment"
            )
            self.reset_branch_ref()
            self.add_branch_id()
        return self

    def add_branch_id_by_store_number(self) -> 'ReportProcessor':
        """
        if branch numbers are in the preprocessed data in place of customer info, 
            this method replaces matching by customer name, 
            city name, and state name seperately
            
        Unmatched data is put into the errors table
        """
        new_column: str = "customer_branch_id"
        left_on_list = ["store_number", "customer"]
        data_copy = self.staged_data.copy()
        try:
            # for int-like store numbers -> remove the float-like representation from the string
            data_copy["store_number"] = data_copy["store_number"].astype(float).astype(int).astype(str)
        except:
            # store number is likely alphanumeric and will match properly
            pass

        merged_with_branches = pd.merge(
                data_copy, self.branches,
                how="left", left_on=left_on_list,
                right_on=["store_number", "customer_id"],
                suffixes=(None,"_ref_table")
        ) 
        try:
            new_col_values = merged_with_branches.loc[:,"id_ref_table"].fillna(0).astype(int).to_list()
        except KeyError:
            new_col_values = merged_with_branches.loc[:,"id"].fillna(0).astype(int).to_list()

        self.staged_data.loc[:, new_column] = new_col_values
        no_match_table = self.staged_data.loc[self.staged_data[new_column]==0,left_on_list]
        self._send_event_by_submission(no_match_table.index.to_list(),ErrorType(4))
        return self

    def add_branch_by_transfer_direction(self):
        """
        if receiver in territory (+)
        if sender in territory (-)
        if receiver not in territory (drop)
        if sender not in territory (drop)
        if sender or receiver not found (add to branches or errors?)
        """

    def filter_out_any_rows_unmapped(self) -> 'ReportProcessor':
        mask = self.staged_data.loc[:,~self.staged_data.columns.isin(["submission_id","inv_amt","comm_amt"])].all('columns')
        data_dropped = self.staged_data[~mask].index.to_list()
        self.staged_data = self.staged_data[mask]
        self._send_event_by_submission(data_dropped, "Rows Removed")
        return self


    def register_submission(self) -> 'ReportProcessor':
        """reigsters a new submission to the database and returns the id number of that submission"""
        self.submission_id = self.api.record_submission(db=self.session, submission=self.submission)
        return self

    def drop_extra_columns(self) -> 'ReportProcessor':
        self.staged_data = self.staged_data.loc[:,["submission_id","customer_branch_id","inv_amt","comm_amt"]]
        return self

    def register_commission_data(self) -> 'ReportProcessor':
        if self.staged_data.empty:
            # my method for removing rows checks for existing rows with falsy values.
            # Avoid writing a blank row in the database from an empty dataframe
            return self
        else:
            self.staged_data = self.staged_data.dropna() # just in case
        self.api.record_final_data(db=self.session, data=self.staged_data)
        self._send_event_by_submission(self.staged_data.index.to_list(), "Data Recorded")
        return self

    def preprocess(self) -> 'ReportProcessor':
        report_name = self.api.get_report_name_by_id(db=self.session, report_id=self.submission.report_id)
        sub_id = self.submission_id
        file = self.submission.file
        preprocessor: AbstractPreProcessor = self.preprocessor(report_name, sub_id, file)

        try:
            ppdata = preprocessor.preprocess()
        except Exception:
            raise FileProcessingError("There was an error while we attempted to process the file", submission_id=sub_id)
        
        match ppdata.data.columns.to_list():
            case ["store_number", *other_cols]:
                self.use_store_numbers = True
            case [*other_cols, "direction", "warehouse"]:
                self.inter_warehouse_transfer = True

        for step_num, event_arg_tuple in enumerate(ppdata.events):
            if step_num == 0:
                event.post_event(*event_arg_tuple, start_step=1, session=self.session)
            else:
                event.post_event(*event_arg_tuple, session=self.session)
        self.staged_data = ppdata.data.copy()
        self.ppdata = ppdata
        return self

    def insert_submission_id(self) -> 'ReportProcessor':
        self.staged_data.insert(0,"submission_id",self.submission_id)
        return self

    def insert_recorded_at_column(self) -> 'ReportProcessor':
        self.staged_data["recorded_at"] = datetime.now()
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

        if not self.reintegration:
            self.register_submission()      \
            .preprocess()                   \
            .insert_submission_id()
        else:
            try:             
                self._filter_for_existing_records_with_target_error_type() \
                    .remove_error_db_entries()
            except EmptyTableException:
                return
        
        self.fill_customer_ids()        \
            .filter_out_any_rows_unmapped()
            
        if self.use_store_numbers:
            self.add_branch_id_by_store_number()
        elif self.inter_warehouse_transfer:
            self.add_branch_by_transfer_direction()
        else:
            self.fill_city_ids()            \
            .filter_out_any_rows_unmapped() \
            .fill_state_ids()               \
            .filter_out_any_rows_unmapped() \
            .add_branch_id()
            
        self.filter_out_any_rows_unmapped() \
            .drop_extra_columns()           \
            .insert_recorded_at_column()    \
            .register_commission_data()     

        return self.submission_id if not self.reintegration else None