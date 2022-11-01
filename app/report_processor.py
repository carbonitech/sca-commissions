from datetime import datetime
from typing import Hashable, Union
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

    def total_commissions(self, dataset: str=None) -> int:
        total_comm = self.staged_data.loc[:,"comm_amt"].sum()
        return round(total_comm)

    def total_sales(self, dataset: str=None) -> int:
        if dataset == "ppdata":
            data = self.ppdata.data
        else:
            data = self.staged_data

        total_sales = data.loc[:,"inv_amt"].sum()
        return round(total_sales)

    def _send_event_by_submission(self, indecies: list, event_: Hashable, msg: str=None) -> None:
        if self.reintegration:
            table = self.error_table.loc[indecies,:]
        else:
            table = self.ppdata.data.loc[indecies,:]
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

    def fill_customer_ids(self, data=pd.DataFrame(), pipe=True) -> Union['ReportProcessor',pd.DataFrame]:
        """converts customer column customer id #s using the map_customer_names reference table"""
        left_on_name = "customer"
        if not pipe:
            operating_data = data
        else:
            operating_data = self.staged_data

        if operating_data.empty:
            return self if pipe else operating_data

        merged_with_name_map = pd.merge(
                operating_data, self.map_customer_names,
                how="left", left_on=left_on_name, right_on="recorded_name"
            )
        
        # customer column is going from a name string to an id integer
        operating_data[left_on_name] = merged_with_name_map.loc[:,"customer_id"].fillna(0).astype(int).to_list()

        no_match_indices = operating_data.loc[operating_data[left_on_name] == 0].index.to_list()
        self._send_event_by_submission(no_match_indices,ErrorType(1))
        operating_data = self._filter_out_any_rows_unmapped(operating_data)
        if operating_data.empty:
            raise EmptyTableException
        if pipe:
            self.staged_data = operating_data
            return self
        return operating_data


    def fill_city_ids(self, data=pd.DataFrame(), pipe=True) -> Union['ReportProcessor',pd.DataFrame]:
        """converts city column city id #s using the map_city_names reference table"""
        left_on_name = "city"
        if not pipe:
            operating_data = data
        else:
            operating_data = self.staged_data

        if operating_data.empty:
            return self if pipe else operating_data

        merged_w_cities_map = pd.merge(
                operating_data, self.map_city_names,
                how="left", left_on=left_on_name, right_on="recorded_name"
        )

        # city column is going from a name string to an id integer
        operating_data[left_on_name] = merged_w_cities_map.loc[:,"city_id"].fillna(0).astype(int).to_list()
        no_match_indices = operating_data.loc[operating_data[left_on_name] == 0].index.to_list()
        self._send_event_by_submission(no_match_indices,ErrorType(2))
        operating_data = self._filter_out_any_rows_unmapped(operating_data)
        if pipe:
            self.staged_data = operating_data
            return self
        return operating_data


    def fill_state_ids(self, data=pd.DataFrame(), pipe=True) -> Union['ReportProcessor',pd.DataFrame]:
        """converts column supplied in the args to id #s using the map_state_names reference table"""
        left_on_name = "state"
        if not pipe:
            operating_data = data
        else:
            operating_data = self.staged_data

        if operating_data.empty:
            return self if pipe else operating_data

        merged_w_states_map = pd.merge(
                operating_data, self.map_state_names,
                how="left", left_on=left_on_name, right_on="recorded_name"
            )

        # state column is going from a name string to an id integer
        operating_data[left_on_name] = merged_w_states_map.loc[:,"state_id"].fillna(0).astype(int).to_list()
        
        no_match_indices = operating_data.loc[operating_data[left_on_name] == 0].index.to_list()
        self._send_event_by_submission(no_match_indices,ErrorType(3))
        operating_data = self._filter_out_any_rows_unmapped(operating_data)
        if pipe:
            self.staged_data = operating_data
            return self
        return operating_data


    def add_branch_id(self, data=pd.DataFrame(), pipe=True) -> Union['ReportProcessor',pd.DataFrame]:
        """
        Adds the customer's branch id, if the assignment exists.
        Un-matched rows are added to the customer_branches table

        """
        new_column: str = "customer_branch_id"
        left_on_list = ["customer","city","state"]

        if not pipe:
            operating_data = data
        else:
            operating_data = self.staged_data

        if operating_data.empty:
            operating_data["customer_branch_id"] = None
            operating_data["in_territory"] = None
            return self if pipe else operating_data

        merged_with_branches = pd.merge(
                operating_data, self.branches,
                how="left", left_on=left_on_list,
                right_on=["customer_id","city_id","state_id"],
                suffixes=(None,"_ref_table")
        ) 
        try:
            new_col_values = merged_with_branches.loc[:,"id_ref_table"].fillna(0).astype(int).to_list()
        except KeyError:
            new_col_values = merged_with_branches.loc[:,"id"].fillna(0).astype(int).to_list()

        in_territory = merged_with_branches.loc[:,"in_territory"].fillna(0).to_list()

        operating_data.loc[:, new_column] = new_col_values
        operating_data.loc[:, "in_territory"] = in_territory

        if "store_number" in operating_data.columns.to_list():
            no_match_table = operating_data.loc[operating_data[new_column]==0,left_on_list+["store_number"]]
            no_match_table.columns = ["customer_id", "city_id", "state_id", "store_number"]
        else:
            no_match_table = operating_data.loc[operating_data[new_column]==0,left_on_list]
            no_match_table.columns = ["customer_id", "city_id", "state_id"]

        if not no_match_table.empty:
            no_match_records = no_match_table.drop_duplicates()
            self.api.create_new_customer_branch_bulk(self.session,no_match_records.to_dict(orient="records"))
            self._send_event_by_submission(
                no_match_records.index.to_list(),
                "Formatting",f"added {len(no_match_records)} branches to the branches table"
            )
            self.reset_branch_ref()
            self.add_branch_id()
        operating_data = self._filter_out_any_rows_unmapped(operating_data)
        if pipe:
            self.staged_data = operating_data
            return self
        return operating_data


    def add_branch_id_by_store_number(self) -> 'ReportProcessor':
        """
        if branch numbers are in the preprocessed data in place of customer info, 
            this method replaces matching by customer name, 
            city name, and state name seperately.
        However, if there's unmatched data and there are customer, city, and state columns in the data
            attempt to use individual name mappings
            
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

        in_territory = merged_with_branches.loc[:,"in_territory"].fillna(0).to_list()

        self.staged_data.loc[:, new_column] = new_col_values
        self.staged_data.loc[:, "in_territory"] = in_territory
        
        # if city and state are present, try going with mapping those and concatentating what you can get
        table_columns = set(data_copy.columns.to_list())
        core_columns = ["city", "state"]
        if set(core_columns).issubset(table_columns):
            no_match_table = self.staged_data.loc[self.staged_data[new_column]==0, ["submission_id"] + left_on_list + core_columns]
            if not no_match_table.empty:
                retry_result = self.fill_city_ids(no_match_table, pipe=False)
                retry_result = self.fill_state_ids(retry_result, pipe=False)
                self.staged_data = pd.concat([self.staged_data, self.add_branch_id(retry_result, pipe=False)])
            else:
                self.staged_data = self._filter_out_any_rows_unmapped(self.staged_data)
        else:   # otherwise just send these match failures to errors table
            no_match_table = self.staged_data.loc[self.staged_data[new_column]==0, left_on_list]
            self._send_event_by_submission(no_match_table.index.to_list(),ErrorType(4))
            self.staged_data = self._filter_out_any_rows_unmapped(self.staged_data)
        return self

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
        self._send_event_by_submission(self.staged_data.index.to_list(),"Formatting", "assigned negative dollars to sender warehouses")
        return self


    def remove_out_of_territory_branches(self) -> 'ReportProcessor':
        self.staged_data = self.staged_data[self.staged_data["in_territory"]]
        return self
        

    def _filter_out_any_rows_unmapped(self, data: pd.DataFrame) -> pd.DataFrame:
        if data.empty:
            return data
        mask = data.loc[:,~data.columns.isin(["submission_id","inv_amt","comm_amt"])].all('columns')
        data_dropped = data[~mask].index.to_list()
        data = data[mask]
        self._send_event_by_submission(data_dropped, "Rows Removed")
        return data


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
            raise FileProcessingError("There was an error attempting to process the file", submission_id=sub_id)
        
        match ppdata.data.columns.to_list():
            case ["store_number", *other_cols]:
                self.use_store_numbers = True
            case [*other_cols, "direction", "store_number"]:
                self.use_store_numbers = True
                self.inter_warehouse_transfer = True

        for step_num, event_arg_tuple in enumerate(ppdata.events):
            if step_num == 0:
                event.post_event(*event_arg_tuple, start_step=1, session=self.session)
            else:
                event.post_event(*event_arg_tuple, session=self.session)


        self.ppdata = ppdata

        if self.ppdata.data.loc[:,"comm_amt"].eq(0).all():
            if self.submission.total_commission_amount:
                total_sales = self.total_sales("ppdata")/100 # back from cents to dollars
                comm_rate = self.submission.total_commission_amount/total_sales
                self.ppdata.data.loc[:,"comm_amt"] = self.ppdata.data["inv_amt"]*comm_rate
                event.post_event(
                    "Formatting",
                    f"filled comm_amt column by applying the commission rate {comm_rate*100:.2f}% to inv_amt, "\
                    f"derived from total commissions divided by total sales: "\
                    f"${self.submission.total_commission_amount:,.2f} / ${total_sales:,.2f} = {comm_rate*100:.2f}%",
                    self.submission_id,
                    session=self.session
                )
                
        self.staged_data = ppdata.data.copy()
        return self

    def insert_submission_id(self) -> 'ReportProcessor':
        self.staged_data.insert(0,"submission_id",self.submission_id)
        return self

    def insert_recorded_at_column(self) -> 'ReportProcessor':
        self.staged_data["recorded_at"] = datetime.utcnow()
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
        self.fill_customer_ids()
        if self.inter_warehouse_transfer:
            self.assign_value_by_transfer_direction()
        if self.use_store_numbers:
            self.add_branch_id_by_store_number()
        else:
            self.fill_city_ids()            \
            .fill_state_ids()               \
            .add_branch_id()
            
        self.remove_out_of_territory_branches() \
        .drop_extra_columns()               \
        .insert_recorded_at_column()        \
        .register_commission_data()     

        return self.submission_id if not self.reintegration else None