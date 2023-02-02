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


    TODO: refactor branch id handling. There's a lot of retrying/redirecting, even recursion. This area of the code is confusing.
    """
    def __init__(
            self,
            session: Session,
            user: api_adapter.User,
            preprocessor: AbstractPreProcessor = None, # a class obj, not an instance
            submission: NewSubmission = None,
            target_err: ErrorType = None,
            error_table: pd.DataFrame = pd.DataFrame(),
            submission_id: int|None = None
        ):

        self.skip = False
        self.reintegration = False
        self.use_store_numbers = False
        self.inter_warehouse_transfer = False
        self.session = session
        self.api = api_adapter.ApiAdapter()
        self.user_id = user.id(self.session) if user.verified else None
        self.submission_id = submission_id
        self.lookup_state = False
        self.use_default_branch_city = False
        self.split_by_defaults = False

        if not submission_id:
            del self.submission_id
        
        if preprocessor and submission:
            if issubclass(preprocessor, AbstractPreProcessor) and isinstance(submission, NewSubmission):
                self.submission = submission
                self.preprocessor = preprocessor
                self.standard_commission_rate = self.api.get_commission_rate(session, submission.manufacturer_id, user_id=self.user_id)
                self.split = self.api.get_split(session, submission.report_id, user_id=self.user_id)
                self.default_branch = self.api.get_default_branch(session, submission.report_id, user_id=self.user_id)


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

        self.map_customer_names = self.api.get_mappings(session, "map_customer_names", user_id=self.user_id)
        self.map_city_names = self.api.get_mappings(session, "map_city_names", user_id=self.user_id)
        self.map_state_names = self.api.get_mappings(session, "map_state_names", user_id=self.user_id)
        self.branches = self.api.get_branches(session, user_id=self.user_id)

    def reset_branch_ref(self):
        self.branches = self.api.get_branches(db=self.session, user_id=self.user_id)

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
            sub_id_table = sub_id_table.loc[:,~sub_id_table.columns.isin(['submission_id','id','reason','user_id'])]
            if event_ == "Formatting" and msg:
                event.post_event(
                    event_,
                    msg,
                    submission_id=sub_id,
                    user_id=self.user_id,
                    start_step=self.api.last_step_num(db=self.session, submission_id=sub_id)+1,
                    session=self.session
                )
            else:
                event.post_event(
                    event_,
                    sub_id_table,
                    submission_id=sub_id,
                    user_id=self.user_id,
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
                user_id=self.user_id,
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


    def fill_city_ids(self, data=pd.DataFrame(), pipe=True, use_lookup: bool=False) -> Union['ReportProcessor',pd.DataFrame]:
        """converts city column city id #s using the map_city_names reference table"""
        left_on_name = "city"
        if not pipe:
            operating_data = data
        else:
            operating_data = self.staged_data

        if operating_data.empty:
            return self if pipe else operating_data

        if use_lookup: # default branch city by state
            all_defaults = pd.read_sql("""
                SELECT cb.id, customer_id, loc.city, loc.state
                from customer_branches as cb
                join locations as loc
                on loc.id = cb.location_id
                where default_branch
                and cb.user_id = %(user_id)s;
                """, self.session.get_bind(),params={"user_id": self.user_id})
            merge_with_defaults = pd.merge(operating_data, all_defaults,
            how="left", left_on="customer", right_on="customer_id")
            operating_data[left_on_name] = merge_with_defaults.loc[:,"city"].fillna("NOT FOUND").astype(str).to_list()


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


    def fill_state_ids(self, data=pd.DataFrame(), pipe=True, use_lookup: bool=False) -> Union['ReportProcessor',pd.DataFrame]:
        """converts column supplied in the args to id #s using the map_state_names reference table"""
        left_on_name = "state"
        if not pipe:
            operating_data = data
        else:
            operating_data = self.staged_data

        if operating_data.empty:
            return self if pipe else operating_data

        if use_lookup:
            customer_locations = pd.read_sql("""
                SELECT cb.customer_id, ci.id as city, l.state
                FROM customer_branches as cb
                JOIN locations as l on l.id = cb.location_id
                JOIN cities as ci on ci.name = l.city
                WHERE cb.user_id = %(user_id)s;
            """, con=self.session.get_bind(), params={"user_id": self.user_id})
            merge_with_defaults = pd.merge(operating_data, customer_locations,
            how="left", left_on=["customer", "city"], right_on=["customer_id", "city"])
            operating_data[left_on_name] = merge_with_defaults.loc[:,"state"].fillna("NOT FOUND").astype(str).to_list()

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
            return self if pipe else operating_data

        # some customers have two stores in the same geo differentiated only by store numbers.
        # if a seperate canonical city name is used, dedup isn't needed, but it doesn't hurt
        deduped_branches = self.branches.drop_duplicates(subset=["customer_id", "city_id", "state_id"])

        merged_with_branches = pd.merge(
                operating_data, deduped_branches,
                how="left", left_on=left_on_list,
                right_on=["customer_id","city_id","state_id"],
                suffixes=(None,"_ref_table")
        ) 
        try:
            new_col_values = merged_with_branches.loc[:,"id_ref_table"].fillna(0).astype(int).to_list()
        except KeyError:
            new_col_values = merged_with_branches.loc[:,"id"].fillna(0).astype(int).to_list()

        operating_data.loc[:, new_column] = new_col_values

        if "store_number" in operating_data.columns.to_list():
            no_match_table = operating_data.loc[operating_data[new_column]==0,left_on_list+["store_number"]]
            no_match_table.columns = ["customer_id", "city_id", "state_id", "store_number"]
        else:
            no_match_table = operating_data.loc[operating_data[new_column]==0,left_on_list]
            no_match_table.columns = ["customer_id", "city_id", "state_id"]

        if not no_match_table.empty:
            no_match_records = no_match_table.drop_duplicates()
            self.api.create_new_customer_branch_bulk(self.session,self.user_id,no_match_records.to_dict(orient="records"))
            self._send_event_by_submission(
                no_match_records.index.to_list(),
                "Formatting",f"added {len(no_match_records)} branches to the branches table" #TODO if multiple submissions are part of this step in a reintegration, this same message shows up in processing steps for both, which is inaccurate for both and makes it appear as though a multiple of the actual number was added
            )
            self.reset_branch_ref()
            result = self.add_branch_id(data=operating_data, pipe=pipe)
            return result
        else:
            if pipe:
                operating_data = self._filter_out_any_rows_unmapped(operating_data)
                self.staged_data = operating_data
                return self
            else:
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

        self.staged_data.loc[:, new_column] = new_col_values

        # if city and state are present, try going with mapping those and concatentating what you can get
        table_columns = set(data_copy.columns.to_list())
        core_columns = ["city", "state"]
        if set(core_columns).issubset(table_columns):
            no_match_table = self.staged_data.loc[self.staged_data[new_column]==0, ["submission_id"] + left_on_list + core_columns]
            msg = f"Using store numbers, {str(len(no_match_table))} rows failed to match. City and State columns are present. Attempting to map customers by mapping cities and states instead."
            self._send_event_by_submission(no_match_table.index.to_list(),"Formatting",msg=msg)
            if not no_match_table.empty:
                retry_result = self.fill_city_ids(no_match_table, pipe=False)
                retry_result = self.fill_state_ids(retry_result, pipe=False)
                retry_result = self.add_branch_id(retry_result, pipe=False)
                if not retry_result.empty:
                    #only update values with matching index to the data subset re-processed
                    self.staged_data.loc[
                        self.staged_data.index.isin(retry_result.index),
                        new_column] = retry_result[new_column]
                    self.staged_data[new_column] = self.staged_data[new_column].fillna(0).astype(int)
                self.staged_data = self._filter_out_any_rows_unmapped(self.staged_data, suppress_event=True) # will duplicate all drops made implicitly above if not suppressed
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


    def _filter_out_any_rows_unmapped(self, data: pd.DataFrame, suppress_event: bool=False) -> pd.DataFrame:
        if data.empty:
            return data
        mask = data.loc[:,~data.columns.isin(["submission_id","inv_amt","comm_amt","row_index"])].all('columns')
        data_dropped = data[~mask].index.to_list()
        data = data[mask]
        if not suppress_event:
            self._send_event_by_submission(data_dropped, "Rows Removed")
        return data


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
        self._send_event_by_submission(self.staged_data.index.to_list(), "Data Recorded")
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
            "default_branch": self.default_branch
        }
        try:
            ppdata = preprocessor.preprocess(**optional_params)
        except Exception:
            raise FileProcessingError("There was an error attempting to process the file", submission_id=sub_id)

        col_list = ppdata.data.columns.to_list()
        
        match col_list:
            # additional columns
            case ["store_number", *other_cols]:
                self.use_store_numbers = True
            case [*other_cols, "direction", "store_number"]:
                self.use_store_numbers = True
                self.inter_warehouse_transfer = True
            # missing columns
            case ["customer", "city", "inv_amt", "comm_amt"]:
                self.lookup_state = True
            case ["customer", "state", "inv_amt", "comm_amt"]:
                self.use_default_branch_city = True
            case ["customer", "inv_amt", "comm_amt"]:
                self.split_by_defaults = True

        for step_num, event_arg_tuple in enumerate(ppdata.events):
            if step_num == 0:
                event.post_event(*event_arg_tuple, user_id=self.user_id, start_step=1, session=self.session)
            else:
                event.post_event(*event_arg_tuple, user_id=self.user_id, session=self.session)


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

    def split_customer_values_by_default_branches(self) -> 'ReportProcessor':
        all_defaults = self.session.execute("SELECT id, customer_id from customer_branches where default_branch;")
        

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
                (
                    self
                    .set_submission_status("PROCESSING")
                    .preprocess()
                    .insert_submission_id()
                    .insert_user_id()
                )
            else:
                (
                    self
                    ._filter_for_existing_records_with_target_error_type()
                    .remove_error_db_entries()
                )

            self.fill_customer_ids()
            if self.inter_warehouse_transfer:
                self.assign_value_by_transfer_direction()

            if self.use_store_numbers:
                self.add_branch_id_by_store_number()
            elif self.split_by_defaults:
                self.split_customer_values_by_default_branches()
            else:
                (
                    self
                    .fill_city_ids(use_lookup=self.use_default_branch_city)
                    .fill_state_ids(use_lookup=self.lookup_state)
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
            raise FileProcessingError(err, submission_id=self.submission_id)
        else:
            self\
            .drop_extra_columns()\
            .insert_recorded_at_column()\
            .register_commission_data()\
            .set_submission_status("COMPLETE")
        return self.submission_id if not self.reintegration else None