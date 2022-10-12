from datetime import datetime
import pandas as pd

from app import event
from db.db_services import DatabaseServices
from entities.preprocessor import AbstractPreProcessor
from entities.submission import NewSubmission
from entities.error import ErrorType

from services import api_adapter

class EmptyTableException(Exception):
    pass

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

    This Processor is almost identical to the error_reintegration.Reintegrator processor. The main
    difference is the ability to ingest an error table, check the error types against a target error type,
    and emit processing step events by-submission. Another key difference is that error reprocessing does not
    require registration of a new submission id.

    TODO: Merge error_reintegration.Reintegrator with this class to reduce duplication of processing logic 
    """
    def __init__(
            self, preprocessor: AbstractPreProcessor, 
            submission: NewSubmission, database: DatabaseServices
        ):
        self.database = database
        self.submission = submission
        self.preprocessor = preprocessor
        self.map_customer_name = database.get_mappings("map_customer_name")
        self.map_city_names = database.get_mappings("map_city_names")
        self.map_state_names = database.get_mappings("map_state_names")
        self.branches = database.get_branches()
        self.api = api_adapter.ApiAdapter()

    def reset_branch_ref(self):
        self.branches = self.database.get_branches()

    def premapped_data_by_indices(self, indices: list) -> pd.DataFrame:
        return self.ppdata.data.iloc[indices]

    def total_commissions(self) -> int:
        total_comm = self.staged_data.loc[:,"comm_amt"].sum()
        return round(total_comm)


    def total_sales(self) -> int:
        total_sales = self.staged_data.loc[:,"inv_amt"].sum()
        return round(total_sales)


    async def fill_customer_ids(self) -> 'ReportProcessor':
        """converts customer column customer id #s using the map_customer_name reference table"""
        left_on_name = self.ppdata.customer_name_col
            
        merged_with_name_map = pd.merge(
                self.staged_data, self.map_customer_name,
                how="left", left_on=left_on_name, right_on="recorded_name"
            )
        
        # customer column is going from a name string to an id integer
        self.staged_data[left_on_name] = merged_with_name_map.loc[:,"customer_id"].fillna(0).astype(int).to_list()

        no_match_indices = self.staged_data.loc[self.staged_data[left_on_name] == 0].index.to_list()
        unmapped_no_match_table = self.ppdata.data.iloc[no_match_indices]
        event.post_event(ErrorType(1), unmapped_no_match_table, submission_id=self.submission_id)
        return self


    async def fill_city_ids(self) -> 'ReportProcessor':
        """converts city column city id #s using the map_city_names reference table"""
        left_on_name = self.ppdata.city_name_col
            
        merged_w_cities_map = pd.merge(
                self.staged_data, self.map_city_names,
                how="left", left_on=left_on_name, right_on="recorded_name"
        )

        # city column is going from a name string to an id integer
        self.staged_data[left_on_name] = merged_w_cities_map.loc[:,"city_id"].fillna(0).astype(int).to_list()
        no_match_indices = self.staged_data.loc[self.staged_data[left_on_name] == 0].index.to_list()
        unmapped_no_match_table = self.ppdata.data.iloc[no_match_indices]
        event.post_event(ErrorType(2), unmapped_no_match_table, submission_id=self.submission_id)
        return self


    async def fill_state_ids(self) -> 'ReportProcessor':
        """converts column supplied in the args to id #s using the map_state_names reference table"""
        left_on_name = self.ppdata.state_name_col

        merged_w_states_map = pd.merge(
                self.staged_data, self.map_state_names,
                how="left", left_on=left_on_name, right_on="recorded_name"
            )

        # state column is going from a name string to an id integer
        self.staged_data[left_on_name] = merged_w_states_map.loc[:,"state_id"].fillna(0).astype(int).to_list()
        
        no_match_indices = self.staged_data.loc[self.staged_data[left_on_name] == 0].index.to_list()
        unmapped_no_match_table = self.ppdata.data.iloc[no_match_indices]
        event.post_event(ErrorType(3), unmapped_no_match_table, submission_id=self.submission_id)
        return self


    async def add_branch_id(self) -> 'ReportProcessor':
        """
        Adds the customer's branch id, if the assignment exists.
        Un-matched rows are added to the customer_branches table without a rep assigned

        """
        new_column: str = "customer_branch_id"
        left_on_list = self.ppdata.map_rep_customer_ref_cols

        merged_with_branches = pd.merge(
                self.staged_data, self.branches,
                how="left", left_on=left_on_list,
                right_on=["customer_id","city_id","state_id"]
        ) 

        new_col_values = merged_with_branches.loc[:,"id"].fillna(0).astype(int).to_list()
        self.staged_data.loc[:, new_column] = new_col_values

        no_match_table = self.staged_data.loc[self.staged_data[new_column]==0,["customer","city","state"]]
        if not no_match_table.empty:
            no_match_table.columns = ["customer_id","city_id","state_id"]
            no_match_records = no_match_table.drop_duplicates().to_dict(orient="records")
            self.api.create_new_customer_branch_bulk(no_match_records)
            event.post_event("Formatting", f"added {len(no_match_records)} branches to the branches table without a rep assignment",submission_id=self.submission_id)
            self.reset_branch_ref()
            await self.add_branch_id()
        return self


    async def filter_out_any_rows_unmapped(self) -> 'ReportProcessor':
        mask = self.staged_data.loc[:,~self.staged_data.columns.isin(["submission_id","inv_amt","comm_amt"])].all('columns')
        data_dropped = self.staged_data[~mask]
        self.staged_data = self.staged_data[mask]
        event.post_event("Rows Removed", data_dropped, self.submission_id)
        return self


    async def register_submission(self) -> 'ReportProcessor':
        """reigsters a new submission to the database and returns the id number of that submission"""
        self.submission_id = self.database.record_submission(self.submission)
        return self

    async def drop_extra_columns(self) -> 'ReportProcessor':
        self.staged_data = self.staged_data.loc[:,["submission_id","customer_branch_id","inv_amt","comm_amt"]]
        return self

    async def register_commission_data(self) -> 'ReportProcessor':
        if self.staged_data.empty:
            # my method for removing rows checks for existing rows with falsy values.
            # Avoid writing a blank row in the database from an empty dataframe
            return self
        else:
            self.staged_data = self.staged_data.dropna() # just in case
        self.database.record_final_data(self.staged_data)
        event.post_event("Data Recorded", self.staged_data, self.submission_id)
        return self

    async def preprocess(self) -> 'ReportProcessor':
        report_name = self.database.get_report_name_by_id(self.submission.report_id)
        sub_id = self.submission_id
        file = self.submission.file
        preprocessor: AbstractPreProcessor = self.preprocessor(report_name, sub_id, file)
        ppdata = preprocessor.preprocess()
        # send events from preprocessing using the manufacturuer (domain obj)
        for step_num, event_arg_tuple in enumerate(ppdata.events):
            if step_num == 0:
                event.post_event(*event_arg_tuple,start_step=1)
            else:
                event.post_event(*event_arg_tuple)
        self.staged_data = ppdata.data.copy()
        self.ppdata = ppdata
        return self

    async def insert_submission_id(self) -> 'ReportProcessor':
        self.staged_data.insert(0,"submission_id",self.submission_id)
        return self

    async def insert_recorded_at_column(self) -> 'ReportProcessor':
        self.staged_data["recorded_at"] = datetime.now()
        return self

    async def process_and_commit(self) -> None:
        """
        Taking preprocessed data, use reference tables from the database
        to map customer names, city names, state names, and reps
        by id numbers

        Effects: commits the submission data, final commission data, errors, and processing steps
                to the database 
        """

        await self.register_submission()
        await self.preprocess()
        await self.insert_submission_id()
        await self.fill_customer_ids()
        await self.filter_out_any_rows_unmapped()
        await self.fill_city_ids()
        await self.filter_out_any_rows_unmapped()
        await self.fill_state_ids()
        await self.filter_out_any_rows_unmapped()
        await self.add_branch_id()
        await self.filter_out_any_rows_unmapped()
        await self.drop_extra_columns()
        await self.filter_out_any_rows_unmapped()
        await self.insert_recorded_at_column()
        await self.register_commission_data()
        
        return