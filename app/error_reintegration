import pandas as pd
import json

from app import event
from db.db_services import DatabaseServices
from entities.error import ErrorType

class Reintegrator:

    def __init__(self, 
            target_err: ErrorType,
            error_table: pd.DataFrame,
            database: DatabaseServices
        ):
        self.database = database
        self.map_customer_name = database.get_mappings("map_customer_name")
        self.map_city_names = database.get_mappings("map_city_names")
        self.map_state_names = database.get_mappings("map_state_names")
        self.branches = database.get_branches()
        self.reps_to_cust_branch_ref = database.get_reps_to_cust_branch_ref()
        self.target_err = target_err
        self.error_table = pd.concat(   # expand row_data into dataframe columns
            [
                error_table, 
                pd.json_normalize(error_table.pop("row_data").apply(json.loads),max_level=1)
            ], 
            axis=1
        )
        return

    async def fill_customer_ids(self) -> 'Reintegrator':
        """converts customer column customer id #s using the map_customer_name reference table"""
        left_on_name = self.ppdata.customer_name_col
            
        merged_with_name_map = pd.merge(
                self.staged_data, self.map_customer_name,
                how="left", left_on=left_on_name, right_on="recorded_name"
            )
        # customer column is going from a name string to an id integer
        self.staged_data[left_on_name] = merged_with_name_map.loc[:,"customer_id"].fillna(0).astype(int)

        no_match_indices = self.staged_data.loc[self.staged_data[left_on_name] == 0].index.to_list()
        unmapped_no_match_table = self.ppdata.data.iloc[no_match_indices]
        event.post_event(ErrorType(1), unmapped_no_match_table, submission_id=self.submission_id)
        return self
