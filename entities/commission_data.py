from typing import List
from dataclasses import dataclass
from pandas import DataFrame

from entities.processing_step import ProcessingStep
from entities.error import Error


@dataclass
class PreProcessedData:
    data: DataFrame
    map_rep_customer_ref_cols: List[str]
    customer_name_col: str
    city_name_col: str
    state_name_col: str

    def total_commission(self) -> int:
        return self.data.loc[:,"comm_amt"].sum()

    def total_sales(self) -> int:
        return self.data.loc[:,"inv_amt"].sum()


@dataclass
class PostProcessedData:
    data: DataFrame
    process_steps: List[ProcessingStep]
    errors: List[Error]

    def total_commissions(self):
        return round(self.data.loc[:,"comm_amt"].sum())/100

    def total_sales(self):
        return round(self.data.loc[:,"inv_amt"].sum())/100