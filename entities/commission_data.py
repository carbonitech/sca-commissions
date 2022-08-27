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
    events: List[tuple]
    

@dataclass
class PostProcessedData:
    data: DataFrame
    process_steps: List[ProcessingStep]
    errors: List[Error]


@dataclass
class OneLineAdjustment:
    customer: str
    city: str
    state: str
    inv_amt: int
    comm_amt: int