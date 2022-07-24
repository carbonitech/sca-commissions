from typing import List
from dataclasses import dataclass
from pandas import DataFrame

from entities.processing_step import ProcessingStep

@dataclass
class PreProcessedData:
    data: DataFrame
    process_steps: List[ProcessingStep]
    map_rep_customer_ref_cols: List[str]
    customer_name_col: str
    city_name_col: str
    state_name_col: str
