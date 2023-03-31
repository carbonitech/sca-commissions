from typing import List
from dataclasses import dataclass
from pandas import DataFrame

from entities.error import Error


@dataclass
class PreProcessedData:
    data: DataFrame
    

@dataclass
class PostProcessedData:
    data: DataFrame
    errors: List[Error]
