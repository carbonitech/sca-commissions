from dataclasses import dataclass
from pandas import DataFrame

@dataclass
class PreProcessedData:
    data: DataFrame
    customer: bool = False
    city: bool = False
    state: bool = False