from dataclasses import dataclass
from pandas import DataFrame

@dataclass
class PreProcessedData:
    data: DataFrame