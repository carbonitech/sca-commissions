from typing import List
from dataclasses import dataclass
from pandas import DataFrame

from entities.processing_step import ProcessingStep
from entities.error import Error


@dataclass
class PreProcessedData:
    data: DataFrame
    

@dataclass
class PostProcessedData:
    data: DataFrame
    process_steps: List[ProcessingStep]
    errors: List[Error]
