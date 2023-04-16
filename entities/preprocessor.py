from abc import ABC, abstractmethod

from pandas import Series
from entities.commission_data import PreProcessedData
from entities.commission_file import CommissionFile

class AbstractPreProcessor(ABC):
    
    def __init__(self, report_name: str, submission_id: int, file: CommissionFile):
        self.report_name = report_name
        self.submission_id = submission_id
        self.file = file

    def upper_all_str(col: Series) -> Series:
        col_cp = col.copy()
        if col_cp.dtype == "object":
            col_cp = col_cp.str.upper().str.strip()
        return col_cp

        
    @abstractmethod
    def preprocess(self, **kwargs)-> PreProcessedData:
        pass