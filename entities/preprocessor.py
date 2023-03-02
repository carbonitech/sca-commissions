from abc import ABC, abstractmethod

from entities.commission_data import PreProcessedData
from entities.commission_file import CommissionFile

class AbstractPreProcessor(ABC):
    
    def __init__(self, report_name: str, submission_id: int, file: CommissionFile):
        self.report_name = report_name
        self.submission_id = submission_id
        self.file = file
        
    @abstractmethod
    def preprocess(self, **kwargs)-> PreProcessedData:
        pass