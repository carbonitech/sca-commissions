from abc import ABC, abstractmethod

from entities.commission_data import PreProcessedData
from entities.commission_file import CommissionFile

class PreProcessor(ABC):

    def __init__(self, report_id: int, submission_id: int, file: CommissionFile):
        self.report_id = report_id
        self.submission_id = submission_id
        self.file = file
        
    @abstractmethod
    def preprocess(self)-> PreProcessedData:
        pass