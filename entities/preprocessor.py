from abc import ABC, abstractmethod

from entities.submission import NewSubmission
from entities.commission_data import PreProcessedData

class PreProcessor(ABC):

    def __init__(self, submission: NewSubmission, submission_id: int):
        self.submission = submission
        self.submission_id = submission_id
        
    @abstractmethod
    def preprocess(self)-> PreProcessedData:
        pass