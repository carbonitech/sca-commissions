from abc import ABC, abstractmethod

from entities.submission import NewSubmission

class PreProcessor(ABC):

    def __init__(self, submission: NewSubmission):
        self.submission = submission
        
    @abstractmethod
    def preprocess(self, submission):
        pass