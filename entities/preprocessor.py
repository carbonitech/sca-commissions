from abc import ABC, abstractmethod

class PreProcessor(ABC):

    @abstractmethod
    def preprocess(self, submission):
        pass