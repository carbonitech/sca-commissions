from dataclasses import dataclass
from json import dumps
from enum import Enum


class ErrorType(Enum):
    CustomerNotFound = 1
    CityNotFound = 2
    StateNotFound = 3
    BranchNotFound = 4
    RepNotAssigned = 5


@dataclass
class Error:
    row_index: int
    field: str
    value_type: type
    value_content: str
    reason: ErrorType
    row_data: dict

    def keys(self):
        return list(self.__dict__.keys())
        
    def __getitem__(self,key):
        attr_value = getattr(self,key)
        if isinstance(attr_value,dict):
            return dumps(attr_value)
        elif isinstance(attr_value,type):
            return attr_value.__name__
        elif isinstance(attr_value,ErrorType):
            return attr_value.value
        else:
            return attr_value

    def add_submission_id(self, value: int) -> None:
        self.submission_id = value
