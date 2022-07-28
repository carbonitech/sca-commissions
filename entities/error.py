from dataclasses import dataclass
from json import dumps

@dataclass
class Error:
    row_index: int
    field: str
    value_type: type
    value_content: str
    reason: str
    row_data: dict

    def __post_init__(self) -> None:
        self.value_type: str = self.value_type.__name__
        self.row_data: str = dumps(self.row_data)

    def keys(self):
        return list(self.__dict__.keys())
        
    def __getitem__(self,key):
        return getattr(self,key)

    def add_submission_id(self, value: int) -> None:
        self.submission_id = value