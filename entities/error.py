from dataclasses import dataclass

@dataclass
class Error:
    submission_id: int
    row_index: int
    field: str
    value_type: type
    value_content: str
    reason: str
    row_data: dict

    def keys(self):
        return list(self.__dict__.keys())
        
    def __getitem__(self,key):
        return getattr(self,key)