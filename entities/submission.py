
from datetime import datetime
from dataclasses import dataclass, field

@dataclass
class NewSubmission:
    file: bytes
    report_month: int
    report_year: int
    report_id: int
    manufacturer_id: int
    sheet_name: str
    submission_date: datetime = field(default_factory=datetime.today, init=False)

    def keys(self):
        return list(self.__dict__.keys())
        
    def __getitem__(self,key):
        return getattr(self,key)
    

@dataclass
class RegisteredSubmission:
    id: int
    report_month: int
    report_year: int
    report_id: int
    submission_date: datetime

    def keys(self):
        return list(self.__dict__.keys())
        
    def __getitem__(self,key):
        return getattr(self,key)
