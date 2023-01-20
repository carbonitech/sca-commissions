
from datetime import datetime
from dataclasses import dataclass, field
from entities.commission_file import CommissionFile

@dataclass
class NewSubmission:
    file: CommissionFile
    reporting_month: int
    reporting_year: int
    report_id: int
    manufacturer_id: int
    user_id: int
    submission_date: datetime = field(default_factory=datetime.today, init=False)
    total_commission_amount: float|None
    total_freight_amount: float|None
    additional_file_1: bytes|None
    #following is actually an enum in postgres
    status: str = "QUEUED"

    def keys(self):
        unpackable_attrs = list(self.__dict__.keys())
        unpackable_attrs.remove("file")
        unpackable_attrs.remove("manufacturer_id")
        unpackable_attrs.remove("total_freight_amount")
        unpackable_attrs.remove("additional_file_1")
        return unpackable_attrs
        
    def __getitem__(self,key):
        return getattr(self,key)