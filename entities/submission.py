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
    manufacturer_name: str
    user_id: int
    user_name: str
    submission_date: datetime = field(default_factory=datetime.today, init=False)
    total_commission_amount: float|None
    total_freight_amount: float|None
    additional_file_1: bytes|None
    total_rebate_credits: float|None
    status: str = "QUEUED" # enum in postgres

    def __post_init__(self):
        self.s3_key = self.s3_keygen()

    def keys(self):
        unpackable_attrs = list(self.__dict__.keys())
        unpackable_attrs.remove("file")
        unpackable_attrs.remove("manufacturer_id")
        unpackable_attrs.remove("total_freight_amount")
        unpackable_attrs.remove("additional_file_1")
        unpackable_attrs.remove("total_rebate_credits")
        unpackable_attrs.remove("user_name")
        unpackable_attrs.remove("manufacturer_name")
        return unpackable_attrs
        
    def __getitem__(self,key):
        return getattr(self,key)
    
    def s3_keygen(self) -> str:
        return f'{self.user_name}/{self.manufacturer_name}/{self.reporting_year}/{self.reporting_month:02}/{self.file.file_name}'