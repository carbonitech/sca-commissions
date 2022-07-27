
from datetime import datetime
from dataclasses import dataclass, field
from pandas import DataFrame
from entities.commission_file import CommissionFile

@dataclass
class NewSubmission:
    file: CommissionFile
    reporting_month: int
    reporting_year: int
    report_id: int
    manufacturer_id: int
    submission_date: datetime = field(default_factory=datetime.today, init=False)

    def keys(self):
        unpackable_attrs = list(self.__dict__.keys())
        unpackable_attrs.remove("file")
        unpackable_attrs.remove("manufacturer_id")
        return unpackable_attrs
        
    def __getitem__(self,key):
        return getattr(self,key)

    def file_df(self) -> DataFrame:
        return self.file.to_df()
    