from dataclasses import dataclass
import pandas as pd

@dataclass
class CommissionFile:
    file_data: bytes
    sheet_name: str

    def to_df(self) -> pd.DataFrame:
        return pd.read_excel(self.file_data, sheet_name=self.sheet_name)