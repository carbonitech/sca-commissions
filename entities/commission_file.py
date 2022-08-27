from dataclasses import dataclass
import pandas as pd

@dataclass
class CommissionFile:
    file_data: bytes

    def __post_init__(self):
        with pd.ExcelFile(self.file_data) as excel_file:
            excel_file: pd.ExcelFile
            assert len(excel_file.sheet_names) == 1, "Uploaded Excel file is expected to have 1 sheet only"
    def to_df(self) -> pd.DataFrame:
        return pd.read_excel(self.file_data)
