from dataclasses import dataclass
import pandas as pd
from io import BytesIO

@dataclass
class CommissionFile:
    file_data: bytes|BytesIO

    def to_df(self, combine_sheets=False) -> pd.DataFrame:
        if combine_sheets:
            with pd.ExcelFile(self.file_data) as excel_file:
                excel_file: pd.ExcelFile
                data = [excel_file.parse(sheet) for sheet in excel_file.sheet_names]
                return pd.concat(data, ignore_index=True)
        return pd.read_excel(self.file_data)
