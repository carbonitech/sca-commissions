from dataclasses import dataclass
import pandas as pd
from io import BytesIO

@dataclass
class CommissionFile:
    file_data: bytes|BytesIO

    def to_df(self, combine_sheets=False) -> pd.DataFrame:
        """
        read only visible sheets in the excel file
        if combine_sheets is True, attempt to UNION all visible sheets
        """
        with pd.ExcelFile(self.file_data, engine="openpyxl") as excel_file:
            excel_file: pd.ExcelFile
            visible_sheets = [sheet.title for sheet in excel_file.book.worksheets if sheet.sheet_state == "visible"]
            if combine_sheets:
                data = [excel_file.parse(sheet) for sheet in visible_sheets]
                return pd.concat(data, ignore_index=True)
            
            return pd.read_excel(self.file_data, sheet_name=visible_sheets[0])
