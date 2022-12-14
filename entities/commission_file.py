from dataclasses import dataclass
import pandas as pd
import tabula
from PyPDF2 import PdfFileReader
from io import BytesIO

@dataclass
class CommissionFile:
    file_data: bytes|BytesIO

    def to_df(self, combine_sheets=False, split_sheets=False, pdf: str=None) -> pd.DataFrame:
        """
        read only visible sheets in the excel file
        if combine_sheets is True, attempt to UNION all visible sheets

        For PDF files, two strategies are available
            "text": raw text dumped into a Panadas Series. Lines split by newline/return characted 
            "table": if the data is formatted as a table in the sheet, extract that table as-is.
        """
        if strategy := pdf:
            if strategy.lower() == "text":
                all_text = ""
                for page in PdfFileReader(BytesIO(self.file_data)).pages:
                    all_text += page.extract_text()
                text_list = all_text.splitlines()
                text_list_compact = [line.strip() for line in text_list if line.strip()]
                return pd.Series(text_list_compact)
            elif strategy.lower() == "table":
                return tabula.read_pdf(BytesIO(self.file_data), pages="all")[0]

        try:
            with pd.ExcelFile(self.file_data, engine="openpyxl") as excel_file:
                excel_file: pd.ExcelFile
                visible_sheets = [sheet.title for sheet in excel_file.book.worksheets if sheet.sheet_state == "visible"]
                if combine_sheets:
                    data = [excel_file.parse(sheet) for sheet in visible_sheets]
                    return pd.concat(data, ignore_index=True)
                if split_sheets:
                    return {sheet: excel_file.parse(sheet) for sheet in visible_sheets}
                    

                
                return pd.read_excel(self.file_data, sheet_name=visible_sheets[0])
        except:
            # TODO make sure this fallback using xlrd also ignores hidden sheets
            with pd.ExcelFile(self.file_data, engine="xlrd") as excel_file:
                excel_file: pd.ExcelFile
                if combine_sheets:
                    data = [excel_file.parse(sheet) for sheet in excel_file.sheet_names]
                    return pd.concat(data, ignore_index=True)
                if split_sheets:
                    return {sheet: excel_file.parse(sheet) for sheet in excel_file.sheet_names}
                return pd.read_excel(self.file_data)