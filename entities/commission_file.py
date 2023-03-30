from dataclasses import dataclass
import pandas as pd
import tabula
from PyPDF2 import PdfReader
from io import BytesIO

@dataclass
class CommissionFile:
    file_data: bytes|BytesIO

    def to_df(self, combine_sheets=False, split_sheets=False, pdf: str=None, skip: int=0) -> pd.DataFrame:
        """
        read only visible sheets in the excel file
        if combine_sheets is True, attempt to UNION all visible sheets

        `skip` parameter allows skipping of rows or pages in the output, depending on the parsing strategy used

        For PDF files, two strategies are available
            "text": raw text dumped into a Panadas Series. Lines split by newline/return characted 
            "table": if the data is formatted as a table in the sheet, extract that table as-is.

        """
        if strategy := pdf:
            if strategy.lower() == "text":
                all_text = ""
                for page in PdfReader(BytesIO(self.file_data)).pages:
                    all_text += page.extract_text()
                text_list = all_text.splitlines()
                text_list_compact = [line.strip() for line in text_list if line.strip()]
                return pd.Series(text_list_compact)
            elif strategy.lower() == "table":
                return tabula.read_pdf(BytesIO(self.file_data), pages="all")[skip]

        try:
            with pd.ExcelFile(self.file_data, engine="openpyxl") as excel_file:
                excel_file: pd.ExcelFile
                visible_sheets = [sheet.title for sheet in excel_file.book.worksheets if sheet.sheet_state == "visible"]
                if combine_sheets:
                    # columns are treated by lowering and de-spacing before combination
                    data = [
                        excel_file.parse(sheet, skiprows=skip)\
                                .rename(columns=lambda col: col.replace(" ","").lower())
                            for sheet in visible_sheets
                            ]
                    result = pd.concat(data, ignore_index=True)
                if split_sheets:
                    return {sheet: excel_file.parse(sheet, skiprows=skip) for sheet in visible_sheets}
                
                result: pd.DataFrame = pd.read_excel(self.file_data, sheet_name=visible_sheets[0], skiprows=skip)
        except:
            # TODO make sure this fallback using xlrd also ignores hidden sheets
            with pd.ExcelFile(self.file_data, engine="xlrd") as excel_file:
                excel_file: pd.ExcelFile
                if combine_sheets:
                    # columns are treated by lowering and de-spacing before combination
                    data = [
                        excel_file.parse(sheet, skiprows=skip)\
                            .rename(columns=lambda col: col.replace(" ","").lower())
                        for sheet in excel_file.sheet_names
                        ]
                    result =  pd.concat(data, ignore_index=True)
                if split_sheets:
                    return {sheet: excel_file.parse(sheet, skiprows=skip) for sheet in excel_file.sheet_names}
                result =  pd.read_excel(self.file_data, skiprows=skip)

        return result.rename(columns=lambda col: col.lower().replace(" ", ""))