from dataclasses import dataclass
import pandas as pd
import tabula
from PyPDF2 import PdfReader
from io import BytesIO

@dataclass
class CommissionFile:
    file_data: bytes|BytesIO

    def to_df(
            self,
            combine_sheets=False,
            split_sheets=False,
            pdf: str=None,
            skip: int=0,
            treat_headers: bool=True,
            make_header_a_row: bool=False
        ) -> pd.DataFrame:
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
                if combine_sheets:
                    all_tables = tabula.read_pdf(BytesIO(self.file_data), pages="all")[skip:]
                    all_tables = [table.T.reset_index().T for table in all_tables]
                    combined =  pd.concat(all_tables, ignore_index=True)
                    return combined[~combined.loc[:,0].str.startswith('Unnamed')]
                else:
                    return tabula.read_pdf(BytesIO(self.file_data), pages="all")[skip]

        try:
            excel_data = self.excel_extract('openpyxl', combine_sheets, skip, split_sheets)
        except:
            excel_data = self.excel_extract('xlrd', combine_sheets, skip, split_sheets)

        if make_header_a_row:
            if isinstance(excel_data, dict):
                result = {sheet: data.T.reset_index().T.reset_index(drop=True) for sheet, data in excel_data.items()}
            else:
                result = excel_data.T.reset_index().T.reset_index(drop=True)
        elif treat_headers:
            # if we made a header a row, the headers become integers and this isn't needed, so treatment is an alternative
            if isinstance(excel_data, dict):
                result = {sheet: data.rename(columns=lambda col: col.lower().replace(' ')) for sheet, data in excel_data.items()}
            else:
                result = excel_data.rename(columns=lambda col: col.lower().replace(' '))
        else:
            result = excel_data
        
        return result


    def excel_extract(
            self,
            engine: str,
            combine_sheets: bool=False,
            skip: int=0,
            split_sheets: bool=False
    ) -> pd.DataFrame|dict[str,pd.DataFrame]:

        with pd.ExcelFile(self.file_data, engine=engine) as excel_file:
            excel_file: pd.ExcelFile
            if engine == "openpyxl":
                # use only visible sheets in case some irrelevant ones are hidden, but ahead of line
                # otherwise a hidden sheet would get pulled in instead of the intended visible one
                sheets = [sheet.title for sheet in excel_file.book.worksheets if sheet.sheet_state == "visible"]
            elif engine == "xlrd":
                # NOTE I'm not sure how to grab only visible sheets with xlrd
                sheets = excel_file.sheet_names

            if combine_sheets:
                data = [excel_file.parse(sheet, skiprows=skip) for sheet in sheets]
                result = pd.concat(data, ignore_index=True)
            elif split_sheets:
                data_sheets: dict[str,pd.DataFrame] = {sheet: excel_file.parse(sheet, skiprows=skip) for sheet in sheets}
                result = data_sheets
            else: 
                result = pd.read_excel(self.file_data, sheet_name=sheets[0], skiprows=skip)
        return result