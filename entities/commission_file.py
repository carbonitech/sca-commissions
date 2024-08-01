from dataclasses import dataclass
import re
import pandas as pd
import tabula
from PyPDF2 import PdfReader
from io import BytesIO


@dataclass
class CommissionFile:
    file_data: bytes | BytesIO
    file_mime: str
    file_name: str
    file_password: str = None

    def decrypt_file(self) -> None:
        if self.file_password:
            import msoffcrypto
            from io import BytesIO

            file_decrypted = BytesIO()
            decrypter = msoffcrypto.OfficeFile(BytesIO(self.file_data))
            decrypter.load_key(password=str(self.file_password))
            decrypter.decrypt(file_decrypted)
            self.file_data = file_decrypted

    def to_df(
        self,
        combine_sheets=False,
        split_sheets=False,
        pdf: str = None,
        skip: int = 0,
        treat_headers: bool = False,
        make_header_a_row: bool = False,
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
                    all_tables = tabula.read_pdf(BytesIO(self.file_data), pages="all")[
                        skip:
                    ]
                    all_tables = [table.T.reset_index().T for table in all_tables]
                    combined = pd.concat(all_tables, ignore_index=True)
                    return combined[~combined.loc[:, 0].str.startswith("Unnamed")]
                else:
                    return tabula.read_pdf(BytesIO(self.file_data), pages="all")[skip]
        self.decrypt_file()  # only does something if a password was passed into the CommissionFile constructor
        try:
            excel_data = self.excel_extract(
                "openpyxl", combine_sheets, skip, split_sheets, treat_headers
            )
        except:
            excel_data = self.excel_extract(
                "xlrd", combine_sheets, skip, split_sheets, treat_headers
            )

        if make_header_a_row:
            if isinstance(excel_data, dict):
                result = {
                    sheet: data.T.reset_index().T.reset_index(drop=True)
                    for sheet, data in excel_data.items()
                }
            else:
                result = excel_data.T.reset_index().T.reset_index(drop=True)

        else:
            result = excel_data

        return result

    @staticmethod
    def clean_header(header: str) -> str:
        header = str(header).lower()
        header = re.sub(r"[^a-z0-9.,]", "", header)
        if header.isnumeric():
            header = "replacednumber"
        else:
            try:
                float(header)
            except:
                pass
            else:
                header = "replacednumber"
        return header

    def excel_extract(
        self,
        engine: str,
        combine_sheets: bool = False,
        skip: int = 0,
        split_sheets: bool = False,
        treat_headers: bool = False,
    ) -> pd.DataFrame | dict[str, pd.DataFrame]:

        match self.file_data:
            case BytesIO():
                file_data = self.file_data
            case bytes():
                file_data = BytesIO(self.file_data)
            case _:
                raise Exception(
                    f"file data is of type {type(self.file_data)},"
                    " expected bytes or BytesIO"
                )

        with pd.ExcelFile(file_data, engine=engine) as excel_file:
            excel_file: pd.ExcelFile
            if engine == "openpyxl":
                # use only visible sheets in case some irrelevant ones are hidden, but ahead of line
                # otherwise a hidden sheet would get pulled in instead of the intended visible one
                sheets = [
                    sheet.title
                    for sheet in excel_file.book.worksheets
                    if sheet.sheet_state == "visible"
                ]
            elif engine == "xlrd":
                # NOTE I'm not sure how to grab only visible sheets with xlrd
                sheets = excel_file.sheet_names

            if combine_sheets:
                data: list[pd.DataFrame] = [
                    excel_file.parse(sheet, skiprows=skip) for sheet in sheets
                ]
                if treat_headers:
                    data = [
                        sheet.rename(columns=lambda col: self.clean_header(col))
                        for sheet in data
                    ]
                result = pd.concat(data, ignore_index=True)
            elif split_sheets:
                data_sheets: dict[str, pd.DataFrame] = {
                    sheet: excel_file.parse(sheet, skiprows=skip) for sheet in sheets
                }
                if treat_headers:
                    result = {
                        sheet: data.rename(columns=lambda col: self.clean_header(col))
                        for sheet, data in data_sheets.items()
                    }
                else:
                    result = data_sheets
            else:
                result = pd.read_excel(file_data, sheet_name=sheets[0], skiprows=skip)
                if treat_headers:
                    result = result.rename(columns=lambda col: self.clean_header(col))
        return result
