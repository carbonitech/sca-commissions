from abc import ABC, abstractmethod

from pandas import Series, DataFrame
from entities.commission_data import PreProcessedData
from entities.commission_file import CommissionFile

class AbstractPreProcessor(ABC):

    EXPECTED_TYPES = {
        'id_string': object,
        'inv_amt': float,
        'comm_amt': float
    }
    
    def __init__(self, report_name: str, submission_id: int, file: CommissionFile):
        self.report_name = report_name
        self.submission_id = submission_id
        self.file = file

    @staticmethod
    def upper_all_str(col: Series) -> Series:
        col_cp = col.copy()
        if col_cp.dtype == "object":
            try:
                col_cp = col_cp.str.upper().str.strip()
            except:
                pass
        return col_cp

    @staticmethod
    def get_customer(**kwargs) -> str:
        """Get the specified customer sent as specified_customer
        
            If found, the customer will 
            be represented as a `tuple[int,str]`,
            where `int` is the customer id
            and `str` is the customer name.
        
            The customer name is returned.
        """
        if customer := kwargs.get('specified_customer', None):
            return customer[1]
        else:
            return 'customer'


    
    @staticmethod
    def check_headers_and_fix(cols: str|list[str], df: DataFrame) -> DataFrame:
        """check that a dataframe's headers contain the column name(s)
        supplied in cols. If the dataframe columns do not match,
        iterate through the rows to find the first row with column headers
        and set the dataframe as if all prior rows had been skipped upon loading
        
        if the rows never reveal column headers, the original dataframe is returned

        Returns: DataFrame"""

        if isinstance(cols, str):
            cols = [cols]
        # check that my columns contain the headers I expect
        df = df.rename(columns=lambda col: str(col).lower().replace(' ','')) 
        if not set(cols) & set(df.columns):
            # iterate through the rows until we find the column header
            for index, row in df.iterrows():
                row_vals = [str(value).lower().replace(' ','') for value in row.values.tolist()]
                if set(cols) & set(row_vals):
                    # set the df to use the column row as header
                    # and next row is the first row
                    df.columns = row_vals
                    df = df.iloc[index+1:]
                    break
        return df

        
    @abstractmethod
    def preprocess(self, **kwargs)-> PreProcessedData:
        pass