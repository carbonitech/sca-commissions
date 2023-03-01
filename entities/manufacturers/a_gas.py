"""
Manufacturer report preprocessing definition
for Agas
"""
import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor

class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """processes the standard Agas file"""

        events = []
        customer_name_col: int = 0
        city_name_col: int = 1
        inv_col: int = -4
        comm_col: int = -1

        data = data.dropna(subset=data.columns[city_name_col])
        result = data.iloc[:,[customer_name_col, city_name_col, inv_col, comm_col]]

        customer_name_col: int = 0
        city_name_col: int = 1
        inv_col: int = 2
        comm_col: int = 3

        result.iloc[:,inv_col] = result.iloc[:,inv_col].replace(r'[^-.0-9]','',regex=True).astype(float) # convert string currency figure to float
        result.iloc[:,comm_col] = result.iloc[:,comm_col].replace(r'[^-.0-9]','',regex=True).astype(float) # convert string currency figure to float
        result.iloc[:,inv_col] = result.iloc[:,inv_col]*100
        result.iloc[:,comm_col] = result.iloc[:,comm_col]*100
        for col in [customer_name_col,city_name_col]:
            result.iloc[:, col] = result.iloc[:,col].str.upper()
            result.iloc[:, col] = result.iloc[:,col].str.strip()
        
        col_names = self.result_columns.copy()
        col_names.pop(2) # remove "state"

        result.columns = col_names
        result = result.dropna(axis=1, how="all") # drop blank state column if state not in the report
        return PreProcessedData(result,events)


    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing,
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(pdf="table"), **kwargs)
        else:
            return
