"""
Manufacturer report preprocessing definition
for Famco
"""
import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor

class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """processes the Famco standard report"""

        events = []
        customer_name_col: str = "ship to name"
        city_name_col: str = "ship to city"
        inv_col_index: int = 13
        inv_col: str = "sales"
        comm_col: str = "extcom"

        data = data.dropna(how="all",axis=1)
        # condense the table by removing all empty cells by column and then recombining them
        data = pd.concat([data[col].dropna() for col in data.columns.to_list()], axis=1, ignore_index=True)
        # move first row up to the headers
        data.columns = data.iloc[0].str.lower()
        data = data.iloc[1:].reset_index(drop=True)
        data = data.dropna(subset=data.columns.to_list()[0])
        data = data.rename(columns={data.columns[inv_col_index]: inv_col})
        result = data.loc[:,[customer_name_col, city_name_col, inv_col, comm_col]]

        result.loc[:,inv_col] = result[inv_col]*100
        result.loc[:,comm_col] = result[comm_col]*100

        for col in [customer_name_col,city_name_col]:
            result.loc[:, col] = result[col].str.upper()
            result.loc[:, col] = result[col].str.strip()
        
        col_names = self.result_columns.copy()
        col_names.pop(2) # remove "state"
        result.columns = col_names
        return PreProcessedData(result,events)


    def _johnstone_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """processes the Famco Johnstone report"""

        events = []
        default_customer_name: str = "" # BUG USE DYNAMIC DEFAULT
        store_number_col: str = ""
        city_name_col: str = ""
        state_name_col: str = ""
        inv_col: str = ""
        comm_col: str = ""

        raise NotImplementedError
        
        result.loc[:,inv_col] = result[inv_col]*100
        result.loc[:,comm_col] = result[comm_col]*100
        
        for col in [city_name_col,state_name_col]:
            result.loc[:, col] = result[col].str.upper()
            result.loc[:, col] = result[col].str.strip()

        result.columns = ["store_number"] + self.result_columns
        return PreProcessedData(result,events)


    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing,
            "johnstone_pos": self._johnstone_report_preprocessing
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(), **kwargs)
        else:
            return