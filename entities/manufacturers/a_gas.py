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

        customer_name_col: int = 0
        city_name_col: int = 1
        inv_col: int = -4
        comm_col: int = -1

        data = data.dropna(subset=data.columns[city_name_col])
        result = data.iloc[:,[customer_name_col, city_name_col, inv_col, comm_col]]

        result.columns = ["customer", "city", "int_amt", "comm_amt"]
        customer_name_col, city_name_col, inv_col, comm_col = result.columns.tolist()


        # convert string currency figure to float
        result.loc[:, inv_col] = result[inv_col].replace(r'^\(','-', regex=True)
        result.loc[:, inv_col] = result[inv_col].replace(r'[^-.0-9]','',regex=True).astype(float)

        # convert string currency figure to float
        result.loc[:, comm_col] = result[comm_col].replace(r'^\(','-', regex=True)
        result.loc[:, comm_col] = result[comm_col].replace(r'[^-.0-9]','',regex=True).astype(float)

        result.loc[:, inv_col] *= 100
        result.loc[:, comm_col] *= 100
        result = result.apply(self.upper_all_str)
        
        result = result.dropna(axis=1, how="all")
        result["id_string"] = result[[customer_name_col, city_name_col]].apply("_".join, axis=1)

        return PreProcessedData(result)


    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing,
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(pdf="table"), **kwargs)
        else:
            return
