"""
Manufacturer report preprocessing definition
for Cerro Flow
"""
import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor

class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """processes the Cerro Flow commission report"""

        customer_name_col: str = "sold to name"
        inv_col: str = "sum of net value"
        comm_col: str = "sum of comm amount"


        data = data.dropna(subset="Description")
        data = data.fillna(method="ffill")

        result = data.loc[:,[customer_name_col, inv_col, comm_col]]

        result.loc[:,inv_col] *= 100
        result.loc[:,comm_col] *= 100
        for col in [customer_name_col]:
            result.loc[:, col] = result[col].str.upper()
            result.loc[:, col] = result[col].str.strip()
        result.columns = ["customer", "inv_amt", "comm_amt"]
        result["id_string"] = result["customer"]
        return PreProcessedData(result)


    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing,
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(), **kwargs)
        else:
            return