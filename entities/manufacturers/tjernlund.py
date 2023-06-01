"""
Manufacturer report preprocessing definition
for Tjernlund
"""
import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor

class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """processes the Tjernlund standard report"""

        customer_col: str = "address1"
        invoice_num: str = 'invoice_no'
        inv_col: str = "extension"
        comm_col_i: int = -1  # depends on removing trailing columns
        comm_col: str = "comm_amt"

        data = self.check_headers_and_fix([customer_col, inv_col], data)
        data.iloc[:,5:] = data.iloc[:,5:].shift(-1) # line up customer names with values
        data = (
            data.dropna(subset=invoice_num)
                .dropna(axis=1, how="all")
        )
        data[customer_col] = data[customer_col].ffill()
        data = data.rename(columns={data.columns[comm_col_i]: comm_col})
        data[comm_col] = data[comm_col].astype(float)
        data["id_string"] = data[customer_col]
        result = data.loc[:,["id_string", inv_col, comm_col]]
        result[inv_col] *= 100
        result[comm_col] *= 100
        result = result.rename(columns={inv_col: "inv_amt"})
        return PreProcessedData(result)


    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing,
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(treat_headers=True), **kwargs)
        else:
            return