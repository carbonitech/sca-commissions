"""
Manufacturer report preprocessing definition
for Ambro Controls
"""
import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor

class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """processes the Ambro Controls standard report"""

        customer_name_col: str = "customername"
        city_name_col: str = "shiptocity"
        state_name_col: str = "state"
        state_name_col_alt: str = "shiptostate"
        inv_col: str = "amount"
        comm_col: str = "commissionpayable"

        active_state_col = state_name_col # switch will flip to the alternate if there's an error

        data = data.dropna(subset=data.columns.to_list()[0])
        try: 
            result = data.loc[:,[customer_name_col, city_name_col, active_state_col, inv_col, comm_col]]
        except KeyError:
            active_state_col = state_name_col_alt
            result = data.loc[:,[customer_name_col, city_name_col, active_state_col, inv_col, comm_col]]
        result.loc[:,inv_col] *= 100
        result.loc[:,comm_col] *= 100
        for col in [customer_name_col,city_name_col,active_state_col]:
            result.loc[:, col] = result[col].str.upper()
            result.loc[:, col] = result[col].str.strip()
        result["id_string"] = result[[customer_name_col, city_name_col, active_state_col]].apply("_".join, axis=1)
        result.columns = ["customer","city", "state","inv_col", "comm_col", "id_string"]
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