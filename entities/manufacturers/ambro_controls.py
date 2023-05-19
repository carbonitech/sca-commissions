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
        state_name_col_alt0: str = "shiptostate"
        state_name_col_alt1: str = "county"
        inv_col: str = "amount"
        comm_col: str = "commissionpayable"

        active_state_col = state_name_col # switch will flip to the alternate if there's an error
        data = self.check_headers_and_fix([customer_name_col, city_name_col,inv_col], data)
        data = data.dropna(subset=data.columns.to_list()[0])
        try: 
            result = data.loc[:,[customer_name_col, city_name_col, active_state_col, inv_col, comm_col]]
        except KeyError:
            active_state_col = state_name_col_alt0
            try:
                result = data.loc[:,[customer_name_col, city_name_col, active_state_col, inv_col, comm_col]]
            except KeyError:
                active_state_col = state_name_col_alt1
                result = data.loc[:,[customer_name_col, city_name_col, active_state_col, inv_col, comm_col]]
                
        result.loc[:,inv_col] *= 100
        result.loc[:,comm_col] *= 100
        result = result.apply(self.upper_all_str)
        result["id_string"] = result[[customer_name_col, city_name_col, active_state_col]].apply("_".join, axis=1)
        result.columns = ["customer","city", "state", "inv_amt", "comm_amt", "id_string"]
        return PreProcessedData(result)

    def _re_michel_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """process the RE Michel report"""
        raise NotImplementedError


    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing,
            "re_michel_pos": self._re_michel_report_preprocessing
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(treat_headers=True), **kwargs)
        else:
            return