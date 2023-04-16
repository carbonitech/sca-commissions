"""
Manufacturer report preprocessing definition
for Hardcast
"""
import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor

class PreProcessor(AbstractPreProcessor):
    """
        Remarks:
            - Hardcast's report comes as a single file with several tabs, but only one tab is needed
            - Using 'Commissions by Sales Office'
    """

    def _standard_report_preprocessing(self, data: pd.DataFrame) -> PreProcessedData:
        """processes the 'Commissions by Sales Office' tab of the Hardcast commission report"""

        customer_name_col: str = "soldto"
        city_name_col: str = "shiptocity"
        state_name_col: str = "shiptostate"
        inv_col: str = "salesbaseforcomm"
        comm_col: str = "comm"

        drop_col = "salesgroup"
        data = data.drop(columns=drop_col)
        data = data.loc[~(data["commrate"] == "*"),:]
        data = data.dropna(how="all")
        data = data.fillna(method="ffill")
        data = data[data[customer_name_col].str.contains(customer_name_col) == False]

        result = data.loc[:,[customer_name_col, city_name_col, state_name_col, inv_col, comm_col]]
        result.loc[:,inv_col] *= 100
        result.loc[:,comm_col] *= 100

        result = result.apply(self.upper_all_str)

        col_names = ["customer", "city", "state", "inv_amt", "comm_amt"]
        result.columns = col_names
        result["id_string"] = result[col_names[:3]].apply("_".join, axis=1)
        return PreProcessedData(result)


    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": (self._standard_report_preprocessing,4),
        }
        preprocess_method, skip_param = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(skip=skip_param))
        else:
            return