"""
Manufacturer report preprocessing definition
for Hardcast
"""
import pandas as pd
import numpy as np
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

        events = []
        customer_name_col: str = "Sold To"
        city_name_col: str = "ShipTo City"
        state_name_col: str = "Ship To State"
        inv_col: str = "Sales Base for Comm"
        comm_col: str = "Comm"

        drop_col = "Sales Group"
        data = data.drop(columns=drop_col)
        events.append(("Formatting",f"dropped column {drop_col}",self.submission_id))
        data = data.loc[~(data["Comm Rate"] == "*"),:]
        events.append(("Formatting","removed all rows with a star (*) in the Comm Rate column",self.submission_id))
        data = data.fillna(method="ffill")
        events.append(("Formatting",f"converted to a flat table by copying customer info (name, city, state) into blank rows",self.submission_id))
        result = data.loc[:,[customer_name_col, city_name_col, state_name_col, inv_col, comm_col]]
        result.loc[:,inv_col] = result[inv_col]*100
        result.loc[:,comm_col] = result[comm_col]*100
        result.columns = self.result_columns # local result.cols are same length and position as self.result_columns
        return PreProcessedData(result,events)


    def preprocess(self) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing,
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df())
        else:
            return