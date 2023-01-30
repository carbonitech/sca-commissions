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

        events = []
        customer_name_col: str = "customer name"
        city_name_col: str = "ship to city"
        state_name_col: str = "state"
        state_name_col_alt: str = "ship to state"
        inv_col: str = "amount"
        comm_col: str = "commission payable"


        active_state_col = state_name_col

        data = data.rename(columns=lambda col: col.strip().lower())
        data = data.dropna(subset=data.columns.to_list()[0])
        events.append(("Formatting","removed all rows with no values in the first column",self.submission_id))
        try: 
            result = data.loc[:,[customer_name_col, city_name_col, active_state_col, inv_col, comm_col]]
        except KeyError:
            active_state_col = state_name_col_alt
            result = data.loc[:,[customer_name_col, city_name_col, active_state_col, inv_col, comm_col]]
        result.loc[:,inv_col] = result[inv_col]*100
        result.loc[:,comm_col] = result[comm_col]*100
        for col in [customer_name_col,city_name_col,active_state_col]:
            result.loc[:, col] = result[col].str.upper()
            result.loc[:, col] = result[col].str.strip()
        result.columns = self.result_columns
        return PreProcessedData(result,events)


    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing,
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(), **kwargs)
        else:
            return