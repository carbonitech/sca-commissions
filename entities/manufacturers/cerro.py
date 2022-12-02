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

        events = []
        customer_name_col: str = "SOLD TO NAME"
        city_name_col: str = "Ship to City"
        state_name_col: str = "Ship to State"
        inv_col: str = "Sum of NET Value"
        comm_col: str = "Sum of COMM AMOUNT"

        events.append(("Formatting",f"Using columns by name: {customer_name_col}, {city_name_col}, {state_name_col}, {inv_col}, {comm_col}",self.submission_id))

        data = data.dropna(subset="Description")
        events.append(("Formatting","removed all rows with no values in the Description column",self.submission_id))
        data = data.fillna(method="ffill")
        events.append(("Formatting",f"filled data in blank rows with the values above them",self.submission_id))

        result = data.loc[:,[customer_name_col, city_name_col, state_name_col, inv_col, comm_col]]

        result.loc[:,inv_col] = result[inv_col]*100
        result.loc[:,comm_col] = result[comm_col]*100
        for col in [customer_name_col,city_name_col,state_name_col]:
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