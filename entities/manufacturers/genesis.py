"""
Manufacturer report preprocessing definition
for Genesis Cable / Resideo
"""
import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor

class PreProcessor(AbstractPreProcessor):


    def _standard_report_preprocessing(self, data: pd.DataFrame) -> PreProcessedData:
        """processes the 'Sales Detail' genesis file"""

        events = []
        customer_name_col: str = "Cust Name"
        city_name_col: str = "City"
        state_name_col: str = "State"
        inv_col: str = "Amount"
        comm_col: str = "Comm Due"

        data = data.dropna(subset=data.columns.to_list()[0])
        events.append(("Formatting","removed all rows with no values in the first column",self.submission_id))
        result = data.loc[:,[customer_name_col, city_name_col, state_name_col, inv_col, comm_col]]
        result.loc[:,inv_col] = result[inv_col]*100
        result.loc[:,comm_col] = result[comm_col]*100
        result.columns = self.result_columns # local result.cols are same length and position as self.result_columns
        return PreProcessedData(result,events)


    def preprocess(self) -> PreProcessedData:
        method_by_name = {
            "sales_detail": self._standard_report_preprocessing,
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df())
        else:
            return