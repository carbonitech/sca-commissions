"""
Manufacturer report preprocessing definition
for Berry Global
"""
import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor

class PreProcessor(AbstractPreProcessor):
    """
    Remarks:
        - Berry sends a standard with one tab and a POS report file with seperate tabs for each account
        - standard reports are monthly and POS reports are quarterly

        
    Returns: PreProcessedData object with data and attributes set to enable further processing
    """

    def _stanard_report_preprocessing(self, data: pd.DataFrame) -> PreProcessedData:
        
        events = []
        customer_name_col: str = "BILL TO NAME"
        city_name_col: str = "CITY"
        state_name_col: str = "STATE"
        inv_col: str = "COMMISSIONABLE SALES"
        comm_col: str = "COMMISSION"

        data = data.dropna(subset=data.columns.to_list()[0])
        events.append(("Formatting","removed all rows with no values in the first column",self.submission_id))
        data = data[data["STATUS"] == "CLSD"]
        events.append(("Formatting","kept only rows showing STATUS as CLSD",self.submission_id))
        result = data.loc[:,[customer_name_col, city_name_col, state_name_col, inv_col, comm_col]]
        result.loc[:,inv_col] = result[inv_col]*100
        result.loc[:,comm_col] = result[comm_col]*100
        result.columns = self.result_columns # local result.cols are same length and position as self.result_columns
        return PreProcessedData(result,events)

    def preprocess(self) -> PreProcessedData:
        method_by_name = {
            "standard": self._stanard_report_preprocessing,
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df())
        else:
            return