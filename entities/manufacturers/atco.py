"""
Manufacturer report preprocessing definition
for Atco Flex
"""
import re
import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor

class PreProcessor(AbstractPreProcessor):
    """
    Remarks:
        - Atco sends standard reports in one file with one tab, and POS seperately, one file each
        - standard reports are monthly and POS reports also appear to be monthly

    Returns: PreProcessedData object with data and attributes set to enable further processing
    """

    def _standard_report_preprocessing(self, data: pd.DataFrame) -> PreProcessedData:
        
        events = []
        customer_name_col: str = "Customer Name"
        city_name_col: str = "Ship To City"
        state_name_col: str = "Ship To State"
        inv_col: str = "Net Cash"
        comm_col: str = "Commission"

        data = data.dropna(subset=data.columns.to_list()[0])
        events.append(("Formatting","removed all rows with no values in the first column",self.submission_id))
        result = data.loc[:,[customer_name_col, city_name_col, state_name_col, inv_col, comm_col]]
        result.loc[:,inv_col] = result[inv_col]*100
        result.loc[:,comm_col] = result[comm_col]*100
        result.columns = self.result_columns # local result.cols are same length and position as self.result_columns
        return PreProcessedData(result,events)

    def _re_michel_report_preprocessing(self, data: pd.DataFrame) -> PreProcessedData:

        default_customer_name: str = "RE MICHEL"
        commission_rate = 0.02

        events = []
        store_number_col: int = 0
        city_name_col: int = 1
        state_name_col: int = 2
        inv_col: int = 7
        customer_name_col: int = -1
        comm_col: int = customer_name_col-1

        def isolate_city_name(row: pd.Series) -> str:
            city_state = row[city_name_col].split(" ")
            return " ".join(city_state[:city_state.index(row[state_name_col])]).upper()

        data = data.dropna(subset=data.columns.to_list()[0])
        events.append(("Formatting","removed all rows with no values in the first column",self.submission_id))
        data.iloc[:,city_name_col] = data.apply(isolate_city_name, axis=1)
        events.append(("Formatting",f"isolated city name in column {str(city_name_col+1)} by keeping everything up to the state name",self.submission_id))
        data.iloc[:,inv_col] = data.iloc[:,inv_col]*100
        data["comm_amt"] = data.iloc[:,inv_col]*commission_rate
        events.append(("Formatting",r"added commissions column by calculating "+str(commission_rate*100)+r"% of the inv_amt",
            self.submission_id))
        data["customer"] = default_customer_name
        print(data)

        result = data.iloc[:,[store_number_col, customer_name_col, city_name_col, state_name_col, inv_col, comm_col]]
        result.columns = ["store_number"] + self.result_columns
        print(result)
        return PreProcessedData(result,events)


    def preprocess(self) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing,
            "re_michel_POS": self._re_michel_report_preprocessing
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df())
        else:
            return