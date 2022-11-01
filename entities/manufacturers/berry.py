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

    def _standard_report_preprocessing(self, data: pd.DataFrame) -> PreProcessedData:
        
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

    def _baker_report_preprocessing(self, data: pd.DataFrame) -> PreProcessedData:

        events = []
        default_customer_name: str = "BAKER DISTRIBUTING"
        store_number_col: str = "Store #"
        city_name_col: str = "Store Name"
        state_name_col: str = "Store State"
        inv_col_pos: int = -1         # this col name is tied to the calendar, use position to rename it
        inv_col_name: str = "inv_amt" # name for the now col to replace the position-dependent one
        comm_col: str = "comm_amt" # will be calculated

        data = data.dropna(subset=store_number_col)
        events.append(("Formatting",f"removed all rows with no values in the {store_number_col} column",self.submission_id))
        data = data.dropna(axis=1, how='all')
        events.append(("Formatting","removed columns with no values",self.submission_id))
        data[store_number_col] = data[store_number_col].astype(str)
        data.loc[:,inv_col_name] = data.iloc[:,inv_col_pos]*100 # now we have a well-named column for sales dollars
        data.loc[:,comm_col] = 0 # commission rate is dynamically generated by a total commission dollar amount received separately - deal with this down-stream
        events.append(("Formatting",f"added a commission amount column filled with zeros (0), to fill in later using the total amount",
            self.submission_id))       
        data.loc[:,"customer"] = default_customer_name
        events.append(("Formatting",f"added a column with customer name {default_customer_name} in all rows",
            self.submission_id))
        result = data.loc[:,
            [store_number_col, "customer", city_name_col, state_name_col, inv_col_name, "comm_amt"]
        ]
        result.columns = [store_number_col] + self.result_columns
        return PreProcessedData(result,events)

    def _re_michel_report_preprocessing(self, data: pd.DataFrame) -> PreProcessedData:   ...
    def _winsupply_report_preprocessing(self, data: pd.DataFrame) -> PreProcessedData:   ...
    def _johnstone_report_preprocessing(self, data: pd.DataFrame) -> PreProcessedData:   ...
    def _united_refrigeration_report_preprocessing(self, data: pd.DataFrame) -> PreProcessedData:   ...


    def preprocess(self) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing,
            "baker_pos": self._baker_report_preprocessing,
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df())
        else:
            return