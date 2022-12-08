"""
Manufacturer report preprocessing definition
for Agas
"""
import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor

class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """processes the standard Agas file"""

        events = []
        customer_name_col: int = 0
        city_name_col: str = "City"
        state_name_col: str = "State"
        city_state_col: int = 1
        inv_col: int = 6
        comm_col: int = 9

        data = data.T.reset_index().T.reset_index()  # move headers to first row and keep row index intact
        events.append(("Formatting","moved headers to first row. Headers now integers",self.submission_id))
        data[[city_name_col, state_name_col]] = data.pop(city_state_col).str.split(", ", expand=True)
        events.append(("Formatting",f"split city-state, column {city_state_col}, into {city_name_col} and {state_name_col} columns",self.submission_id))
        data = data.dropna(subset=city_name_col)
        events.append(("Formatting",f"removed all rows with no values in the {city_name_col} column",self.submission_id))
        result = data.loc[:,[customer_name_col, city_name_col, state_name_col, inv_col, comm_col]]

        result[inv_col] = result[inv_col].replace(r'[^.0-9]','',regex=True).astype(float) # convert string currency figure to float
        result[comm_col] = result[comm_col].replace(r'[^.0-9]','',regex=True).astype(float) # convert string currency figure to float
        result.loc[:,inv_col] = result[inv_col]*100
        result.loc[:,comm_col] = result[comm_col]*100
        for col in [customer_name_col,city_name_col,state_name_col]:
            result.loc[:, col] = result[col].str.upper()
            result.loc[:, col] = result[col].str.strip()
        result.columns = self.result_columns
        result.index
        return PreProcessedData(result,events)


    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing,
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(from_pdf=True), **kwargs)
        else:
            return
