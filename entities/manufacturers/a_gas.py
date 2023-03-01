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
        inv_col: int = 7
        comm_col: int = 10
        no_state: bool = False

        data = data.T.reset_index().T.reset_index()  # move headers to first row and keep row index intact
        try:
            city_state_data =  data.pop(city_state_col)
            data[[city_name_col, state_name_col]] = city_state_data.str.split(", ", expand=True)
        except ValueError:
            # assuming this error means the state was not included in the data, only city.
            data[city_name_col] = city_state_data
            no_state = True
        data = data.dropna(subset=city_name_col)
        if no_state:
            result = data.loc[:,[customer_name_col, city_name_col, inv_col, comm_col]]
        else:    
            result = data.loc[:,[customer_name_col, city_name_col, state_name_col, inv_col, comm_col]]

        result[inv_col] = result[inv_col].replace(r'[^-.0-9]','',regex=True).astype(float) # convert string currency figure to float
        result[comm_col] = result[comm_col].replace(r'[^-.0-9]','',regex=True).astype(float) # convert string currency figure to float
        result.loc[:,inv_col] = result[inv_col]*100
        result.loc[:,comm_col] = result[comm_col]*100
        for col in [customer_name_col,city_name_col,state_name_col]:
            if col == state_name_col and no_state:
                continue
            result.loc[:, col] = result[col].str.upper()
            result.loc[:, col] = result[col].str.strip()
        
        col_names = self.result_columns.copy()
        if no_state:
            col_names.pop(2) # remove "state"

        result.columns = col_names
        result = result.dropna(axis=1, how="all") # drop blank state column if state not in the report
        return PreProcessedData(result,events)


    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing,
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(pdf="table"), **kwargs)
        else:
            return
