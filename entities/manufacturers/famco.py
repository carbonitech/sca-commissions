"""
Manufacturer report preprocessing definition
for Famco
"""
import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor

class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """processes the Famco standard report"""

        # headers are lost once the df is condensed
        customer_name_col: int = 2
        city_name_col: int = 3
        inv_col: int = 11
        comm_col: int = -1

        data = data.dropna(how="all",axis=1)
        # condense the table by removing all empty cells by column and then recombining them
        data = pd.concat([data[col].dropna() for col in data.columns.to_list()], axis=1, ignore_index=True)
        data = data.reset_index(drop=True)
        data = data.dropna(subset=data.columns[0])
        result = data.iloc[:,[customer_name_col, city_name_col, inv_col, comm_col]]

        result.iloc[:,inv_col] *= 100
        result.iloc[:,comm_col] *= 100

        result = result.apply(self.upper_all_str)
        
        col_names = ["customer", "city", "inv_amt", "comm_amt"]
        result.columns = col_names
        result["id_string"] = result[col_names[:2]].apply("_".join, axis=1)
        return PreProcessedData(result)


    def _johnstone_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """processes the Famco Johnstone report"""

        store_number_col: str = "storeno"
        city_name_col: str = "storename"
        state_name_col: str = "storestate"
        inv_col: str = "lastmocogs"
        comm_rate = kwargs.get("standard_commission_rate",0)

        data_cols = [store_number_col, city_name_col, state_name_col, inv_col]
        data = data.loc[:,data_cols]

        data.loc[:,inv_col] *= 100
        data.loc[:,"comm_amt"] = data[inv_col]*comm_rate
        
        data = data.apply(self.upper_all_str)
        data["id_string"] = data[data_cols[:-1]].astype(str).apply("_".join, axis=1)
        
        result_cols = ["inv_amt", "comm_amt","id_string"]
        result = data.iloc[:,-3:]
        result.columns = result_cols
        return PreProcessedData(result)


    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": (self._standard_report_preprocessing,2),
            "johnstone_pos": (self._johnstone_report_preprocessing,0)
        }
        preprocess_method, skip_param = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(skip=skip_param), **kwargs)
        else:
            return