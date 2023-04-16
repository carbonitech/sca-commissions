"""
Manufacturer report preprocessing definition
for Friedrich A/C
"""
import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor

class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """processes the Friedrich Paid tab"""

        customer_name_col: str = "customername"
        city_name_col: str = "shiptocity"
        state_name_col: str = "shiptostate"
        inv_col: str = "netsales"
        comm_col: str = "repcomission"

        data = data.dropna(subset=data.columns.to_list()[0])
        if customer_name_col not in data.columns.to_list():
            data = data.rename(columns=data.iloc[0]).drop(data.index[0])
        data = data.dropna(how="all",axis=1)

        result = data.loc[:,[customer_name_col, city_name_col, state_name_col, inv_col, comm_col]]

        result.loc[:,inv_col] *= 100
        result.loc[:,comm_col] *= 100
        result = result.apply(self.upper_all_str)
        col_names = ["customer", "city", "state", "inv_amt", "comm_amt"]
        result.columns = col_names
        result["id_string"] = result[col_names[:3]].apply("_".join, axis=1)

        return PreProcessedData(result)


    def _johnstone_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """processes the Friedrich Johnstone tab"""

        events = []
        default_customer_name: str = "JOHNSTONE SUPPLY"
        store_number_col: str = "Store Number"
        city_name_col: str = "CustName"
        state_name_col: str = "State"
        inv_col: str = "NetSales"
        comm_col: str = "Commission"

        data = data.dropna(subset=data.columns.to_list()[0])
        events.append(("Formatting","removed all rows with no values in the first column",self.submission_id))
        if city_name_col not in data.columns.to_list():
            data = data.rename(columns=data.iloc[0]).drop(data.index[0])
        data = data.dropna(how="all",axis=1)
        events.append(("Formatting","removed columns with no values",self.submission_id))
        data[store_number_col] = data[store_number_col].astype(str)
        data[store_number_col] = data[store_number_col].str.strip()
        data.loc[:,"customer"] = default_customer_name
        events.append(("Formatting",f"added a column with customer name {default_customer_name} in all rows",
            self.submission_id))
        result = data.loc[:,
            [store_number_col, "customer", city_name_col, state_name_col, inv_col, comm_col]
        ]

        result.loc[:,inv_col] *= 100
        result.loc[:,comm_col] *= 100
        
        result = result.apply(self.upper_all_str)

        result.columns = ["store_number"] + self.result_columns
        return PreProcessedData(result,events)


    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "paid": (self._standard_report_preprocessing,1),
            "johnstone_pos": (self._johnstone_report_preprocessing,1)
        }
        preprocess_method, skip_param = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(skip=skip_param), **kwargs)
        else:
            return