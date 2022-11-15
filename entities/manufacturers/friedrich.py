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

        events = []
        customer_name_col: str = "CustomerName"
        city_name_col: str = "ShipToCity"
        state_name_col: str = "ShipToState"
        inv_col: str = "Net sales"
        comm_col: str = "Rep comission"

        data = data.dropna(subset=data.columns.to_list()[0])
        events.append(("Formatting","removed all rows with no values in the first column",self.submission_id))
        if customer_name_col not in data.columns.to_list():
            data = data.rename(columns=data.iloc[0]).drop(data.index[0])
        data = data.dropna(how="all",axis=1)
        events.append(("Formatting","removed columns with no values",self.submission_id))

        result = data.loc[:,[customer_name_col, city_name_col, state_name_col, inv_col, comm_col]]

        result.loc[:,inv_col] = result[inv_col]*100
        result.loc[:,comm_col] = result[comm_col]*100
        for col in [customer_name_col,city_name_col,state_name_col]:
            result.loc[:, col] = result[col].str.upper()
            result.loc[:, col] = result[col].str.strip()
        result.columns = self.result_columns
        return PreProcessedData(result,events)


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

        result.loc[:,inv_col] = result[inv_col]*100
        result.loc[:,comm_col] = result[comm_col]*100
        
        for col in [city_name_col,state_name_col]:
            result.loc[:, col] = result[col].str.upper()
            result.loc[:, col] = result[col].str.strip()

        result.columns = ["store_number"] + self.result_columns
        return PreProcessedData(result,events)


    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "paid": self._standard_report_preprocessing,
            "johnstone_pos": self._johnstone_report_preprocessing
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(), **kwargs)
        else:
            return