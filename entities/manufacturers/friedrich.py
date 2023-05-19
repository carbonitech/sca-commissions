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

        default_customer_name: str = "JOHNSTONE SUPPLY"
        store_number_col: str = "storenumber"
        city_name_col: str = "custname"
        state_name_col: str = "state"
        inv_col: str = "netsales"
        comm_col: str = "commission"

        data = data.dropna(subset=data.columns[0])
        data = data.dropna(how="all",axis=1)
        data[store_number_col] = data[store_number_col].astype(str)
        data[store_number_col] = data[store_number_col].str.strip()
        data["customer"] = default_customer_name
        result = data.loc[:,
            [store_number_col, "customer", city_name_col, state_name_col, inv_col, comm_col]
        ]
        result.loc[:,inv_col] *= 100
        result.loc[:,comm_col] *= 100
        result['id_string'] = result[[store_number_col, "customer", city_name_col, state_name_col]].fillna('').apply('_'.join, axis=1)
        result = result.loc[:,['id_string', inv_col, comm_col]]
        result = result.rename(columns={inv_col: "inv_amt", comm_col: 'comm_amt'})
        result = result.apply(self.upper_all_str)
        return PreProcessedData(result)


    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "paid": (self._standard_report_preprocessing,1),
            "johnstone_pos": (self._johnstone_report_preprocessing,1)
        }
        preprocess_method, skip_param = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(skip=skip_param, treat_headers=True), **kwargs)
        else:
            return