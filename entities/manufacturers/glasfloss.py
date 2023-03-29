"""
Manufacturer report preprocessing definition
for Glasfloss
"""
import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor

class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """processes the standard Glasfloss file"""

        customer_name_col: str = "shiptoname"
        city_name_col: str = "shiptocity"
        state_name_col: str = "shiptostate"
        inv_col: str = "sumofsales"
        comm_col: str = "comm_amt"
        total_freight: float = kwargs.get("total_freight_amount", None)
        comm_rate = kwargs.get("standard_commission_rate",0)

        data = data.dropna(subset=data.columns.to_list()[1])
        data.loc[:,inv_col] = data[inv_col]*100
        if total_freight:
            total_sales = data[inv_col].sum()/100 # converted to dollars
            discount_rate = total_freight/total_sales
            data.loc[:,inv_col] = data[inv_col]*(1-discount_rate)
            data.loc[:,comm_col] = data[inv_col]*comm_rate
        else:
            data.loc[:,comm_col] = 0

        result = data.loc[:,[customer_name_col, city_name_col, state_name_col, inv_col, comm_col]]
        col_names = ["customer", "city", "state", "inv_col", comm_col]
        result["id_string"] = result[col_names[:3]].apply("_".join, axis=1)
        result.columns = col_names

        return PreProcessedData(result)


    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": (self._standard_report_preprocessing, 2),
        }
        preprocess_method, skip_param = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(skip=skip_param), **kwargs)
        else:
            return