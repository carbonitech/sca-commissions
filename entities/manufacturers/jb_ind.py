"""
Manufacturer report preprocessing definition
for JB Industries
"""
import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor

class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """processes the standard JB file"""

        customer_name_col: str = "name"
        city_name_col: str = "city"
        state_name_col: str = "code"
        inv_col: str = "grosssale"
        comm_col: str = "cmsnamount"

        data = data.dropna(subset=data.columns.to_list()[0])
        result = data.loc[:,[customer_name_col, city_name_col, state_name_col, inv_col, comm_col]]
        result = result.groupby(result.columns.tolist()[:3]).sum().reset_index()
        result.loc[:,inv_col] *= 100
        result.loc[:,comm_col] *= 100
        result = result.apply(self.upper_all_str)

        col_names = ["customer", "city", "state", "inv_amt", "comm_amt"]
        result.columns = col_names
        result["id_string"] = result[col_names[:3]].apply("_".join, axis=1)
        return PreProcessedData(result)


    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing,
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(treat_headers=True), **kwargs)
        else:
            return