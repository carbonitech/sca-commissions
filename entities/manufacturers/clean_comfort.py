"""
Manufacturer report preprocessing definition
for Clean Comfort
"""
import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor

class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:

        customer_or_branch: str = 'entity'
        city: str = 'city'
        state: str = 'state'
        inv_amt: str = 'netamt'
        comm_rate = kwargs.get('standard_commission_rate', 0)

        id_cols = [customer_or_branch, city, state]

        data = self.check_headers_and_fix(id_cols, data)
        data = data.dropna(subset=data.columns[0]).apply(self.upper_all_str)
        data[inv_amt] = data[inv_amt].astype(float) # loads as an 'object' dtype
        data[inv_amt] *= 100
        data["comm_amt"] = data[inv_amt]*comm_rate
        data = data.groupby(id_cols).sum(numeric_only=True).reset_index()
        data["id_string"] = data[id_cols].apply("_".join, axis=1)
        result = data.loc[:,["id_string",inv_amt,"comm_amt"]]
        result = result.rename(columns={inv_amt: "inv_amt"})

        return PreProcessedData(result)

    def _prostat_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        return self._standard_report_preprocessing(data, **kwargs)

    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": (self._standard_report_preprocessing,3),
            "prostat": (self._prostat_report_preprocessing,3),
        }
        preprocess_method, skip_param = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(skip=skip_param), **kwargs)
        else:
            return