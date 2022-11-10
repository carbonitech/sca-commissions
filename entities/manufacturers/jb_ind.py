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

        events = []
        customer_name_col: str = "Name"
        city_name_col: str = "City"
        state_name_col: str = "Code"
        inv_col: str = "Gross Sale"
        comm_col: str = "Cmsn Amount"

        data = data.dropna(subset=data.columns.to_list()[0])
        events.append(("Formatting","removed all rows with no values in the first column",self.submission_id))
        result = data.loc[:,[customer_name_col, city_name_col, state_name_col, inv_col, comm_col]]
        result = result.groupby(result.columns.tolist()[:3]).sum().reset_index()
        events.append(("Formatting",
            "grouped data by report-given customer name, city, and state, "\
            f"and summed {inv_col} and {comm_col} values",
            self.submission_id
        ))
        data.loc[:,inv_col] = data[inv_col]*100
        data.loc[:,comm_col] = data[comm_col]*100
        for col in [customer_name_col,city_name_col,state_name_col]:
            data.loc[:, col] = data[col].str.upper
            data.loc[:, col] = data[col].str.strip
        result.columns = self.result_columns
        return PreProcessedData(result,events)


    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing,
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(), **kwargs)
        else:
            return