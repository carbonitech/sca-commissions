"""
Manufacturer report preprocessing definition
for Milwaukee Tool
"""
import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor

class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """processes the Milwaukee Full Detail List tab"""

        events = []
        customer_name_col: str = "Customer Name"
        city_name_col: str = "City"
        state_name_col: str = "State"
        inv_col: str = "Prorated Sales Amt"
        comm_col: str = "Commission"

        if missed_transfers := kwargs.get("additional_file_1", None):
            missed_transfers_df: pd.DataFrame = pd.read_excel(missed_transfers)
            data = pd.concat([data, missed_transfers_df])

        data = data.dropna(subset=data.columns.to_list()[0])
        events.append(("Formatting","removed all rows with no values in the first column",self.submission_id))
        result = data.loc[:,[customer_name_col, city_name_col, state_name_col, inv_col, comm_col]]
        result = result.groupby(result.columns.tolist()[:3]).sum().reset_index()
        events.append(("Formatting",
            "grouped data by report-given customer name, city, and state, "\
            f"and summed {inv_col} and {comm_col} values",
            self.submission_id
        ))
        result.loc[:,inv_col] = result[inv_col]*100
        result.loc[:,comm_col] = result[comm_col]*100
        for col in [customer_name_col,city_name_col,state_name_col]:
            result.loc[:, col] = result[col].str.upper()
            result.loc[:, col] = result[col].str.strip()
        result.columns = self.result_columns
        return PreProcessedData(result,events)


    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "full_detail_list": self._standard_report_preprocessing,
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(), **kwargs)
        else:
            return