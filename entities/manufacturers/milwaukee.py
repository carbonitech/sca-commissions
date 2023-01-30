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
        customer_name_col: str = "customer name"
        city_name_col: str = "city"
        state_name_col: str = "state"
        inv_col: str = "prorated sales amt"
        comm_col: str = "commission"

        if missed_transfers := kwargs.get("additional_file_1", None):
            missed_transfers_df: pd.DataFrame = pd.read_excel(missed_transfers)
            data = pd.concat([data, missed_transfers_df])
        
        data = data.rename(columns=lambda col: col.strip().lower())
        target_cols = [customer_name_col, city_name_col, state_name_col, inv_col, comm_col]
        data = data.dropna(subset=data.columns.to_list()[0])
        events.append(("Formatting","removed all rows with no values in the first column",self.submission_id))
        result = data.loc[:,target_cols]
        result[inv_col] = pd.to_numeric(result[inv_col], errors="coerce").fillna(0)
        result[comm_col] = pd.to_numeric(result[comm_col], errors="coerce").fillna(0)

        result = result.groupby(result.columns.tolist()[:3]).sum().reset_index()
        events.append(("Formatting",
            "grouped data by report-given customer name, city, and state, "\
            f"and summed {inv_col} and {comm_col} values",
            self.submission_id
        ))
        result.columns = target_cols
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