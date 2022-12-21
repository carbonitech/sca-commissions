"""
Temporary processing scheme for Allied
Remarks:
    - The number of line items in Allied's report is relatively small (less than a dozen)
    - This scheme will assume a file structure that would reasonably come from a Allied, while requiring very little pre-processing work
"""
import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor

class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """processes the Allied standard report"""

        events = []
        customer_name_col: str = "Customer"
        city_name_col: str = "City"
        state_name_col: str = "State"
        inv_col: str = "Sales"
        # required_cols: set = {customer_name_col, city_name_col, state_name_col, inv_col}
        comm_rate: float = kwargs.get("standard_commission_rate", 0)

        # if not required_cols.issubset(set(data.columns.tolist())):
        #     first_header_row = data.index[data.iloc[:,3] == invoice_number_col].item()
        #     data.columns = data.loc[first_header_row]
        #     data = data.drop(first_header_row).reset_index(drop=True)

        data = data.rename(columns=lambda col: col.strip())
        data = data.dropna(subset=data.columns.to_list()[0])
        events.append(("Formatting","removed all rows with no values in the first column",self.submission_id))
        result = data.loc[:,[customer_name_col, city_name_col, state_name_col, inv_col]]
        result.loc[:,inv_col] = result[inv_col]*100
        result.loc[:,"commission"] = result[inv_col]*comm_rate
        events.append(("Formatting",f"added commissions column by calculating {comm_rate*100:,.2f}% of the inv_amt",
            self.submission_id))
        for col in [customer_name_col,city_name_col,state_name_col]:
            result.loc[:, col] = result[col].str.upper()
            result.loc[:, col] = result[col].str.strip()
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