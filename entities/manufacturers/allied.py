"""
Temporary processing scheme for Allied
Remarks:
    - The number of line items in Allied's report is relatively small (less than a dozen)
    - This scheme will assume a file structure that would reasonably come from a Allied, while requiring very little pre-processing work
"""
import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor
from numpy import nan

class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """processes the Allied standard report"""

        customer_name_col: str = "customer"
        inv_col: str = "inv_amt"
        comm_rate: float = kwargs.get("standard_commission_rate", 0)
        
        # replace dashes so empty col removal works, drop empty cols, then leave out grand totals for rows and cols
        data = data.replace('-',nan).dropna(how="all",axis=1).iloc[:-1,:-1]
        data = data.iloc[:,[0,-1]]  # get most recent month only
        data.columns = [customer_name_col, inv_col]

        data[inv_col] = data[inv_col].replace('[^-.0-9]','', regex=True).astype(float)*100
        data["comm_amt"] = data[inv_col]*comm_rate
        data = data.apply(self.upper_all_str)
        # get duplicated names
        dup_names = data.loc[data.duplicated(subset="customer"), ["customer"]].astype(str)
        # create a sequence column by reseting the index twice and reinstating the original index
        dup_names = dup_names.reset_index().reset_index().set_index("index")
        # add the suffix as a hyphen and the sequence number
        dup_names["customer"] += "-" + dup_names["level_0"].astype(str) # level_0 is the column name assigned to the column created by the second reset_index call
        # selectively replace values with suffixed values by index-label
        data.loc[dup_names.index,"customer"] = dup_names["customer"]

        data["id_string"] = data[customer_name_col]
        result = data.iloc[:,-3:].dropna()
        return PreProcessedData(result)


    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": (self._standard_report_preprocessing, 1),
        }
        preprocess_method, skip_param = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(skip=skip_param, pdf="table"), **kwargs)
        else:
            return