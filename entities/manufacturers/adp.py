"""
Manufacturer report preprocessing definition
for Advanced Distributor Products (ADP)
"""
import pandas as pd
import numpy as np
from entities.commission_data import PreProcessedData
from entities.preprocessor import PreProcessor
from app import event

class ADPPreProcessor(PreProcessor):
    """
    Remarks:
        - ADP's report comes as a single file with multiple tabs
        - All reports have the 'Detail' tab, which I'm calling the 'standard' report,
            but other tabs for POS reports vary in name, and sometimes in structure.
        - Reports are expected to come packaged together, seperated in one file by tabs
        
    Returns: PreProcessedData object with data and attributes set to enable further processing
    """

    def _standard_report_preprocessing(self) -> PreProcessedData:
        """processes the 'Detail' tab of the ADP commission report"""

        data = self.submission.file_df()

        data.columns = [col.replace(" ","") for col in data.columns.tolist()]
        event.post_event("Formatting","removed spaces from column names",self.submission_id)

        # convert dollars to cents to avoid demical precision weirdness
        data.NetSales = data.loc[:,"NetSales"].apply(lambda amt: amt*100)
        data.Rep1Commission = data.loc[:,"Rep1Commission"].apply(lambda amt: amt*100)

        ref_col = data.columns.tolist()[0]
        rows_null = data[data[ref_col].isna()]
        data.dropna(subset=ref_col, inplace=True)
        event.post_event("Rows Removed",rows_null.rename(columns={"NetSales":"inv_amt","Rep1Commission":"comm_amt"}),self.submission_id)

        # sum by account convert to a flat table
        piv_table_values = ["NetSales", "Rep1Commission"]
        piv_table_index = ["Customer.1","ShipToCity","ShpToState","Customer","ShipTo"]
        result = pd.pivot_table(
            data,
            values=piv_table_values,
            index=piv_table_index,
            aggfunc=np.sum).reset_index()
        
        event.post_event("Formatting","grouped NetSales and Rep1Commission by sold-to, "
                "ship-to, customer name, city, and state (pivot table)",self.submission_id)

        result = result.drop(columns=["Customer","ShipTo"])
        event.post_event("Formatting", "dropped the ship-to and sold-to id columns",self.submission_id)

        customer_name_col = 'customer'
        city_name_col = 'city'
        state_name_col = 'state'
        result.columns=[customer_name_col,city_name_col,state_name_col,"inv_amt","comm_amt"]
        ref_cols = result.columns.tolist()[:3]

        for ref_col in ref_cols:
            result[ref_col] = result.loc[:,ref_col].apply(str.upper)

        return PreProcessedData(result,ref_cols,customer_name_col,city_name_col,state_name_col)


    def _coburn_report_preprocessing(self) -> PreProcessedData: ...
    def _re_michel_report_preprocessing(self) -> PreProcessedData: ...
    def _lennox_report_preprocessing(self) -> PreProcessedData: ...

    def preprocess(self) -> PreProcessedData:
        method_by_id = {
            1: self._standard_report_preprocessing,
            2: self._coburn_report_preprocessing,
            3: self._lennox_report_preprocessing,
            4: self._re_michel_report_preprocessing
        }
        preprocess_method = method_by_id.get(self.submission.report_id, None)
        if preprocess_method:
            return preprocess_method()
        else:
            return