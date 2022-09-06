"""
Manufacturer report preprocessing definition
for Advanced Distributor Products (ADP)
"""
import pandas as pd
import numpy as np
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor

class PreProcessor(AbstractPreProcessor):
    """
    Remarks:
        - ADP's report comes as a single file with multiple tabs
        - All reports have the 'Detail' tab, which I'm calling the 'standard' report,
            but other tabs for POS reports vary in name, and sometimes in structure.
        - Reports are expected to come packaged together, seperated in one file by tabs
        
    Returns: PreProcessedData object with data and attributes set to enable further processing
    """

    def _standard_report_preprocessing(self,data: pd.DataFrame) -> PreProcessedData:
        """processes the 'Detail' tab of the ADP commission report"""
        events = []

        data.columns = [col.replace(" ","") for col in data.columns.tolist()]
        events.append(("Formatting","removed spaces from column names",self.submission_id))

        # convert dollars to cents to avoid demical imprecision
        data.NetSales = data.loc[:,"NetSales"].apply(lambda amt: amt*100)
        data.Rep1Commission = data.loc[:,"Rep1Commission"].apply(lambda amt: amt*100)

        ref_col = data.columns.tolist()[0]
        rows_null = data[data[ref_col].isna()]
        data.dropna(subset=ref_col, inplace=True)
        events.append(("Rows Removed",rows_null.rename(columns={"NetSales":"inv_amt","Rep1Commission":"comm_amt"}),self.submission_id))

        # sum by account convert to a flat table
        piv_table_values = ["NetSales", "Rep1Commission"]
        piv_table_index = ["Customer.1","ShipToCity","ShpToState","Customer","ShipTo"]
        result = pd.pivot_table(
            data,
            values=piv_table_values,
            index=piv_table_index,
            aggfunc=np.sum).reset_index()
        
        events.append(("Formatting","grouped NetSales and Rep1Commission by sold-to, "
                "ship-to, customer name, city, and state (pivot table)",self.submission_id))

        result = result.drop(columns=["Customer","ShipTo"])
        events.append(("Formatting", "dropped the ship-to and sold-to id columns",self.submission_id))

        result.columns = self.result_columns
        ref_cols = result.columns.tolist()[:3]

        for ref_col in ref_cols:
            result[ref_col] = result.loc[:,ref_col].apply(str.upper).apply(str.strip)

        return PreProcessedData(result, ref_cols, *ref_cols, events)


    def _coburn_report_preprocessing(self,data: pd.DataFrame) -> PreProcessedData:
        events = []

        comm_col_before = data.columns.values[-1]
        comm_col_after = "Commission"
        data = data.rename(columns={comm_col_before: comm_col_after}) # normalize final column in case commission rate changes
        events.append(("Formatting",f"renamed last column {str(comm_col_before)} to {comm_col_after}",self.submission_id))
        data.columns = [col.replace(" ","") for col in data.columns.tolist() if isinstance(col,str)]
        events.append(("Formatting","removed spaces from column names",self.submission_id))

        na_filter_col = "Date"
        branch_total_amount_filter = "Amount"
        key_cols = ["Amount","Branch","Location","Commission"]
        data = data[data[na_filter_col].isna()].loc[:,data.columns.isin(key_cols)]
        data = data[~data[branch_total_amount_filter].isna()]
        events.append(("Formatting",f"filtered for only blank rows in the {na_filter_col} column",self.submission_id))
        events.append(("Formatting",f"filtered out blank rows in the {branch_total_amount_filter} column",self.submission_id))
        events.append(("Formatting",f"filtered out all columns EXCEPT {', '.join(key_cols)}",self.submission_id))

        def strip_branch_number(value: str) -> int:
            return value.lower().strip().replace(" total","")

        data.loc[:,"Branch"] = data["Branch"].apply(strip_branch_number).astype(int)
        events.append(("Formatting","striped Branch column down to branch number only",self.submission_id))

        # convert dollars to cents to avoid demical imprecision
        data.loc[:,"Amount"]= data["Amount"].fillna(0).apply(lambda amt: amt*100)
        data.loc[:,"Commission"] = data["Commission"].fillna(0).apply(lambda amt: amt*100)

        # amounts are negative to represent deductions from Coburn DC shipments to outside of the SCA territory
        total_inv_adj = -data["Amount"].sum()
        total_comm_adj = -data["Commission"].sum()
        
        result = pd.DataFrame([["COBURN", "VARIOUS", "MS", total_inv_adj, total_comm_adj]], columns=self.result_columns)
        ref_cols = self.result_columns[:3]

        for ref_col in ref_cols:
            result[ref_col] = result.loc[:,ref_col].apply(str.upper).apply(str.strip)

        return PreProcessedData(result, ref_cols, *ref_cols, events)


    def _re_michel_report_preprocessing(self, data: pd.DataFrame) -> PreProcessedData: ...
    def _lennox_report_preprocessing(self, data: pd.DataFrame) -> PreProcessedData: ...

    def preprocess(self) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing,
            "coburn": self._coburn_report_preprocessing,
            "lennox": self._lennox_report_preprocessing,
            "re_michel": self._re_michel_report_preprocessing
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df())
        else:
            return