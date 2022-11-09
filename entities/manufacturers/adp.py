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
        events.append(("Rows Removed",rows_null.rename(
            columns={"NetSales":"inv_amt","Rep1Commission":"comm_amt"}
        ),self.submission_id))

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

        return PreProcessedData(result, events)


    def _coburn_report_preprocessing(self,data: pd.DataFrame) -> PreProcessedData:
        """
        Process any tab of the report for Coburn's.
        Only the the sums of sales and commissions are used, 
            and they are reported as a single entry of negative amounts,
            under Customer: COBURN, City: VARIOUS, State: MS
        """
        events = []
        default_customer_name = "COBURN"

        comm_col_before = data.columns.values[-1]
        comm_col_after = "Commission"
        data = data.rename(columns={comm_col_before: comm_col_after}) # if commission rate changes, this column name would change
        events.append(("Formatting",f"renamed last column {str(comm_col_before)} to {comm_col_after}",
            self.submission_id))
        data.columns = [col.replace(" ","") for col in data.columns.tolist() if isinstance(col,str)]
        events.append(("Formatting","removed spaces from column names",self.submission_id))

        na_filter_col = "Date"
        branch_total_amount_filter = "Amount"
        key_cols = ["Amount","Branch","Location","Commission"]
        data = data[data[na_filter_col].isna()].loc[:,data.columns.isin(key_cols)]
        data = data[~data[branch_total_amount_filter].isna()]
        events.append(("Formatting",f"filtered for only blank rows in the {na_filter_col} column",
            self.submission_id))
        events.append(("Formatting",f"filtered out blank rows in the {branch_total_amount_filter} column",
            self.submission_id))
        events.append(("Formatting",f"filtered out all columns EXCEPT {', '.join(key_cols)}",
            self.submission_id))

        def strip_branch_number(value: str) -> int:
            return value.lower().strip().replace(" total","")

        data["Branch"] = data["Branch"].apply(strip_branch_number).astype(int)
        events.append(("Formatting","striped Branch column down to branch number only",self.submission_id))

        # convert dollars to cents to avoid demical imprecision
        data.loc[:,"Amount"]= data["Amount"].fillna(0).apply(lambda amt: amt*100)
        data.loc[:,"Commission"] = data["Commission"].fillna(0).apply(lambda amt: amt*100)

        # amounts are negative to represent deductions from Coburn DC shipments to outside of the SCA territory
        total_inv_adj = -data["Amount"].sum()
        total_comm_adj = -data["Commission"].sum()
        
        result = pd.DataFrame([[default_customer_name, "VARIOUS", "MS", total_inv_adj, total_comm_adj]],
            columns=self.result_columns)
        ref_cols = self.result_columns[:3]

        for ref_col in ref_cols:
            result[ref_col] = result.loc[:,ref_col].apply(str.upper).apply(str.strip)

        return PreProcessedData(result, events)


    def _re_michel_report_preprocessing(self, data: pd.DataFrame) -> PreProcessedData:
        """
        Process any tab of the report for RE Michel
        The data provides branch numbers as well as locations
        Branch numbers are stored in the database and should be used 
            as the preferred method for matching to database customer entities
        Locations in the form of city-state could be parsed out, but with more work
            and prone to run into errors.

        Returns a dataset with branch number, customer name (a default value), 
            and amounts per branch

        """
        events = []
        default_customer_name = "RE MICHEL"

        data = data.dropna(subset=data.columns.tolist()[0])
        events.append(("Formatting","removed all rows with no values in the first column",self.submission_id))

        data = data.dropna(axis=1, how='all')
        events.append(("Formatting","removed columns with no values",self.submission_id))

        data.loc[:,"store_number"] = data.pop("Branch#").astype(str)
        data.loc[:,"inv_amt"] = data.pop("Cost")*0.75*100
        events.append(("Formatting",r"replaced 'Cost' column with 75% of the value, renamed as 'inv_amt'",
            self.submission_id))

        data.loc[:,"comm_amt"] = data["inv_amt"]*0.03
        events.append(("Formatting",r"added commissions column by calculating 3% of the inv_amt",
            self.submission_id))

        data.loc[:,"customer"] = default_customer_name
        events.append(("Formatting",f"added a column with customer name {default_customer_name} in all rows",
            self.submission_id))

        result = data.loc[:,["store_number", "customer", "inv_amt", "comm_amt"]]
        return PreProcessedData(result, events)
        

    def _lennox_report_preprocessing(self, data: pd.DataFrame) -> PreProcessedData:
        
        events = []
        default_customer_name = "LENNOX"

        data = data.dropna(subset=data.columns.tolist()[0])
        events.append(("Formatting","removed all rows with no values in the first column",self.submission_id))

        data.loc[:,"receiving"] = data["Plnt"]
        data.loc[:,"receiving_city"] = data["City"]
        data.loc[:,"receiving_state"] = data["Rg"]
        data.loc[:,"sending"] = data["SPlt"]
        data = data.merge(
            data["Warehouse"].apply(
                lambda value: pd.Series({"sending_city": value.split(", ")[0], "sending_state": value.split(", ")[1]})
            ),
            left_index=True,
            right_index=True
        )
        data.loc[:,"inv_amt"] = data["Ext Price"]*100
        data.loc[:,"comm_amt"] = data["Commission"]*100
        data.loc[:,"customer"] = default_customer_name
        events.append(("Formatting",f"added a column with customer name {default_customer_name} in all rows",
            self.submission_id))
        result_cols = ["receiving_city", "receiving_state", "sending_city", "sending_state", "customer", "inv_amt", "comm_amt", "receiving", "sending"]
        result = data.loc[:,result_cols]
        
        # unpivot sending and receiving (trailing columns) into "direction" (sending/receiving) and warehouse code (i.e. A300)
        result = pd.melt(result,id_vars=result_cols[:-2],var_name="direction",value_name="store_number")
        # sending table only
        sending_table = result.loc[
            result["direction"] == "sending", 
            ["sending_city", "sending_state", "customer", "inv_amt", "comm_amt", "direction", "store_number"]
        ]
        sending_table = sending_table.rename(columns={"sending_city": "city", "sending_state": "state"})
        # receiving table only
        receiving_table = result.loc[
            result["direction"] == "receiving", 
            ["receiving_city", "receiving_state", "customer", "inv_amt", "comm_amt", "direction", "store_number"]
        ]
        receiving_table = receiving_table.rename(columns={"receiving_city": "city", "receiving_state": "state"})
        # recombine (city, state, customer, inv_amt, comm_amt, direction, store_number)
        result = pd.concat([sending_table,receiving_table], ignore_index=True)
        return PreProcessedData(result, events)


    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "detail": self._standard_report_preprocessing,
            "coburn_pos": self._coburn_report_preprocessing,
            "lennox_pos": self._lennox_report_preprocessing,
            "re_michel_pos": self._re_michel_report_preprocessing
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            if self.report_name == "lennox_pos":
                return preprocess_method(self.file.to_df(combine_sheets=True))
            return preprocess_method(self.file.to_df())
        else:
            return

