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

    def _standard_report_preprocessing(self,data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """processes the 'Detail' tab of the ADP commission report"""
        customer_name_col: str = "customer.1"
        city_name_col: str = "shiptocity"
        state_name_col: str = "shptostate"
        inv_col: str = "netsales"
        comm_col: str = "rep1commission"

        # convert dollars to cents to avoid demical imprecision
        data.loc[:,inv_col] *= 100
        data.loc[:,comm_col] *= 100

        ref_col = data.columns.tolist()[0]
        data.dropna(subset=ref_col, inplace=True)

        # sum by account convert to a flat table
        piv_table_values = [inv_col, comm_col]
        piv_table_index = [customer_name_col,city_name_col,state_name_col,"customer","shipto"]
        result = pd.pivot_table(
            data,
            values=piv_table_values,
            index=piv_table_index,
            aggfunc=np.sum).reset_index()
        
        result = result.drop(columns=["customer","shipto"])
        for col in [customer_name_col,city_name_col,state_name_col]:
            result.loc[:, col] = result[col].str.upper()
            result.loc[:, col] = result[col].str.strip()

        result["id_string"] = result[[customer_name_col, city_name_col, state_name_col]].apply("_".join, axis=1)
        result.columns = ["customer", "city", "state", "inv_amt", "comm_amt", "id_string"]
        return PreProcessedData(result)


    def _coburn_report_preprocessing(self,data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """
        Process any tab of the report for Coburn's.
        Only the the sums of sales and commissions are used, 
            and they are reported as a single entry of negative amounts
        """
        comm_col_before = data.columns.values[-1]
        comm_col_after = "commission"
        data = data.rename(columns={comm_col_before: comm_col_after}) # if commission rate changes, this column name would change
        data.columns = [col.replace(" ","").lower() for col in data.columns.tolist() if isinstance(col,str)]


        na_filter_col = "date"
        branch_total_amount_filter = "amount"
        key_cols = ["amount","branch","location","commission"]
        data = data[data[na_filter_col].isna()].loc[:,data.columns.isin(key_cols)]
        data = data[~data[branch_total_amount_filter].isna()]

        def strip_branch_number(value: str) -> int:
            return value.lower().strip().replace(" total","")

        data["branch"] = data["branch"].apply(strip_branch_number).astype(int)

        # convert dollars to cents to avoid demical imprecision
        data.loc[:,"amount"]= data["amount"].fillna(0).apply(lambda amt: amt*100)
        data.loc[:,"commission"] = data["commission"].fillna(0).apply(lambda amt: amt*100)

        # amounts are negative to represent deductions from Coburn DC shipments
        total_inv_adj = -data["amount"].sum()
        total_comm_adj = -data["commission"].sum()

        cols = ["customer", "inv_amt", "comm_amt", "id_string"]
        result = pd.DataFrame([["COBURN",total_inv_adj, total_comm_adj]],
            columns=cols[:-1])

        result.loc[:, cols[0]] = result[cols[0]].str.upper()
        result.loc[:, cols[0]] = result[cols[0]].str.strip()

        result["id_string"] = result[cols[0]]
        result.columns = cols

        return PreProcessedData(result)


    def _re_michel_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
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
        split: float = kwargs.get("split", 1.0)
        comm_rate: float = kwargs.get("standard_commission_rate",0)

        data = data.dropna(subset=data.columns[0])
        data = data.dropna(axis=1, how='all')
        data.loc[:,"store_number"] = data.pop("branch#").astype(str)
        data.loc[:,"inv_amt"] = data.pop("cost")*split*100
        data.loc[:,"comm_amt"] = data["inv_amt"]*comm_rate
        data.loc[:,"customer"] = "RE MICHEL"
        result = data.loc[:,["store_number", "customer", "inv_amt", "comm_amt"]]
        result["id_string"] = result[["store_number", "customer"]].apply("_".join, axis=1)
        return PreProcessedData(result)
        

    def _lennox_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        
        data = data.dropna(subset=data.columns.tolist()[0])
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
        data.loc[:,"customer"] = "LENNOX"
        result_cols = ["receiving_city", "receiving_state", "sending_city", "sending_state",
                       "customer", "inv_amt", "comm_amt", "receiving", "sending"]
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
        result.loc[:,"city"] = result["city"].str.upper()
        result.loc[:,"state"] = result["state"].str.upper()
        result["id_string"] = result[["store_number", "customer", "city", "state"]].apply("_".join, axis=1)
        return PreProcessedData(result)


    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "detail": (self._standard_report_preprocessing,0),
            "coburn_pos": (self._coburn_report_preprocessing,3),
            "lennox_pos": (self._lennox_report_preprocessing,0),
            "re_michel_pos": (self._re_michel_report_preprocessing,2)
        }
        preprocess_method, skip = method_by_name.get(self.report_name, None)
        if preprocess_method:
            if self.report_name == "lennox_pos":
                return preprocess_method(self.file.to_df(combine_sheets=True, skip=skip), **kwargs)
            return preprocess_method(self.file.to_df(skip=skip), **kwargs)
        else:
            return

