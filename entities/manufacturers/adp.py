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
        customer: str = "customer.1"
        city: str = "shiptocity"
        state: str = "shptostate"
        sales: str = "netsales"
        commission: str = "rep1commission"

        data = self.check_headers_and_fix([customer, city, state, sales, commission], data)
        data.dropna(subset=customer, inplace=True)
        data.loc[:,sales] *= 100
        data.loc[:,commission] *= 100

        # sum by account convert to a flat table
        piv_table_values = [sales, commission]
        piv_table_index = [customer,city,state,"customer","shipto"]
        result = pd.pivot_table(
            data,
            values=piv_table_values,
            index=piv_table_index,
            aggfunc=np.sum).reset_index()
        
        result = result.drop(columns=["customer","shipto"])
        result = result.apply(self.upper_all_str)

        result["id_string"] = result[[customer, city, state]].apply("_".join, axis=1)
        result = result[['id_string', sales, commission]].rename(columns={sales: "inv_amt", commission: "comm_amt"})
        return PreProcessedData(result)


    def _coburn_report_preprocessing(self,data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """
        Process any tab of the report for Coburn's.
        Only the the sums of sales and commissions are used, 
            and they are reported as a single entry of negative amounts
        """
        customer: str = self.get_customer(**kwargs)
        sales = "amount"
        store_id = "branch"
        location = "location"

        headers = [sales, store_id, location]
        data = self.check_headers_and_fix(headers,data)

        commission_i = data.columns.values[-1]
        commission = "comm_amt"
        data = data.rename(columns={commission_i: commission}) # if commission rate changes, this column name would change

        data = data[data["state"].isna()]
        data = data.dropna(subset=sales)

        data.loc[:, sales] = data[sales].fillna(0) * 100
        data.loc[:, commission] = data[commission].fillna(0) * 100

        # amounts are negative to represent deductions from Coburn DC shipments
        total_sales = data[sales].sum()
        total_commission = -data[commission].sum() # file negates the negative sales, so make commissions negative again
        if not data['date'].isna().all():
            # in the file variant where there are values in this field
            # the sum has captured the grand total as well as the data we want
            # halving takes the grand total back out of the number
            total_sales /= 2
            total_commission /= 2
        cols = ["id_string", "inv_amt", commission]
        result = pd.DataFrame([[customer, total_sales, total_commission]], columns=cols)
        result = result.apply(self.upper_all_str)

        return PreProcessedData(result)


    def _re_michel_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:

        customer = self.get_customer(**kwargs)
        location = "name"
        sales = "inv_amt"
        commission = "comm_amt"
        split: float = kwargs.get("split", 1.0)
        comm_rate: float = kwargs.get("standard_commission_rate",0)

        data = self.check_headers_and_fix(cols="cost", df=data)

        data = data.dropna(subset=data.columns[1])
        data = data.dropna(axis=1, how='all')
        data.loc[:, sales] = data.pop("cost")*split*100
        data.loc[:, commission] = data[sales]*comm_rate
        data.loc[:,"id_string"] = customer
        data.loc[:,"id_string"] = data[["id_string", location]].apply("_".join, axis=1)
        result = data.loc[:,["id_string", sales, commission]]
        result = result.apply(self.upper_all_str)
        return PreProcessedData(result)
        

    def _lennox_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        
        customer = self.get_customer(**kwargs)
        sales = "inv_amt"
        commission = "comm_amt"
        rec_city = "receiving_city"
        rec_state = "receiving_state"
        receiving = "receiving"
        send_city = "sending_city"
        send_state = "sending_state"
        sending = "sending"
        territory = kwargs.get('territory')
        
        data = data.dropna(subset=data.columns[0])
        data = data.rename(columns={
            "plnt": receiving,
            "city": rec_city,
            "rg": rec_state,
            "splt": sending
        })

        def split_sender_location(value: str) -> pd.Series:
            city, state = value.split(', ')
            result = {
                send_city: city,
                send_state: state
            }
            return pd.Series(result)

        data = data.merge(
            data["warehouse"].apply(split_sender_location),
            left_index=True,
            right_index=True
        )
        data.loc[:, sales] = data["extprice"]*100
        data.loc[:, commission] = data["commission"]*100
        data.loc[:, "customer"] = customer
        result_cols = [rec_city, rec_state, send_city, send_state,
                       "customer", sales, commission, receiving, sending]
        result = data.loc[:,result_cols]
        
        # split sending and receiving warehouses, assigning negative values to the sender, and then combine the tables
        result = pd.melt(result,id_vars=result_cols[:-2],var_name="direction",value_name="store_number")
        # sending table
        sending_table = result.loc[
            result["direction"] == sending, 
            [send_city, send_state, "customer", sales, commission, "direction", "store_number"]
        ]
        sending_table = sending_table.rename(columns={send_city: "city", send_state: "state"})
        sending_table[sales] *= -1
        sending_table[commission] *= -1
        # receiving table
        receiving_table = result.loc[
            result["direction"] == receiving, 
            [rec_city, rec_state, "customer", sales, commission, "direction", "store_number"]
        ]
        receiving_table = receiving_table.rename(columns={rec_city: "city", rec_state: "state"})
        # recombine 
        result = pd.concat([sending_table,receiving_table], ignore_index=True)
        assert result[sales].sum() == 0, "sales values do not add up to zero"
        assert result[commission].sum() == 0, "commission values do not add up to zero"
        result = result[result['state'].isin(territory)]
        result = result.apply(self.upper_all_str)
        result["id_string"] = result[["customer", "city", "state"]].apply("_".join, axis=1)
        result = result[['id_string', sales, commission]]
        return PreProcessedData(result)


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
                return preprocess_method(self.file.to_df(combine_sheets=True, treat_headers=True), **kwargs)
            return preprocess_method(self.file.to_df(treat_headers=True), **kwargs)
        else:
            return

