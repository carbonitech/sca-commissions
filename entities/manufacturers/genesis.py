"""
Manufacturer report preprocessing definition
for Genesis Cable / Resideo
"""
import pandas as pd
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor

class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """processes the 'Sales Detail' genesis file"""

        customer_name_col: str = "custname"
        city_name_col: str = "city"
        state_name_col: str = "state"
        inv_col: str = "amount"
        comm_col: str = "commdue"

        data = data.dropna(subset=data.columns.to_list()[0])
        result = data.loc[:,[customer_name_col, city_name_col, state_name_col, inv_col, comm_col]]
        result.loc[:,inv_col] *= 100
        result.loc[:,comm_col] *= 100
        col_names = ["customer", "city", "state", "inv_amt", "comm_amt"]
        result.columns = col_names
        result = result.apply(self.upper_all_str)
        result["id_string"] = result[col_names[:3]].apply("_".join, axis=1)
        return PreProcessedData(result)


    def _baker_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """
        processes the 'Baker' genesis file
        Remarks:
            - Entire amount is divided between in-territory states based on branch count
            - split is applied to both sales and commission
        """

        customer_name_col: str = "customername"
        city_name_col: str = "city"
        state_name_col: str = "st."
        inv_col: str = "amount"
        comm_col: str = "comm.due"

        split: float = kwargs.get("split", 1.0)
        branch_proportions: pd.DataFrame = kwargs.get("customer_proportions_by_state", pd.DataFrame())
        comm_ref_col: str   # will set which column to use for the commission calculation

        if kwargs.get("lennox"):
            alt_split = 0.01
            comm_ref_col = inv_col
        else:
            alt_split = 100*split
            comm_ref_col = comm_col

        data = data.dropna(axis=1, how='all')
        data = data.dropna(subset=data.columns.to_list()[0])
        data = data.loc[:,[customer_name_col, city_name_col, state_name_col, inv_col, comm_col]]
        data.loc[:,inv_col] = data[inv_col]*100*split
        data.loc[:,comm_col] = data[comm_ref_col]*alt_split
        
        # Locations in the data are not territory-specific although they're used to calculate the values.
        # Total sales and commissions are split between states in-territory based on total proportion of branch count
        total_inv = data[inv_col].sum()
        total_comm = data[comm_col].sum()
        if not branch_proportions.empty:
            branch_proportions["inv_amt"] = branch_proportions["total_share"]*total_inv
            branch_proportions["comm_amt"] = branch_proportions["total_share"]*total_comm
            branch_proportions['id_string'] = branch_proportions[['customer', 'state']].apply('_'.join, axis=1)
            result = branch_proportions[['id_string', 'inv_amt', 'comm_amt']]
        else:
            specified_customer = self.get_customer(**kwargs)
            result = pd.DataFrame([[specified_customer, total_inv, total_comm]], columns=['id_string', 'inv_amt', 'comm_amt'])
        return PreProcessedData(result)


    def _lennox_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """
            Processing is nearly identical to Baker's report, except minor tweaks to the use
            of commission split values. The tables have the same structure and headings after extraction

            Setting `lennox=True` will induce the following method to use commission column calculations
            appropriate for the Lennox report
        """
        return self._baker_report_preprocessing(data, lennox=True, **kwargs)

    def _winsupply_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        
        customer_name_col: str = "billtoaddress1"
        city_name_col: str = "billtoaddress4"
        city_name_alt: str = "billtoaddress3"
        state_name_col: str = "billtostate"
        inv_col: str = "mtdsales"
        comm_col: str = "comm_amt" 
        comm_col_index: int = -1 # commission column has an unreliable name

        data = data.dropna(axis=1, how='all')
        data = data.dropna(subset=data.columns.to_list()[0])
        data.loc[:,comm_col] = data.iloc[:,comm_col_index]
        data[city_name_col].fillna(data[city_name_alt], inplace=True)
        result = data.loc[:,[customer_name_col, city_name_col, state_name_col, inv_col, comm_col]]
        result.loc[:,inv_col] *= 100
        result.loc[:,comm_col] *= 100
        col_names = ["customer", "city", "state", "inv_amt", "comm_amt"]
        result.columns = col_names
        result = result.apply(self.upper_all_str)
        result["id_string"] = result[col_names[:3]].apply("_".join, axis=1)
        return PreProcessedData(result)

    def _rebate_detail_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:

        customer_name_col: str = 'customername'
        customer_name_index: int = data.columns.to_list().index(customer_name_col)
        inv_col_index: int = -2
        comm_col_index: int = -1

        result = data.iloc[:,[customer_name_index, inv_col_index, comm_col_index]]

        data = data.dropna(axis=1, how='all')
        data = data.dropna(subset=data.columns.to_list()[0])

        # negating (cent) values to reflect rebate as a refund
        result.iloc[:,inv_col_index] *= -100
        result.iloc[:,comm_col_index] *= -100

        result = result.apply(self.upper_all_str)

        col_names = ["customer", "inv_amt", "comm_amt"]
        result.columns = col_names
        result["id_string"] = result[col_names[0]]

        return PreProcessedData(result)

    def _johnstone_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:

        customer: str = self.get_customer(**kwargs)
        city: str = "storename"
        state: str = "storestate"
        sales: str = "lastmocogs"
        commission: str = 'comm_amt'
        comm_rate: float = kwargs.get('standard_commission_rate', 0)

        data = self.check_headers_and_fix([city, state, sales], data)
        data = data.dropna(subset=city)
        data.loc[:, "customer"] = customer
        data[sales] *= 100
        data[commission] = data[sales] * comm_rate
        data["id_string"] = data[["customer", city, state]].apply("_".join, axis=1)
        result = data[["id_string", sales, commission]].rename(columns={sales: "inv_amt"})
        return PreProcessedData(result)



    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "sales_detail": (self._standard_report_preprocessing,0),
            "baker_pos": (self._baker_report_preprocessing,33),
            "lennox_pos": (self._lennox_report_preprocessing,45),
            "winsupply_pos": (self._winsupply_report_preprocessing,0),
            "rebate_detail": (self._rebate_detail_report_preprocessing,0),
            "johnstone_pos": (self._johnstone_report_preprocessing,0)
        }
        preprocess_method, skip_param = method_by_name.get(self.report_name, None)
        if preprocess_method:
            if self.report_name == "winsupply_pos":
                return preprocess_method(self.file.to_df(combine_sheets=True, treat_headers=True), **kwargs)
            return preprocess_method(self.file.to_df(skip=skip_param, treat_headers=True), **kwargs)
        else:
            return