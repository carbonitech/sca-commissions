"""
Manufacturer report preprocessing definition
for C&D Value
"""
import pandas as pd
from numpy import nan
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor

class PreProcessor(AbstractPreProcessor):

    def _standard_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        """
        processes the C&D standard report
        
        Remarks:
            - This detail report has a lot of empty space and merged cells
            - This file has sales amounts by account and invoice number, but not commissions
            - commission amounts are in a seperate file by invoice number, these are not necessarily all the same
                commission rate from one month to the next
        """
        invoice_number_col: str = "invoicenumber"
        comm_col: int = -1
        customer_col: int = 2
        inv_col_comm: int = -3

        sales_report: pd.DataFrame = pd.read_excel(kwargs.get("additional_file_1"))\
                    .rename(columns=lambda col: col.lower().replace(" ",""))
        customer_name_col: int = 4
        city_name_col: str = "city"
        state_name_col: str = "state"
        inv_col: int = 15
        invoice_num_col: int = 1
        address_col: int = 9

        # extract total commission dollars by invoice number
        if invoice_number_col not in data.columns.tolist():
            first_header_row = data.index[data.iloc[:,3] == invoice_number_col].item()
            data.columns = data.loc[first_header_row]
            data = data.drop(first_header_row).reset_index(drop=True)
        data = pd.concat([data.loc[:,invoice_number_col], data.iloc[:,[customer_col,inv_col_comm,comm_col]]], axis=1).dropna()
        data.columns = ["invoice_summary","customer","inv_amt","comm_amt"]
        data["invoice_summary"] = data["invoice_summary"].astype(int)
        data["inv_amt"] = data["inv_amt"].astype(float)*100
        data["comm_amt"] = data["comm_amt"].astype(float)*100

        # in the sales report, inoice numbers are in their own column and share the same index as customer info and sales figure
        invoice_numbers = sales_report.iloc[:,invoice_num_col].dropna()
        if "Invoice Number" in invoice_numbers.values:
            invoice_numbers = invoice_numbers.iloc[1:]
            
            
        value_indecies = invoice_numbers.index.to_list()
        sales_report = sales_report.loc[value_indecies]
        sales_report[sales_report.columns[invoice_num_col]] = sales_report.iloc[:,invoice_num_col].astype(int)
        # address is combined and needs to be split into city and state
        sales_report[[city_name_col,state_name_col]] = sales_report.iloc[:,address_col].str.split(", ", expand=True)
        sales_report.loc[:, state_name_col] = sales_report[state_name_col].str.slice(0,2)
        # make a new table with just invoice number, customer, sales, city, and state
        sales_report = pd.concat([sales_report.iloc[:,[1,customer_name_col,inv_col]], sales_report.loc[:,[city_name_col,state_name_col]]], axis=1)
        # reorder columns so invoice amount is at the end
        sales_report = sales_report.iloc[:,[0,1,3,4,2]]
        sales_report.columns = ["invoice", "customer", "city", "state", "inv_amt"]
        sales_report.loc[:, "inv_amt"] *= 100
        result = sales_report.groupby(list(sales_report.columns[:4])).sum().reset_index()
        
        # merge the sales report data with the commission report data by invoice number
        merged = data.merge(result, how="left", left_on="invoice_summary", right_on="invoice", suffixes=(None,"_1"))
        id_cols = ["customer", "city", "state"]
        merged["id_string"] = merged[id_cols].fillna("").apply("_".join, axis=1)
        # drop invoice number column
        result_cols = ["id_string", "inv_amt", "comm_amt"]
        result = merged.loc[:,result_cols]
        # standardize
        result = result.apply(self.upper_all_str)
        # without invoice column now, group by branch
        result = result.groupby("id_string").sum().reset_index()

        return PreProcessedData(result)
    

    def _baker_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        monthly_sales: str = "monthlytotal"
        commissions: str = "comm"
        store_number: str = "branch#"
        city: str = "branchcity"
        state: str = "branchstate"

        data = data.dropna(subset=data.columns[0])
        data["inv_amt"] = data.loc[:,data.columns.str.startswith(monthly_sales)].fillna(0).sum(axis=1)*100
        data["comm_amt"] = data.loc[:,data.columns.str.contains(commissions + r"(\.\d)?$", regex=True)].fillna(0).sum(axis=1)*100
        data["id_string"] = data[[store_number, city, state]].astype(str).apply("_".join, axis=1)

        result = data.loc[:,["id_string", "inv_amt", "comm_amt"]].apply(self.upper_all_str)
        return PreProcessedData(result)


    def _johnstone_report_preprocessing(self, data: pd.DataFrame, **kwargs) -> PreProcessedData:
        monthly_sales: str = "cogs"
        commissions: str = "comm"
        store_number: str = "storeno"
        city: str = "storename"
        state: str = "state"

        data = data.replace('^\s+$',nan, regex=True) # make sure cells that have only 0-n space characters are also flagged as NaN
        data = data.dropna(subset=data.columns[0])
        data["inv_amt"] = data.loc[:,data.columns.str.endswith(monthly_sales)].fillna(0).sum(axis=1)*100
        data["comm_amt"] = data.loc[:,data.columns.str.endswith(commissions + r"(\.\d)?$", regex=True)].fillna(0).sum(axis=1)*100
        data["id_string"] = data[[store_number, city, state]].astype(str).apply("_".join, axis=1)

        result = data.loc[:,["id_string", "inv_amt", "comm_amt"]].apply(self.upper_all_str)
        return PreProcessedData(result)


    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": (self._standard_report_preprocessing,2),
            "baker": (self._baker_report_preprocessing,1),
            "johnstone": (self._johnstone_report_preprocessing,0),
        }
        preprocess_method, skip_param = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(skip=skip_param), **kwargs)
        else:
            return

