"""
Manufacturer report preprocessing definition
for C&D Value
"""
import pandas as pd
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
        events = []
        customer_name_col: int = 4
        city_name_col: str = "city"
        state_name_col: str = "state"
        inv_col: int = 15
        invoice_num_col: int = 1
        address_col: int = 9

        summary_file: pd.DataFrame = pd.read_excel(kwargs.get("additional_file_1"))
        invoice_number_col: str = "Invoice Number"
        comm_col: int = -1

        # inoice numbers are in their own column and share the same index as customer info and sales figure
        value_indecies = data.iloc[:,invoice_num_col].dropna().index.to_list()[1:] # 0th value is the column header
        result = data.loc[value_indecies]
        # address is combined and needs to be split into city and state
        result[[city_name_col,state_name_col]] = result.iloc[:,address_col].str.split(", ", expand=True)
        result.loc[:, state_name_col] = result[state_name_col].str.slice(0,2)
        # make a new table with just invoice number, customer, sales, city, and state
        result = pd.concat([result.iloc[:,[1,customer_name_col,inv_col]], result.loc[:,[city_name_col,state_name_col]]], axis=1)
        # reorder columns so invoice amount is at the end
        result = result.iloc[:,[0,1,3,4,2]]
        result.columns = ["invoice", "customer", "city", "state", "inv_amt"]
        result = result.groupby(result.columns.tolist()[:4]).sum().reset_index()
        # merge with the summary_file to fill total commission values by invoice number!
        summary_file = pd.concat([summary_file.loc[:,invoice_number_col], summary_file.iloc[:,comm_col]], axis=1)
        summary_file.columns = ["invoice_summary","total_comm"]
        merged = result.merge(summary_file, how="left", left_on="invoice", right_on="invoice_summary")
        result.loc[:,"comm_amt"] = merged["total_comm"]
        # drop invoice number column
        result = result.drop(columns="invoice")
        # standardize
        result.columns = self.result_columns
        for col in range(3):
            result.iloc[:, col] = result.iloc[:, col].str.upper()
            result.iloc[:, col] = result.iloc[:, col].str.strip()

        return PreProcessedData(result, events)

