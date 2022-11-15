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

        summary_file: pd.DataFrame = pd.read_excel(kwargs.get("additional_file_1")).dropna()
        invoice_number_col: str = "Invoice Number"
        comm_col: int = -1

        # inoice numbers are in their own column and share the same index as customer info and sales figure
        value_indecies = data.iloc[:,invoice_num_col].dropna().index.to_list()[1:] # 0th value is the column header
        result = data.loc[value_indecies]
        result.iloc[:,invoice_num_col] = result.iloc[:,invoice_num_col].astype(int)
        events.append(("Formatting","filtered for rows with an invoice number in the second column and converted from text to numbers",self.submission_id))
        # address is combined and needs to be split into city and state
        result[[city_name_col,state_name_col]] = result.iloc[:,address_col].str.split(", ", expand=True)
        result.loc[:, state_name_col] = result[state_name_col].str.slice(0,2)
        events.append(("Formatting",f"split the address in column {address_col+1} into city and state columns",self.submission_id))
        # make a new table with just invoice number, customer, sales, city, and state
        result = pd.concat([result.iloc[:,[1,customer_name_col,inv_col]], result.loc[:,[city_name_col,state_name_col]]], axis=1)
        # reorder columns so invoice amount is at the end
        result = result.iloc[:,[0,1,3,4,2]]
        result.columns = ["invoice", "customer", "city", "state", "inv_amt"]
        result.loc[:, "inv_amt"] = result["inv_amt"]*100
        result = result.groupby(result.columns.tolist()[:4]).sum().reset_index()
        events.append(("Formatting",f"grouped data by invoice number, customer, city, and state. Index changed.",self.submission_id))
        
        # merge with the summary_file to fill total commission values by invoice number!
        summary_file = pd.concat([summary_file.loc[:,invoice_number_col], summary_file.iloc[:,comm_col]], axis=1)
        summary_file.columns = ["invoice_summary","total_comm"]
        summary_file["invoice_summary"] = summary_file["invoice_summary"].astype(int)
        merged = result.merge(summary_file, how="left", left_on="invoice", right_on="invoice_summary")
        result.loc[:,"comm_amt"] = merged["total_comm"]*100
        events.append(("Formatting",f"Merged Commission Amounts by invoice number using the reference file",self.submission_id))
        # drop invoice number column
        result = result.drop(columns="invoice")
        # standardize
        result.columns = self.result_columns
        for col in range(3):
            result.iloc[:, col] = result.iloc[:, col].str.upper()
            result.iloc[:, col] = result.iloc[:, col].str.strip()

        # without invoice column now, group by branch
        result = result.groupby(result.columns.tolist()[:3]).sum().reset_index()
        events.append(("Formatting",f"Dropped the Invoice number column and grouped sales again by Customer, City, and State. Index changed.",self.submission_id))

        return PreProcessedData(result, events)


    def preprocess(self, **kwargs) -> PreProcessedData:
        method_by_name = {
            "standard": self._standard_report_preprocessing,
        }
        preprocess_method = method_by_name.get(self.report_name, None)
        if preprocess_method:
            return preprocess_method(self.file.to_df(), **kwargs)
        else:
            return

