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

        invoice_number_col: str = "Invoice Number"
        comm_col: int = -1

        sales_report: pd.DataFrame = pd.read_excel(kwargs.get("additional_file_1"))
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
        data = pd.concat([data.loc[:,invoice_number_col], data.iloc[:,comm_col]], axis=1).dropna()
        data.columns = ["invoice_summary","total_comm"]
        data["invoice_summary"] = data["invoice_summary"].astype(int)
        data["total_comm"] = data["total_comm"].astype(float)

        # in the sales report, inoice numbers are in their own column and share the same index as customer info and sales figure
        invoice_numbers = sales_report.iloc[:,invoice_num_col].dropna()
        if "Invoice Number" in invoice_numbers.values:
            invoice_numbers = invoice_numbers.iloc[1:]
        value_indecies = invoice_numbers.index.to_list()
        sales_report = sales_report.loc[value_indecies]
        sales_report[sales_report.columns[invoice_num_col]] = sales_report.iloc[:,invoice_num_col].astype(int)
        events.append(("Formatting","filtered for rows with an invoice number in the second column and converted from text to numbers",self.submission_id))
        # address is combined and needs to be split into city and state
        sales_report[[city_name_col,state_name_col]] = sales_report.iloc[:,address_col].str.split(", ", expand=True)
        sales_report.loc[:, state_name_col] = sales_report[state_name_col].str.slice(0,2)
        events.append(("Formatting",f"split the address in column {address_col+1} into city and state columns",self.submission_id))
        # make a new table with just invoice number, customer, sales, city, and state
        sales_report = pd.concat([sales_report.iloc[:,[1,customer_name_col,inv_col]], sales_report.loc[:,[city_name_col,state_name_col]]], axis=1)
        # reorder columns so invoice amount is at the end
        sales_report = sales_report.iloc[:,[0,1,3,4,2]]
        sales_report.columns = ["invoice", "customer", "city", "state", "inv_amt"]
        sales_report.loc[:, "inv_amt"] = sales_report["inv_amt"]*100
        result = sales_report.groupby(sales_report.columns.tolist()[:4]).sum().reset_index()
        events.append(("Formatting",f"grouped data by invoice number, customer, city, and state. Index changed.",self.submission_id))
        
        # merge the sales report data with the commission report data by invoice number
        merged = result.merge(data, how="left", left_on="invoice", right_on="invoice_summary")
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

