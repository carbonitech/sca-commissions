"""
Manufacturer report processing definition
for Advanced Distributor Products (ADP)
"""
import pandas as pd
import numpy as np
from app.manufacturers.base import Manufacturer, Submission, Error

class AdvancedDistributorProducts(Manufacturer):

    name = "ADP" 

    reports_by_sheet = {
        'standard': {'sheet_name': 'Detail'},
        'RE Michel POS': {'sheet_name': 'RE Michel', 'skiprows': 2},
        'Coburn POS': {'sheet_name': 'Coburn'},
        'Lennox POS': [{'sheet_name': 'Marshalltown'},
            {'sheet_name': 'Houston'},
            {'sheet_name': 'Carrollton'}]
    }

    def __init__(self, report: Submission):
        super().__init__()
        self.report = report

    ## these report processing procedures should all run together in a 'default' run
    ## but able to be run independently, not failing on 'missing' sheets

    def _process_standard_report(self):
        """processes the 'Detail' tab of the ADP commission report
            returns sales & commission data in the desired format"""

        data: pd.DataFrame = pd.read_excel(self.report.file, **self.reports_by_sheet['standard'])

        # take spaces out of column names
        data.columns = [col.replace(" ","") for col in data.columns.tolist()]
        
        # convert dollars to cents to avoid demical precision weirdness
        data.NetSales = data.loc[:,"NetSales"].apply(lambda amt: amt*100)
        data.Rep1Commission = data.loc[:,"Rep1Commission"].apply(lambda amt: amt*100)

        # use pivot table to sum by account and reset index to get flat table again (repeat labels)
        piv_table_values = ["NetSales", "Rep1Commission"]
        piv_table_index = ["Customer.1","ShipToCity","ShpToState","Customer","ShipTo"]
        sums_grouped_by_acct = pd.pivot_table(
            data,
            values=piv_table_values,
            index=piv_table_index,
            aggfunc=np.sum).reset_index()

        # sold-to and ship-to were needed to generate the new_data_table, but not needed for the final report
        sums_grouped_by_acct = sums_grouped_by_acct.drop(columns=["Customer","ShipTo"])

        # rename the columns for the final report
        sums_grouped_by_acct.rename(columns=["customer_name", "city", "state", "inv_amt", "comm_amt"])

        # normalize names
        #TODO Normalize names here (make sure to store 'errors' such as unmapped values)
        customer_name_map = self.mappings["map_customer_name"]
        merged_with_name_map = pd.merge(sums_grouped_by_acct, customer_name_map,
                how="left", left_on="customer_name", right_on="recorded_name")
        
        match_is_null = merged_with_name_map["recorded_name"].isnull()
        matches_table = merged_with_name_map[~match_is_null]
        no_match_table = merged_with_name_map[match_is_null]

        sums_grouped_by_acct.customer_name = matches_table.loc[:,"standard_name"]

        for row_index, row_data in no_match_table.to_dict("index").items():
            error_obj = Error(
                submission_id=self.report.id,
                row_index=row_index,
                field="Customer Name",
                value_type=str,
                value_content=row_data["customer_name"],
                reason="Customer name in the commission file is not mapped to a standard name",
                row_data={row_index: row_data})
            self.report.errors.append(error_obj)
        
        # map reps 
        #TODO Map Reps Here (make sure to store 'errors' such as unmapped values)

        # add manufacturer, year, and month columns
        #TODO add manufacturer, year, and month columns here

        # update report submission aggregates
        self.report.total_comm += sums_grouped_by_acct["Comm Amt"].sum()

        # set cents back to dollars for the final report
        sums_grouped_by_acct["Inv Amt"] = sums_grouped_by_acct.loc[:,"Inv Amt"].apply(lambda amt: amt/100)
        sums_grouped_by_acct["Comm Amt"] = sums_grouped_by_acct.loc[:,"Comm Amt"].apply(lambda amt: amt/100)

        # set Submission attributes
        self.report.final_comm_data = sums_grouped_by_acct

        return


    def _process_coburn_report(self):
        pass

    def _process_re_michel_report(self):
        pass

    def _process_lennox_report(self):
        pass

    def process_reports(self):
        """runs all reports, ignoring errors from 'missing' reports
        and recording errors for unexpected sheets
        returns final commission data"""