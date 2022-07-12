"""
Manufacturer report processing definition
for Advanced Distributor Products (ADP)
"""
import pandas as pd
import numpy as np
from app.manufacturers.base import Manufacturer, Submission, Error

class AdvancedDistributorProducts(Manufacturer):
    """
    Remarks:
        - ADP's report comes as a single file with multiple tabs
        - All reports have the 'Detail' tab, which I'm calling the 'standard' report,
            but other tabs for POS reports vary in name, and sometimes in structure,
            albeit in predictable ways.
        - Reports are expected to come packaged together, so all report processing procedures
            should expect to be called, but fail gracefully or 'pass' when they aren't needed
    """

    name = "ADP" 

    reports_by_sheet = {
        'standard': {'sheet_name': 'Detail'},
        'RE Michel POS': {'sheet_name': 'RE Michel', 'skiprows': 2},
        'Coburn POS': {'sheet_name': 'Coburn'},
        'Lennox POS': [{'sheet_name': 'Marshalltown'},
            {'sheet_name': 'Houston'},
            {'sheet_name': 'Carrollton'}]
    }

    def __init__(self, submission: Submission):
        super().__init__()
        self.submission = submission
        

    def _process_standard_report(self):
        """processes the 'Detail' tab of the ADP commission report
            returns sales & commission data in the desired format"""

        data: pd.DataFrame = pd.read_excel(self.submission.file, **self.reports_by_sheet['standard'])

        # take spaces out of column names
        data.columns = [col.replace(" ","") for col in data.columns.tolist()]
        
        # convert dollars to cents to avoid demical precision weirdness
        data.NetSales = data.loc[:,"NetSales"].apply(lambda amt: amt*100)
        data.Rep1Commission = data.loc[:,"Rep1Commission"].apply(lambda amt: amt*100)

        # use pivot table to sum by account and reset index to get flat table again (repeat labels)
        piv_table_values = ["NetSales", "Rep1Commission"]
        piv_table_index = ["Customer.1","ShipToCity","ShpToState","Customer","ShipTo"]
        result = pd.pivot_table(
            data,
            values=piv_table_values,
            index=piv_table_index,
            aggfunc=np.sum).reset_index()

        # sold-to and ship-to were needed to generate the new_data_table, but not needed for the final report
        result = result.drop(columns=["Customer","ShipTo"])

        # rename the columns for the final report
        result.rename(columns=["customer_name", "city", "state", "inv_amt", "comm_amt"])

        # normalize names
        customer_name_map = self.mappings["map_customer_name"]
        merged_with_name_map = pd.merge(result, customer_name_map,
                how="left", left_on="customer_name", right_on="recorded_name")
        
        match_is_null = merged_with_name_map["recorded_name"].isnull()
        matches_table = merged_with_name_map[~match_is_null]
        no_match_table = merged_with_name_map[match_is_null]

        result.customer_name = matches_table.loc[:,"standard_name"]

        for row_index, row_data in no_match_table.to_dict("index").items():
            error_obj = Error(
                submission_id=self.submission.id,
                row_index=row_index,
                field="Customer Name",
                value_type=str,
                value_content=row_data["customer_name"],
                reason="Customer name in the commission file is not mapped to a standard name",
                row_data={row_index: row_data})
            self.submission.errors.append(error_obj)
        
        # map reps
        merged_w_ref_table = pd.merge(
                result, self.customer_rep_reference, how="left",
                left_on=["customer_name", "city", "state"],
                right_on=["customers.name", "customer_branches.city", "customer_branches.state"]
        )
        match_is_null = merged_w_ref_table["customers.name"].isnull()
        matches_table = merged_w_ref_table[~match_is_null]
        no_match_table = merged_w_ref_table[match_is_null]

        result = matches_table.loc[:,["representatives.initials"]+result.columns.tolist()]
        result = result.rename(columns={"representatives.initials": "salesman"})
        # -- RECORD ERRORS HERE FOR NO MATCH TO CUSTOMERS TABLE --


        # add manufacturer, year, and month columns
        #TODO add manufacturer, year, and month columns here

        # update report submission aggregates
        self.submission.total_comm += result["Comm Amt"].sum()

        # set cents back to dollars for the final report
        result["Inv Amt"] = result.loc[:,"Inv Amt"].apply(lambda amt: amt/100)
        result["Comm Amt"] = result.loc[:,"Comm Amt"].apply(lambda amt: amt/100)

        # set Submission attributes
        self.submission.final_comm_data = result

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