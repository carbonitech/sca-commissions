"""
Manufacturer report processing definition
for Advanced Distributor Products (ADP)
"""
import pandas as pd
import numpy as np
from app.manufacturers.base import Manufacturer, Submission

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
        
        # convert dollars to cents to avoid demical precision weirdness
        data["  Net Sales"] = data.loc[:,"  Net Sales"].apply(lambda amt: amt*100)
        data["Rep1 Commission"] = data.loc[:,"Rep1 Commission"].apply(lambda amt: amt*100)

        # use pivot table to sum by account and reset index to get flat table again (repeat labels)
        piv_table_values = ["  Net Sales", "Rep1 Commission"]
        piv_table_index = ["Customer.1","ShipToCity","ShpToState","Customer","Ship To"]
        new_data_table = pd.pivot_table(
            data,
            values=piv_table_values,
            index=piv_table_index,
            aggfunc=np.sum).reset_index()

        # sold-to and ship-to were needed to generate the new_data_table, but not needed for the final report
        new_data_table = new_data_table.drop(columns=["Customer","Ship To"])

        # rename the columns for the final report
        new_data_table.rename(columns=["Customer Name", "City", "State", "Inv Amt", "Comm Amt"])

        # normalize names
        #TODO Normalize names here
        
        # map reps 
        #TODO Map Reps Here

        # add manufacturer, year, and month columns
        #TODO add manufacturer, year, and month columns here

        # update report submission aggregates
        self.report.total_comm += new_data_table["Comm Amt"].sum()

        # set cents back to dollars for the final report
        new_data_table["Inv Amt"] = new_data_table.loc[:,"Inv Amt"].apply(lambda amt: amt/100)
        new_data_table["Comm Amt"] = new_data_table.loc[:,"Comm Amt"].apply(lambda amt: amt/100)
        
        # set Submission attributes
        self.report.final_comm_data = new_data_table

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