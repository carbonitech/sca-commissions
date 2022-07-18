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

        # sold-to and ship-to were needed to generate the result, but not needed for the final report
        result = result.drop(columns=["Customer","ShipTo"])

        result.columns=["customer", "city", "state", "inv_amt", "comm_amt"]
        result = self.fill_customer_ids(result)
        result = self.fill_city_ids(result)
        result = self.fill_state_ids(result)
        # use only customers with all ids for the next step
        mask = result.all('columns')
        result = self.add_customer_branch_ids(result[mask])
        # add manufacturer, year, and month columns
        #TODO add manufacturer, year, and month columns here

        # update report submission aggregates
        self.submission.total_comm += result["comm_amt"].sum()

        # set cents back to dollars for the final report
        # result["inv_amt"] = result.loc[:,"inv_amt"].apply(lambda amt: amt/100)
        # result["comm_amt"] = result.loc[:,"comm_amt"].apply(lambda amt: amt/100)

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

    def fill_customer_ids(self, data):
        customer_name_map = self.mappings["map_customer_name"]
        merged_with_name_map = pd.merge(data, customer_name_map,
                how="left", left_on="customer", right_on="recorded_name")
        
        match_is_null = merged_with_name_map["recorded_name"].isnull()
        no_match_table = merged_with_name_map[match_is_null]

        # customer column is going from a name string to an id integer
        data.customer = merged_with_name_map.loc[:,"customer_id"].fillna(0).astype(int)

        # record unmapped values as errors
        for row_index, row_data in no_match_table.to_dict("index").items():
            error_obj = Error(
                submission_id=self.submission.id,
                row_index=row_index,
                field="customer",
                value_type=str,
                value_content=row_data["customer"],
                reason="Customer name in the commission file is not mapped to a standard name",
                row_data={row_index: row_data})
            self.submission.errors.append(error_obj)
        return data

    def fill_city_ids(self,data):
        merged_w_cities_map = pd.merge(
            data, self.mappings["map_city_names"],
            how="left", left_on="city", right_on="recorded_name"
        )

        match_is_null = merged_w_cities_map["recorded_name"].isnull()
        no_match_table = merged_w_cities_map[match_is_null]

        # city column is going from a name string to an id integer
        data.city = merged_w_cities_map.loc[:,"city_id"].fillna(0).astype(int)

        # record unmapped values as errors
        for row_index, row_data in no_match_table.to_dict("index").items():
            error_obj = Error(
                submission_id=self.submission.id,
                row_index=row_index,
                field="city",
                value_type=str,
                value_content=row_data["city"],
                reason="City name in the commission file is not mapped to a standard name",
                row_data={row_index: row_data})
            self.submission.errors.append(error_obj)
        return data

    def fill_state_ids(self, data):
        merged_w_states_map = pd.merge(
            data, self.mappings["map_state_names"],
            how="left", left_on="state", right_on="recorded_name"
        )

        match_is_null = merged_w_states_map["recorded_name"].isnull()
        no_match_table = merged_w_states_map[match_is_null]

        # state column is going from a name string to an id integer
        data.state = merged_w_states_map.loc[:,"state_id"].fillna(0).astype(int)

        # record unmapped values as errors
        for row_index, row_data in no_match_table.to_dict("index").items():
            error_obj = Error(
                submission_id=self.submission.id,
                row_index=row_index,
                field="state",
                value_type=str,
                value_content=row_data["state"],
                reason="State name in the commission file is not mapped to a standard name",
                row_data={row_index: row_data})
            self.submission.errors.append(error_obj)
        return data

    def add_customer_branch_ids(self, data):
        merged_w_customer_branches = pd.merge(
            data, self.customer_branches,
            how="left", left_on=["customer", "city", "state"], 
            right_on=["customer_id","city_id","state_id"]
        )

        match_is_null = merged_w_customer_branches["customer_id"].isnull()
        no_match_table = merged_w_customer_branches[match_is_null]

        data["customer_branch_id"] = merged_w_customer_branches.loc[:,"id"].fillna(0).astype(int)

        # record unmapped values as errors
        for row_index, row_data in no_match_table.to_dict("index").items():
            error_obj = Error(
                submission_id=self.submission.id,
                row_index=row_index,
                field="customer_branch_id",
                value_type=int,
                value_content=0,
                reason="",
                row_data={row_index: row_data})
            self.submission.errors.append(error_obj)
        return data
