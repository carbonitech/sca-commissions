"""
Manufacturer report processing definition
for Advanced Distributor Products (ADP)
"""
import pandas as pd
import numpy as np
from app.entities.base import Manufacturer

class AdvancedDistributorProducts(Manufacturer):
    """
    Remarks:
        - ADP's report comes as a single file with multiple tabs
        - All reports have the 'Detail' tab, which I'm calling the 'standard' report,
            but other tabs for POS reports vary in name, and sometimes in structure.
        - Reports are expected to come packaged together, seperated in one file by tabs
    Effects:
        - Updates Submission object:
            - total_comm: adds commission sum to the running total
            - final_comm_data: concatenate the result from this process
                with other results
            - errors: appends Error objects
    Returns: None

    TODO - add internal checks on commission dollar amounts to ensure integrity
    """

    name = "ADP"

    def process_standard_report(self):
        """processes the 'Detail' tab of the ADP commission report"""

        data: pd.DataFrame = pd.read_excel(self.submission.file, sheet_name=self.submission.sheet_name)

        data.columns = [col.replace(" ","") for col in data.columns.tolist()]
        self.record_processing_step("removed spaces from column names")

        data.dropna(subset=data.columns.tolist()[0], inplace=True)
        self.record_processing_step("removed all rows that have no value in the first column")

        # convert dollars to cents to avoid demical precision weirdness
        data.NetSales = data.loc[:,"NetSales"].apply(lambda amt: amt*100)
        data.Rep1Commission = data.loc[:,"Rep1Commission"].apply(lambda amt: amt*100)

        # sum by account convert to a flat table
        piv_table_values = ["NetSales", "Rep1Commission"]
        piv_table_index = ["Customer.1","ShipToCity","ShpToState","Customer","ShipTo"]
        result = pd.pivot_table(
            data,
            values=piv_table_values,
            index=piv_table_index,
            aggfunc=np.sum).reset_index()
        self.record_processing_step("grouped NetSales and Rep1Commission by sold-to, "
                "ship-to, customer name, city, and state (pivot table)")

        result = result.drop(columns=["Customer","ShipTo"])
        self.record_processing_step("dropped the ship-to and sold-to id columns")

        result.columns=["customer", "city", "state", "inv_amt", "comm_amt"]

        result = self.fill_customer_ids(result, column="customer")
        self.record_processing_step("replaced customer names with customer ids where "
                "a customer reference name was found in the mapping in the database")
        if self.submission.errors:
            self.record_processing_step("failures in customer name mapping logged")
            num_errors = len(self.submission.errors)
        
        result = self.fill_city_ids(result, column="city")
        self.record_processing_step("replaced city names with city ids where "
                "a city reference name was found in the mapping in the database")
        if len(self.submission.errors) > num_errors:
            self.record_processing_step("failures in city name mapping logged")
            num_errors = len(self.submission.errors)
        
        result = self.fill_state_ids(result, column="state")
        self.record_processing_step("replaced state names with state ids where "
                "a state reference name was found in the mapping in the database")
        if len(self.submission.errors) > num_errors:
            self.record_processing_step("failures in state name mapping logged")
            num_errors = len(self.submission.errors)

        mask = result.all('columns')
        map_rep_col_name = "map_rep_customer_id"
        result = self.add_rep_customer_ids(result[mask], ref_columns=["customer", "city", "state"],
            new_column=map_rep_col_name)  # pared down to only customers with all values != 0
        self.record_processing_step("removed all rows that failed to map either a customer id, city id, or state id")
        self.record_processing_step("added the rep-to-customer mapping id by looking up customer, "
                "city, and state ids in a reference table")
        if len(self.submission.errors) > num_errors:
            self.record_processing_step("failurs in rep-to-customer mapping logged")

        mask = result.all('columns')
        result = result[mask]  # filter again for 0's. 0's have been recorded in the errors list
        self.record_processing_step("removed all rows that failed to map rep-to-customer id")

        submission_id_col_name = "submission_id"
        result[submission_id_col_name] = self.submission.id
        result = result.loc[:,[submission_id_col_name,map_rep_col_name,"inv_amt","comm_amt"]]

        # update submission attrs
        self.submission.total_comm += result["comm_amt"].sum()
        self.record_processing_step(f"total commissions successfully processed: ${self.submission.total_comm/100:.2f}")
        self.submission.final_comm_data = pd.concat(
            [self.submission.final_comm_data, result]
        )

        return


    def process_coburn_report(self):
        """process the 'Coburn' tab(s) of the ADP commission report"""

    def process_re_michel_report(self):
        pass

    def process_lennox_report(self):
        pass

