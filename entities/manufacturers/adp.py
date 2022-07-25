"""
Manufacturer report processing definition
for Advanced Distributor Products (ADP)
"""
from typing import List
import pandas as pd
import numpy as np
from entities.commission_data import PreProcessedData
from entities.processing_step import ProcessingStep
from entities.preprocessor import PreProcessor
from entities.submission import NewSubmission

class ADPPreProcessor(PreProcessor):
    """
    Remarks:
        - ADP's report comes as a single file with multiple tabs
        - All reports have the 'Detail' tab, which I'm calling the 'standard' report,
            but other tabs for POS reports vary in name, and sometimes in structure.
        - Reports are expected to come packaged together, seperated in one file by tabs
        
    Returns: PreProcessedData object with data and attributes set to enable further processing
    """

    name = "ADP"

    def _standard_report_preprocessing(self, submission: NewSubmission) -> PreProcessedData:
        """processes the 'Detail' tab of the ADP commission report"""

        process_steps: List[ProcessingStep] = []

        data = submission.file_df()

        data.columns = [col.replace(" ","") for col in data.columns.tolist()]
        process_steps.append(self.processing_step_factory("removed spaces from column names"))

        data.dropna(subset=data.columns.tolist()[0], inplace=True)
        process_steps.append(self.processing_step_factory("removed all rows that have no value in the first column"))

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
        process_steps.append(self.processing_step_factory("grouped NetSales and Rep1Commission by sold-to, "
                "ship-to, customer name, city, and state (pivot table)"))

        result = result.drop(columns=["Customer","ShipTo"])
        process_steps.append(self.processing_step_factory("dropped the ship-to and sold-to id columns"))

        customer_name_col = 'customer'
        city_name_col = 'city'
        state_name_col = 'state'
        ref_cols = result.columns.tolist()[:3]
        result.columns=[customer_name_col,city_name_col,state_name_col,"inv_amt","comm_amt"]

        return PreProcessedData(result,process_steps,ref_cols,customer_name_col,city_name_col,state_name_col)


    def _coburn_report_preprocessing(self, submission: NewSubmission) -> PreProcessedData:
        pass

    def _re_michel_report_preprocessing(self, submission: NewSubmission) -> PreProcessedData:
        pass

    def _lennox_report_preprocessing(self, submission: NewSubmission) -> PreProcessedData:
        pass


    @staticmethod
    def processing_step_factory(step_description: str) -> ProcessingStep:
        return ProcessingStep(description=step_description)


    def preprocess(self, submission: NewSubmission) -> PreProcessedData:
        method_by_id = {
            1: self._standard_report_preprocessing,
            2: self._coburn_report_preprocessing,
            3: self._lennox_report_preprocessing,
            4: self._re_michel_report_preprocessing
        }
        preprocess_method = method_by_id.get(submission.report_id, None)
        if preprocess_method:
            return preprocess_method(submission)
        else:
            return