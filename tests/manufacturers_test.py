import unittest
import os

import pandas as pd

import entities.manufacturers as manufacturers
from entities.submission import NewSubmission
from entities.commission_file import CommissionFile
from entities.commission_data import PreProcessedData


class TestADP(unittest.TestCase):
    def setUp(self) -> None:

        # get adp file data (as bytes)
        adp_file_loc: str = os.getenv("ADP_TEST_FILE")
        with open(adp_file_loc, 'rb') as file:
            self.adp_data: bytes = file.read()
        
        return

    def test_process_standard_report(self):
        """
        Tests:
            - Manufacturer.preprocess() returns a PreProcessedData object
            - columns are what is expected
            - sum of the commission column is equal to original sum
            - sum of the sales column is equal to original sum
            - the seq of step numbers in the list of ProcessingSteps
        """

        file = CommissionFile(file_data=self.adp_data, sheet_name="Detail")
        submission = NewSubmission(
            file=file,
            report_month=5, report_year=2022,
            report_id=1, manufacturer_id=1
        )

        pp_data = manufacturers.adp.ADPPreProcessor().preprocess(submission=submission)


        # retrieve total from file by summing the commission column,
        # clean it, convert to cents, and sum for comparison
        expected_df: pd.DataFrame = submission.file_df()
        expected_df.dropna(subset=expected_df.columns.tolist()[0], inplace=True)
        expected_comm = expected_df.loc[:,"Rep1 Commission"]
        expected_comm = expected_comm.apply(lambda amt: amt*100).sum()
        expected_sales = expected_df.loc[:,"  Net Sales"]
        expected_sales = expected_sales.apply(lambda amt: amt*100).sum()

        sequence_processing_steps = [step_obj.step_num for step_obj in pp_data.process_steps]
        expected_data_cols = ["customer","city","state","inv_amt","comm_amt"]

        self.assertIsInstance(pp_data, PreProcessedData)
        self.assertListEqual(pp_data.data.columns.tolist(), expected_data_cols)
        self.assertEqual(pp_data.total_commission(), expected_comm)
        self.assertEqual(pp_data.total_sales(), expected_sales)
        self.assertListEqual(sequence_processing_steps, list(range(1,len(sequence_processing_steps)+1)))