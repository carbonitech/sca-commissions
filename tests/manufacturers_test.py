import unittest
import dotenv
import os

import pandas as pd
from pandas.testing import assert_frame_equal
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import db_services, models
from app.entities import base
from app.entities.manufacturers import adp

dotenv.load_dotenv()

# keys must match file names in ./tests/db_tables
DB_TABLES = {
        'cities': models.City,
        'states': models.State,
        'customers': models.Customer,
        'customer_branches': models.CustomerBranch,
        'manufacturers': models.Manufacturer,
        'manufacturers_reports': models.ManufacturersReport,
        'representatives': models.Representative,
        'map_customer_name': models.MapCustomerName,
        'map_city_names': models.MapCityName,
        'map_state_names': models.MapStateName,
        'map_reps_customers': models.MapRepsToCustomer,
        'report_submissions_log': models.ReportSubmissionsLog,
        'report_processing_steps_log': models.ReportProcessingStepsLog,
        'current_errors': models.CurrentError,
        'final_commission_data': models.FinalCommissionData
}


class TestADP(unittest.TestCase):
    def setUp(self) -> None:
        """
        set up will be only for ADP data
        more manufacturers will be added here as more are implemented

        not all database tables are populated in set up
        only tables that are needed for data processing references
        """

        # set up database
        db_url = os.getenv("DATABASE_URL")
        self.db = create_engine(db_url)
        models.Base.metadata.create_all(self.db)
        self.db_serv = db_services.DatabaseServices(engine=self.db)

        # load csv files
        tables_dir = './tests/db_tables'
        files: list[str] = os.listdir(tables_dir)
        tables: dict[str,pd.DataFrame] = {
            file[:-4]: pd.read_csv(os.path.join(tables_dir,file))
            for file in files
        }

        # populate database with csv data
        with Session(self.db) as session:
            for table, data in tables.items():
                for row in data.to_dict("records"):
                    # col names in csv must match table schema
                    session.add(DB_TABLES[table](**row)) 
            session.commit()

        # get adp file data (as bytes)
        adp_file_loc: str = os.getenv("ADP_TEST_FILE")
        with open(adp_file_loc, 'rb') as file:
            adp_data: bytes = file.read()

        # set up Submission Object
        self.submission = base.Submission(
            rep_mon=5, rep_year=2022,
            report_id=1, file=adp_data,
            sheet_name="Detail"
        )

        # set up Manufacturer obj
        self.adp = adp.AdvancedDistributorProducts(self.submission)
        
        return

    def test_process_standard_report(self):
        """
        Tests: 
            - total commission amount reported in
                the final commission data and contained in the error set
                are equal in total to the sum of the "Rep1 Commission"
                field in the "Data" tab of the original file 
                (rounded nearest cent, represented as an integer)
            - submission id exists, isn't zero, and there is at least one record
                returned when submissions are retrieved from the database for ADP
        """
        self.adp.process_standard_report()

        # retrieve total from file by summing the commission column,
        # clean it, convert to cents, and sum for comparison
        expected_comm: pd.DataFrame = pd.read_excel(self.submission.file,sheet_name=self.submission.sheet_name)
        expected_comm.dropna(subset=expected_comm.columns.tolist()[0], inplace=True)
        expected_comm = expected_comm.loc[:,"Rep1 Commission"]
        expected_comm = round(expected_comm.apply(lambda amt: amt*100).sum())

        # calculate total commission from processing, in cents
        total_comm_amt_in_errors = 0
        comm_amts_in_errors = {}
        for error in self.submission.errors:
            error: base.Error
            row_index = error.row_index
            # since a row could be reported for an error more than once,
            # updating the same row_index won't add to the total
            # in the summation loop in the next step
            comm_amts_in_errors.update({row_index: error.row_data[row_index]["comm_amt"]})
        for amt in comm_amts_in_errors.values():
            total_comm_amt_in_errors += amt
        total_comm = self.submission.total_comm \
                    +round(total_comm_amt_in_errors)

        # retrieve submission metadata recorded in db
        submissions_for_adp = self.db_serv.get_submissions_metadata(self.adp.id)

        # check added columns for month, year & manufacturer
        all_sub_id_same = (self.submission.final_comm_data.submission_id == self.submission.id).all()
        
        expected_columns = ["submission_id","map_rep_customer_id","inv_amt","comm_amt"]

        sequence_processing_steps = [step_obj.step_num for step_obj in self.submission.processing_steps]

        ## tests
        # commission total compared to sum from original file
        self.assertEqual(total_comm, expected_comm)
        # existance of submission id, and it's not zero
        self.assertTrue(self.submission.id)
        self.assertGreater(self.submission.id,0)
        # existance of a submission for adp in report_submissions_log
        self.assertFalse(submissions_for_adp.empty)
        # check columns have expected values
        self.assertTrue(all_sub_id_same)
        self.assertListEqual(self.submission.final_comm_data.columns.tolist(),expected_columns)
        # check processing_steps
        self.assertListEqual(sequence_processing_steps, list(range(1,len(sequence_processing_steps)+1)))
        for step in self.submission.processing_steps:
            print(step)



    def tearDown(self) -> None:
        models.Base.metadata.drop_all(self.db)
        pass