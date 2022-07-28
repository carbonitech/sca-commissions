import unittest
import dotenv
import os

import pandas as pd
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from db import models, db_services
from entities.manufacturers import adp
from entities.commission_file import CommissionFile
from entities.submission import NewSubmission
from app.report_processor import ReportProcessor

dotenv.load_dotenv()

# keys must match file names in ./tests/db_tables
DB_TABLES = {
        'cities': models.City,
        'states': models.State,
        'customers': models.Customer,
        'customer_branches': models.CustomerBranch,
        'manufacturers': models.ManufacturerDTO,
        'manufacturers_reports': models.ManufacturersReport,
        'representatives': models.Representative,
        'map_customer_name': models.MapCustomerName,
        'map_city_names': models.MapCityName,
        'map_state_names': models.MapStateName,
        'map_reps_customers': models.MapRepToCustomer,
        'report_submissions_log': models.SubmissionDTO,
        'report_processing_steps_log': models.ProcessingStepDTO,
        'current_errors': models.ErrorDTO,
        'final_commission_data': models.FinalCommissionDataDTO
}


class TestSubmissionDataManagement(unittest.TestCase):

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
        file = CommissionFile(adp_data, "Detail")
        self.submission = NewSubmission(
            reporting_month=5, reporting_year=2022,
            report_id=1, manufacturer_id=1,
            file=file
        )

        # process report using the Manufacturer's process
        self.adp_preprocessed_data = adp.ADPPreProcessor(self.submission).preprocess()

        self.report_processor = ReportProcessor(
            data=self.adp_preprocessed_data,
            submission=self.submission,
            database=db_services.DatabaseServices()
        )

        self.report_processor.process_and_commit()

        return


    def test_process_and_committ(self):
        """
        Tests:
            - Processing steps are an ascending sequence of integers starting at 1
            - Data staged for committing has the 4 columns expected, by name, and only those columns
        """
        report_processor = self.report_processor

        self.assertListEqual(
            [step.step_num for step in report_processor.process_steps],
            list(range(1,len(report_processor.process_steps)+1))
            )
        self.assertListEqual(
            report_processor.staged_data.columns.tolist(),
            ["submission_id","map_rep_customer_id","inv_amt","comm_amt"]
        )

        return

    def test_total_commissions(self):
        report_processor = self.report_processor
        total_commissions = report_processor.total_commissions()

        exp_total_commissions = self.submission.file_df().dropna(subset="Customer").loc[:,"Rep1 Commission"]*100
        exp_total_commissions = round(exp_total_commissions.sum())

        self.assertEqual(total_commissions, exp_total_commissions)
        return


    def test_total_sales(self):
        report_processor = ReportProcessor(
            data=self.adp_preprocessed_data,
            submission=self.submission,
            database=db_services.DatabaseServices()
        )

        report_processor.process_and_commit()
        total_sales = report_processor.total_sales()

        exp_total_sales = self.submission.file_df().dropna(subset="Customer").loc[:,"  Net Sales"]*100
        exp_total_sales = round(exp_total_sales.sum())

        self.assertEqual(total_sales, exp_total_sales)
        return


    def test_register_all_errors(self):
        database = db_services.DatabaseServices()
        errors = database.get_errors(self.report_processor.submission_id)
        self.assertEqual(len(self.report_processor.process_errors), len(errors))
        return


    def test_register_all_process_steps(self):
        database = db_services.DatabaseServices()
        process_steps = database.get_processing_steps(self.report_processor.submission_id)
        self.assertEqual(len(self.report_processor.process_steps), len(process_steps))

    def tearDown(self):
        models.Base.metadata.drop_all(self.db)
        return