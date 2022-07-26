import unittest
import dotenv
import os

import pandas as pd
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from db import models
from entities.manufacturers import adp

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
        self.submission = base.Submission(
            rep_mon=5, rep_year=2022,
            report_id=1, file=adp_data,
            sheet_name="Detail"
        )

        # process report using the Manufacturer's process
        adp.AdvancedDistributorProducts(self.submission).process_standard_report()
        
        return

    def test_record_submission(self): ...
    def test_get_submissions(self): ...
    def test_del_submission(self): ...
    def test_record_processing_steps(self): ...
    def test_get_processing_steps(self): ...
    def test_del_processing_steps(self): ...
    def test_record_errors(self): ...
    def test_get_errors(self): ...
    def test_del_error(self): ...
    def test_record_final_data(self): ...
    def test_get_final_data(self): ...
    def test_get_submission_files(self): ...
    def test_record_submission_file(self): ...
    def test_del_submission_file(self): ...

    def tearDown(self):
        models.Base.metadata.drop_all(self.db)
        return
