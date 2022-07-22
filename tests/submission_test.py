import unittest
import dotenv
import os

import pandas as pd
from pandas.testing import assert_frame_equal
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import models
from app.db import db_services

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


class TestSubmissionDataManagement(unittest.TestCase):

    def setUp(self):
        
        # set up database
        db_url = os.getenv("DATABASE_URL")
        self.db = create_engine(db_url)
        models.Base.metadata.create_all(self.db)
        self.db_services = db_services.DatabaseServices(engine=self.db)

        # load csv files for db reference tables
        tables_dir = './tests/db_tables'
        files: list[str] = os.listdir(tables_dir)
        self.tables: dict[str,pd.DataFrame] = {
            file[:-4]: pd.read_csv(os.path.join(tables_dir,file))
            for file in files
        }

        # populate database with csv data
        with Session(self.db) as session:
            for table, data in self.tables.items():
                for row in data.to_dict("records"):
                    # col names in csv must match table schema
                    session.add(DB_TABLES[table](**row)) 
            session.commit()

        # insert expected postgreSQL table id values into each pandas DataFrame
        add_one = lambda val: val+1
        for dataset in self.tables.values():
            dataset.reset_index(inplace=True)
            dataset.rename(columns={"index":"id"}, inplace=True)
            dataset.id = dataset.id.apply(add_one)
            
        return

    def test_get_submissions(self): ...
    def test_record_submission(self): ...
    def test_del_submission(self): ...
    def test_get_processing_steps(self): ...
    def test_record_processing_steps(self): ...
    def test_del_processing_steps(self): ...
    def test_get_errors(self): ...
    def test_record_errors(self): ...
    def test_del_error(self): ...
    def test_record_final_data(self): ...
    def test_get_final_data(self): ...
    def test_get_submission_files(self): ...
    def test_record_submission_file(self): ...
    def test_del_submission_file(self): ...

    def tearDown(self):
        models.Base.metadata.drop_all(self.db)
        return
