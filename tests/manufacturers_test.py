import unittest
import dotenv
import os

import pandas as pd
from pandas.testing import assert_frame_equal
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import db_services, models
from app.manufacturers import base, adp

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

        # set adp file data (as bytes)
        adp_file_loc: str = os.getenv("ADP_TEST_FILE")
        with open(adp_file_loc, 'rb') as file:
            self.adp_data: bytes = file.read()

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
                    session.add(DB_TABLES[table](**row)) 
                    # col names in csv must match table schema
            session.commit()


    def tearDown(self) -> None:
        models.Base.metadata.drop_all(self.db)
        pass