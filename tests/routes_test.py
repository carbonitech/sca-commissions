import unittest
import dotenv
import os
from typing import Dict

import pandas as pd
from pandas.util.testing import assert_frame_equal
from random import randint

from app.db import db
from app.routes import api_services

from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import Session

dotenv.load_dotenv()

class TestApiServiceFunctions(unittest.TestCase):

    def setUp(self):
        db_url = os.getenv("TESTING_DATABASE_URL")
        entries_by_table: Dict[str,dict] = {
            "map_customer_name": {
                "recorded_name": ["MINGLEDROFFS", "EDSSUPPLYCO", "DSC"],
                "standard_name": ["MINGLEDORFFS", "EDS SUPPLY COMPANY","DEALERS SUPPLY COMPANY"]
            },
            "map_rep": {
                "rep_id": [6,6,2],
                "customer_branch_id": [350,19,31],
            },
            "map_city_names": {
                "recorded_name": ["CHATNOGA", "PT_ST_LUCIE", "BLUERIDGE"],
                "standard_name": ["CHATTANOOGA", "PORT SAINT LUCIE", "BLUE RIDGE"]
            },
            "final_commission_data": {
                "year": [2019,2020,2021,2022],
                "month": ["January", "November", "April", "July"],
                "manufacturer": ["ADP", "Berry", "Allied", "Atco"],
                "salesman": ["mwr","sca", "jdc", "red"],
                "customer_name": ["Coastal Supply","Baker Distributing","Dealers Supply","Hinkle Metals"],
                "city": ["Knoxville","Jacksonville","Forest Park","Birmingham"],
                "state": ["TN","FL","GA","AL"],
                "inv_amt": [randint(100,50000000)/100 for _ in range(0,4)]
            }
        }
        entries_by_table["final_commission_data"]["comm_amt"] = [round(num*3/100, 2) for num in entries_by_table["final_commission_data"]["inv_amt"]]
        
        self.db = create_engine(db_url)
        db.Base.metadata.create_all(self.db)
        with Session(self.db) as session:
            session.add(db.MapCustomerName(recorded_name='MINGLEDROFFS', standard_name='MINGLEDORFFS'))
            session.commit()
        return


    def test_get_mapping_tables(self):
        result = api_services.get_mapping_tables(self.db)
        expected = {"map_customer_name","map_city_names","map_reps_customers"}
        self.assertEqual(result,expected)

    def test_get_mappings(self):
        result = api_services.get_mappings(
            conn=self.db,
            table=db.MapCustomerName
        )
        expected = pd.DataFrame({"id":[1],"recorded_name":['MINGLEDROFFS'],"standard_name":['MINGLEDORFFS']})
        assert_frame_equal(result,expected)


    def test_set_mapping(self): self.assertTrue(False)
    def test_del_mapping(self): self.assertTrue(False)
    def test_record_final_data(self): self.assertTrue(False)
    def test_get_final_data(self): self.assertTrue(False)
    def test_get_submissions_metadata(self): self.assertTrue(False)
    def test_del_submission(self): self.assertTrue(False)
    def test_get_submission_files(self): self.assertTrue(False)
    def test_record_submission_file(self): self.assertTrue(False)
    def test_del_submission_file(self): self.assertTrue(False)
    def test_get_processing_steps(self): self.assertTrue(False)
    def test_record_processing_steps(self): self.assertTrue(False)
    def test_del_processing_steps(self): self.assertTrue(False)
    def test_get_errors(self): self.assertTrue(False)
    def test_record_errors(self): self.assertTrue(False)
    def test_correct_error(self): self.assertTrue(False)
    def test_del_error(self): self.assertTrue(False)


    def tearDown(self):
        db.Base.metadata.drop_all(self.db)
        return