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

        entries_by_table: Dict[str,dict] = {
            "map_customer_name": {
                "recorded_name": ["MINGLEDROFFS", "EDSSUPPLYCO", "DSC"],
                "standard_name": ["MINGLEDORFFS", "EDS SUPPLY COMPANY","DEALERS SUPPLY COMPANY"]
            },
            "map_reps_customers": {
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
                "state": ["TN","FL","GA","AL"]
            }
        }

        inv_amts = [randint(100,50000000)/100 for _ in range(0,4)]
        entries_by_table["final_commission_data"]["inv_amt"] = inv_amts
        comm_amts = [round(num*3/100, 2) for num in inv_amts]
        entries_by_table["final_commission_data"]["comm_amt"] = comm_amts

        self.entries_dfs = {tbl_name:pd.DataFrame(data) for tbl_name,data in entries_by_table.items()}
        for tbl,df in self.entries_dfs.items():
             self.entries_dfs[tbl].insert(0,"id",list(range(1,len(df)+1))) 

        db_url = os.getenv("TESTING_DATABASE_URL")
        self.db = create_engine(db_url)
        db.Base.metadata.create_all(self.db)
        with Session(self.db) as session:
            session.add(db.MapCustomerName(recorded_name='MINGLEDROFFS', standard_name='MINGLEDORFFS'))
            session.add(db.MapCustomerName(recorded_name='EDSSUPPLYCO', standard_name='EDS SUPPLY COMPANY'))
            session.add(db.MapCustomerName(recorded_name='DSC', standard_name='DEALERS SUPPLY COMPANY'))
            session.add(db.MapRepsToCustomer(rep_id=6, customer_branch_id=350))
            session.add(db.MapRepsToCustomer(rep_id=6, customer_branch_id=19))
            session.add(db.MapRepsToCustomer(rep_id=2, customer_branch_id=31))
            session.add(db.MapCityName(recorded_name="CHATNOGA", standard_name="CHATTANOOGA"))
            session.add(db.MapCityName(recorded_name="PT_ST_LUCIE", standard_name="PORT SAINT LUCIE"))
            session.add(db.MapCityName(recorded_name="BLUERIDGE", standard_name="BLUE RIDGE"))
            session.add(db.FinalCommissionData(
                year=2019, month="January", manufacturer="ADP", salesman="mwr",
                customer_name="Coastal Supply", city="Knoxville", state = "TN",
                inv_amt=inv_amts[0], comm_amt=comm_amts[0]
            ))
            session.add(db.FinalCommissionData(
                year=2020, month="November", manufacturer="Berry", salesman="sca",
                customer_name="Baker Distributing", city="Jacksonville", state = "FL",
                inv_amt=inv_amts[1], comm_amt=comm_amts[1]
            ))
            session.add(db.FinalCommissionData(
                year=2021, month="April", manufacturer="Allied", salesman="jdc",
                customer_name="Dealers Supply", city="Forest Park", state = "GA",
                inv_amt=inv_amts[2], comm_amt=comm_amts[2]
            ))
            session.add(db.FinalCommissionData(
                year=2022, month="July", manufacturer="Atco", salesman="red",
                customer_name="Hinkle Metals", city="Birmingham", state = "AL",
                inv_amt=inv_amts[3], comm_amt=comm_amts[3]
            ))
            session.commit()
        return


    def test_get_mapping_tables(self):
        result = api_services.get_mapping_tables(self.db)
        expected = {"map_customer_name","map_city_names","map_reps_customers"}
        self.assertEqual(result,expected)

    def test_get_mappings(self):
        results = {
            "map_customer_name": api_services.get_mappings(
                conn=self.db,
                table="map_customer_name"),
            "map_city_names": api_services.get_mappings(
                conn=self.db,
                table="map_city_names"),
            "map_reps_customers": api_services.get_mappings(
                conn=self.db,
                table="map_reps_customers")
            }
        for table,result in results.items():
            expected = self.entries_dfs[table]
            assert_frame_equal(result,expected)

    def test_set_mapping(self):
        data_to_add = {
            "map_customer_name": {"recorded_name": ["WICHITTEN"], "standard_name": ["WITTICHEN SUPPLY COMPANY"]},
            "map_city_names": {"recorded_name": ["FORREST PARK"], "standard_name": ["FOREST PARK"]},
            "map_reps_customers": {"rep_id": [1], "customer_branch_id": [32]}
        }

        mapping_tbls = api_services.get_mapping_tables(self.db)
        for tbl in mapping_tbls:
            set_result = api_services.set_mapping(self.db, tbl, pd.DataFrame(data_to_add[tbl]))
            self.assertTrue(set_result)

        for tbl in data_to_add:
            data_to_add[tbl]["id"] = len(self.entries_dfs[tbl])+1
        
        for mapping_tbl in mapping_tbls:
            expected = pd.concat([
                self.entries_dfs[mapping_tbl],
                pd.DataFrame(data_to_add[mapping_tbl])
                ], ignore_index=True)

            get_result = api_services.get_mappings(self.db, mapping_tbl)
            assert_frame_equal(get_result, expected)

    def test_del_mapping(self):
        mapping_tables = api_services.get_mapping_tables(self.db)
        
        for tbl, data in self.entries_dfs.items():
            if tbl not in mapping_tables:
                continue
            rec_to_del = randint(1,len(data))
            del_result = api_services.del_mapping(self.db, table=tbl, id=rec_to_del)
            self.assertTrue(del_result)
            expected = data[data["id"] != rec_to_del].reset_index(drop=True)
            get_result = api_services.get_mappings(self.db, tbl)
            assert_frame_equal(get_result, expected)

###
    def test_get_final_data(self):
        table = "final_commission_data"
        result = api_services.get_final_data(self.db)
        expected = self.entries_dfs[table]
        assert_frame_equal(result, expected)

    def test_record_final_data(self):
        table = "final_commission_data"
        data_to_add = {
            "year": [2022, 2022],
            "month": ["June","April"],
            "manufacturer": ["AMBRO CONTROLS","AMBRO CONTROLS"],
            "salesman": ["jdc","mwr"],
            "customer_name": ["WINSUPPLY", "EDS SUPPLY COMPANY"],
            "city": ["Baldwin","Nashville"],
            "state": ["GA","TN"],
            "inv_amt": [randint(100,50000000)/100 for _ in range(2)]
        }
        data_to_add["comm_amt"] = [round(inv*3/100, 2) for inv in data_to_add["inv_amt"]]

        record_result = api_services.record_final_data(self.db, pd.DataFrame(data_to_add))
        self.assertTrue(record_result)

        data_to_add["id"] = [len(self.entries_dfs[table])+num for num in range(1,3)]
        expected = pd.concat([self.entries_dfs[table],pd.DataFrame(data_to_add)], ignore_index=True)
        get_result = api_services.get_final_data(self.db)
        assert_frame_equal(get_result, expected)
###
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