import unittest
import dotenv
import os
from typing import Dict

import pandas as pd
from pandas.util.testing import assert_frame_equal
from random import randint

from app.db import db, tables
from app.routes import api_services

dotenv.load_dotenv()

TABLES = tables.TABLES

class TestApiServiceFunctions(unittest.TestCase):

    def setUp(self):
        db_url = os.getenv("TESTING_DATABASE_URL")
        self.tables = TABLES

        self.sql_db = db.SQLDatabase(db_url)
        with self.sql_db as db_conn:
            db_conn: db.SQLDatabase
            for name, schema in self.tables.items():
                db_conn.create_table(table_name=name, columns=schema)
        return


    def test_get_mapping_tables(self):
        mapping_tables: set = api_services.get_mapping_tables(database=self.sql_db)
        expected_tables: set = {table for table in TABLES if table.split("_")[0] == "map"}
        self.assertEqual(mapping_tables, expected_tables)
        return


    def test_get_mappings(self):

        # test data to insert by table
        mapping_table_entries: Dict[str,dict] = {
            "map_customer_name": {
                "recorded_name": "WICHITTEN SUPPLY",
                "standard_name": "WITTICHEN SUPPLY"
            },
            "map_rep": {
                "rep_id": 7,
                "customer_branch_id": 32,
            },
            "map_city_names": {
                "recorded_name": "FORREST PARK",
                "standard_name": "FOREST PARK"
            }
        }

        # create records to retrieve in each table
        with self.sql_db as db_conn:
            db_conn: db.SQLDatabase
            for tbl, entry in mapping_table_entries.items():
                db_conn.create_record(table=tbl, data=entry)

        # test that dataframes are equal
        for tbl, entry in mapping_table_entries.items():
            result = api_services.get_mappings(database=self.sql_db, table=tbl)
            expected = pd.DataFrame({**{"id":[1]},**{k:[v] for k,v in entry.items()}}) # upgrading to python 3.10 could replace unpacking ** with a pipe | operator
            assert_frame_equal(result,expected)

            result_data_list = result.iloc[0].tolist()
            expected_data_list = [1]+[val for val in entry.values()]
            self.assertEqual(result_data_list, expected_data_list)
            return
    

    def test_set_mapping(self):
        ## test insertion and extraction on each table with single record

        # test data to insert by table - first row
        mapping_table_entries_single: Dict[str,dict] = {
            "map_customer_name": {
                "recorded_name": "WICHITTEN SUPPLY",
                "standard_name": "WITTICHEN SUPPLY"
            },
            "map_rep": {
                "rep_id": 7,
                "customer_branch_id": 32,
            },
            "map_city_names": {
                "recorded_name": "FORREST PARK",
                "standard_name": "FOREST PARK"
            }
        }


        for tbl, entry in mapping_table_entries_single.items():
            # set data and confirm set_mapping returns true
            entry_df = pd.DataFrame({k:[v] for k,v in entry.items()})
            setting_return = api_services.set_mapping(database=self.sql_db, table=tbl, data=entry_df)
            self.assertTrue(setting_return)

            # confirm the data is what we expect coming from get_mappings
            result_data = api_services.get_mappings(database=self.sql_db, table=tbl)
            expected = entry_df.copy()
            expected.insert(0,"id",[1])
            assert_frame_equal(result_data, expected)


        ## test insertion and extraction on each table with multiple records
        mapping_table_entries_multi: Dict[str,dict] = {
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
            }
        }

        for tbl, entry in mapping_table_entries_multi.items():
            # set data and confirm set_mapping returns true
            entry_df = pd.DataFrame(entry)
            setting_return = api_services.set_mapping(database=self.sql_db, table=tbl, data=entry_df)
            self.assertTrue(setting_return)
            
            # confirm the data is what we expect coming from get_mappings
            result_data = api_services.get_mappings(database=self.sql_db, table=tbl)
            expected = entry_df.copy()
            expected.insert(0,"id",[2,3,4])
            first_row_vals = [1] + [val for val in mapping_table_entries_single[tbl].values()]
            first_row_cols = ["id"] + [col for col in mapping_table_entries_single[tbl].keys()]
            first_row_df = pd.DataFrame(columns=first_row_cols, data=[first_row_vals])
            expected = pd.concat([first_row_df, expected])
            expected.reset_index(drop=True, inplace=True)
            assert_frame_equal(result_data,expected)
            return

    def test_del_mapping(self):

        mapping_table_entries: Dict[str,dict] = {
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
            }
        }

        for tbl, entry in mapping_table_entries.items():
            # set data
            entry_df = pd.DataFrame(entry)
            api_services.set_mapping(database=self.sql_db, table=tbl, data=entry_df)
            # get a random record_id and delete it
            rec_id = randint(1,3)
            del_result = api_services.del_mapping(database=self.sql_db, table=tbl, id=rec_id)
            # generate expected data
            expected = entry_df.copy()
            expected.insert(0,"id",[1,2,3])
            expected.drop([rec_id-1], inplace=True)
            expected.reset_index(drop=True, inplace=True)
            # get table from db
            result_data = api_services.get_mappings(database=self.sql_db, table=tbl)
            # tests
            self.assertTrue(del_result)
            assert_frame_equal(result_data,expected)
            return


    def test_record_final_data(self):
        entry_dict = {
            "Year": [2019,2020,2021,2022],
            "Month": ["January", "November", "April", "July"],
            "Manufacturer": ["ADP", "Berry", "Allied", "Atco"],
            "Salesman": ["mwr","sca", "jdc", "red"],
            "Customer_Name": ["Coastal Supply","Baker Distributing","Dealers Supply","Hinkle Metals"],
            "City": ["Knoxville","Jacksonville","Forest Park","Birmingham"],
            "State": ["TN","FL","GA","AL"],
            "Inv_Amt": [randint(100,50000000)/100 for _ in range(0,4)],
        }
        entry_dict["Comm_Amt"] = [round(num*3/100, 2) for num in entry_dict["Inv_Amt"]]

        entry = pd.DataFrame(entry_dict)
        success = api_services.record_final_data(database=self.sql_db, data=entry)
        self.assertTrue(success)

        expected = entry.copy()
        expected.insert(0,"id",list(range(1,5)))

        with self.sql_db as db_conn:
            db_conn: db.SQLDatabase
            records = db_conn.select_records(table="final_commission_data")

        result = pd.DataFrame.from_records(records, columns=["id"]+list(entry_dict.keys()))
        result["Inv_Amt"] = pd.to_numeric(result["Inv_Amt"])
        result["Comm_Amt"] = pd.to_numeric(result["Comm_Amt"])
        assert_frame_equal(result, expected)
        return


    def test_get_final_data(self): ...
    def test_get_submissions_metadata(self): ...
    def test_del_submission(self): ...
    def test_get_submission_files(self): ...
    def test_record_submission_file(self): ...
    def test_del_submission_file(self): ...
    def test_get_processing_steps(self): ...
    def test_record_processing_steps(self): ...
    def test_del_processing_steps(self): ...
    def test_get_errors(self): ...
    def test_record_errors(self): ...
    def test_correct_error(self): ...
    def test_del_error(self): ...


    def tearDown(self):
        with self.sql_db as db_conn:
            db_conn: db.SQLDatabase
            for table in self.tables:
                db_conn.remove_table(table)
        return