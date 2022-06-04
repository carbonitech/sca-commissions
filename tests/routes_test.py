import unittest
import dotenv
import os
from typing import Dict

import pandas as pd
from pandas.util.testing import assert_frame_equal

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


    def test_get_mapping_tables(self):

        mapping_tables: set = api_services.get_mapping_tables(database=self.sql_db)
        expected_tables: set = {table for table in TABLES if table.split("_")[0] == "map"}

        self.assertEqual(mapping_tables, expected_tables)


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
    
    
    def test_set_mapping(self):

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

        for tbl, entry in mapping_table_entries.items():

            entry_df = pd.DataFrame({k:[v] for k,v in entry.items()})
            setting_return = api_services.set_mapping(database=self.sql_db, table=tbl, data=entry_df)
            self.assertTrue(setting_return)

            result_data = api_services.get_mappings(database=self.sql_db, table=tbl)
            entry_df.insert(0,"id",[1])
            expected = entry_df
            assert_frame_equal(result_data, expected)

        

    def test_del_mapping(self):
        ...

    def tearDown(self):
        with self.sql_db as db_conn:
            db_conn: db.SQLDatabase
            for table in self.tables:
                db_conn.remove_table(table)