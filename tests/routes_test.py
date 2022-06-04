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


    def test_get_mapping(self):

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

        for tbl, entry in mapping_table_entries.items():
            result = api_services.get_mapping(database=self.sql_db, table=tbl)
            expected = pd.DataFrame({**{"id":[1]},**{k:[v] for k,v in entry.items()}}) # upgrading to python 3.10 could replace unpacking ** with a pipe | operator
            assert_frame_equal(result,expected)
    
    
    def test_set_mapping(self):
        ...

    def test_del_mapping(self):
        ...

    def tearDown(self):
        with self.sql_db as db_conn:
            db_conn: db.SQLDatabase
            for table in self.tables:
                db_conn.remove_table(table)