import unittest
import dotenv
import os

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

        mapping_tables: set = api_services.get_mapping_tables(db=self.sql_db)
        expected_tables: set = {table for table in TABLES if table.split("_")[0] == "map"}

        self.assertEqual(mapping_tables, expected_tables)


    def tearDown(self):
        with self.sql_db as db_conn:
            db_conn: db.SQLDatabase
            for table in self.tables:
                db_conn.remove_table(table)