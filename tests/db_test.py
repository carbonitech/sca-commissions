import unittest
import dotenv
import os

from app.db import db, tables

dotenv.load_dotenv()

TABLES = tables.TABLES

class TestSQLDB(unittest.TestCase):

    def setUp(self):
        db_url = os.getenv("TESTING_DATABASE_URL")
        self.tables = TABLES

        self.sql_db = db.SQLDatabase(db_url)
        with self.sql_db as db_conn:
            db_conn: db.SQLDatabase
            for name, schema in self.tables.items():
                db_conn.create_table(table_name=name, columns=schema)


    def test_creating_data(self):
        """test committing data, loaded into class attrs using set_data, to a SQL database with SQL"""

        with self.sql_db as db_conn:
            db_conn: db.SQLDatabase
            db_conn.create_record(table="customers", data={"name": "Joe Carboni"})
            result = db_conn.connection.execute("SELECT * FROM customers;").fetchone()

        self.assertEqual(result, (1,"Joe Carboni"))



    def test_updating_data(self):
        """test changing existing data in a table in the database using an id"""

        with self.sql_db as db_conn:
            db_conn: db.SQLDatabase
            db_conn.connection.execute("INSERT INTO customers (name) VALUES ('Mark Fields');")
            rec_id = db_conn.connection.execute("SELECT id FROM customers WHERE name = 'Mark Fields';").fetchone()[0]
            
            db_conn.update_record(table="customers", id=rec_id, set_="name", to="Greg Wilkes")
            updated_record = db_conn.connection.execute("SELECT * FROM customers WHERE id = %s;", rec_id).fetchone()

        self.assertEqual(updated_record, (rec_id, "Greg Wilkes"))

    def test_delete_data(self):
        """test deleting existing data in a table in the database using an id"""

        with self.sql_db as db_conn:
            db_conn: db.SQLDatabase
            db_conn.connection.execute("INSERT INTO map_city_names (recorded_name, standard_name) VALUES ('CHATANOOGA','CHATTANOOGA');")
            rec_id = db_conn.connection.execute("SELECT id FROM map_city_names WHERE standard_name = 'CHATTANOOGA';").fetchone()[0]

            db_conn.delete_record(table="map_city_names", id=rec_id)
            result = rec_id = db_conn.connection.execute("SELECT id FROM map_city_names WHERE id = %s;", rec_id).fetchone()

        self.assertEqual(result, None)


    def test_selecting_data(self):
        """test selecting and returning data in a table in the database using either a list of columns or a wildcard"""
        
        with self.sql_db as db_conn:
            db_conn: db.SQLDatabase
            db_conn.connection.execute("INSERT INTO manufacturers_reports (manufacturer_id, report_name, yearly_frequency, POS_report) \
                    VALUES (%s,%s,%s,%s);", (70, "Baker POS Report", 4, True))

            result = db_conn.select_records(table="manufacturers_reports", 
                        columns=["report_name", "POS_report", "manufacturer_id", "yearly_frequency"],
                        constraints={"report_name": "Baker POS Report"})

            db_conn.connection.execute("INSERT INTO manufacturers_reports (manufacturer_id, report_name, yearly_frequency, POS_report) \
                    VALUES (%s,%s,%s,%s);", (4, "ADP Commissions", 12, False))

            result_2 = db_conn.select_records(table="manufacturers_reports")

        self.assertEqual(result, [("Baker POS Report", True, 70, 4)])
        self.assertEqual(result_2,[(1, 70, "Baker POS Report", 4, True),(2, 4, "ADP Commissions", 12, False)])


    def tearDown(self):
        with self.sql_db as db_conn:
            db_conn: db.SQLDatabase
            for table in self.tables:
                db_conn.remove_table(table)
