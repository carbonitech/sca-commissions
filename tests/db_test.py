import unittest
import dotenv
import os
from random import randint, choice

import pandas as pd
from pandas.testing import assert_frame_equal
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from db import models, db_services

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



class TestRefTableManagement(unittest.TestCase):

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


    def test_get_mapping_tables(self):
        result = self.db_services.get_mapping_tables()
        expected = {"map_customer_name","map_city_names",
            "map_state_names","map_reps_customers"}
        self.assertEqual(result,expected)
        return

    def test_get_mappings(self):
        results = {
            "map_customer_name": self.db_services.get_mappings(
                                table="map_customer_name"),
            "map_city_names": self.db_services.get_mappings(
                                table="map_city_names"),
            "map_reps_customers": self.db_services.get_mappings(
                                table="map_reps_customers"),
            "map_state_names": self.db_services.get_mappings(
                                table="map_state_names")
            }
        for table,result in results.items():
            expected = self.tables[table]
            if expected.empty:
                # empty dfs from pd.read_sql return a different index class than expected.
                # this fix avoids failing test when comparing two empty dfs
                expected.index = pd.Index([], dtype='object')
            assert_frame_equal(result,expected)
            return

    def test_set_mapping(self):
        data_to_add = {
            "map_customer_name": {"recorded_name": ["WICHITTEN"], "customer_id": [44]},
            "map_city_names": {"recorded_name": ["FORREST PARK"], "city_id": [24]},
            "map_reps_customers": {"rep_id": [1], "customer_branch_id": [32]},
            "map_state_names": {"recorded_name": ["GORGIA"], "state_id": [12]}
        }

        mapping_tbls = self.db_services.get_mapping_tables()
        for tbl in mapping_tbls:
            set_result = self.db_services.set_mapping(tbl, pd.DataFrame(data_to_add[tbl]))
            self.assertTrue(set_result)

        for tbl in data_to_add:
            data_to_add[tbl]["id"] = len(self.tables[tbl])+1
        
        for mapping_tbl in mapping_tbls:
            if self.tables[mapping_tbl].empty:
                expected = pd.DataFrame(data_to_add[mapping_tbl])
                expected.insert(0,"id",expected.pop("id"))
            else:
                expected = pd.concat([
                    self.tables[mapping_tbl],
                    pd.DataFrame(data_to_add[mapping_tbl])
                    ], ignore_index=True)

            get_result = self.db_services.get_mappings(mapping_tbl)
            assert_frame_equal(get_result, expected)
        return

    def test_del_mapping(self):
        mapping_tables = self.db_services.get_mapping_tables()
        
        for tbl, data in self.tables.items():
            if tbl not in mapping_tables:
                continue
            rec_to_del = randint(1,len(data))
            del_result = self.db_services.del_mapping(table=tbl, id=rec_to_del)
            self.assertTrue(del_result)
            expected = data[data["id"] != rec_to_del].reset_index(drop=True)
            get_result = self.db_services.get_mappings(tbl)
            assert_frame_equal(get_result, expected)
        return


    def test_get_manufacturers_reports(self):
        table = "manufacturers_reports"
        rand_manf_id = choice(self.tables["manufacturers_reports"].loc[:,"manufacturer_id"].tolist())
        result = self.db_services.get_manufacturers_reports(manufacturer_id=rand_manf_id)
        expected = self.tables[table][self.tables[table].manufacturer_id == rand_manf_id].reset_index(drop=True)
        assert_frame_equal(result,expected)
        return


    def tearDown(self):
        models.Base.metadata.drop_all(self.db)
        return

 
class TestJoinedReferenceHelpers(unittest.TestCase):

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


    def test_get_reps_to_cust_branch_ref(self):
        branches: pd.DataFrame = self.tables["customer_branches"]
        rep_map: pd.DataFrame = self.tables["map_reps_customers"]
        all_merged = branches.merge(rep_map, left_on="id", right_on="customer_branch_id", suffixes=(None,"_repmap"))
        
        expected = all_merged.loc[:,["id_repmap", "customer_id", "city_id", 
                "state_id"]]
        expected.rename(columns={"id_repmap": "map_rep_customer_id"}, inplace=True)

        # pd.read_sql produces different ordering than pd.merge
        # sorting both from left to right and resetting the index
        # to execute a comparison for equality
        expected.sort_values(by=expected.columns.tolist(), inplace=True)
        expected.reset_index(drop=True, inplace=True)

        result = self.db_services.get_reps_to_cust_branch_ref()
        result.sort_values(by=result.columns.tolist(), inplace=True)
        result.reset_index(drop=True, inplace=True)

        assert_frame_equal(result,expected)
        return


    def tearDown(self):
        models.Base.metadata.drop_all(self.db)
        return