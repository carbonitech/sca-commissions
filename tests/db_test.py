import unittest
import dotenv
import os
import datetime
from json import dumps
from random import randint, choice

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
        'map_reps_customers': models.MapRepsToCustomer,
        'report_submissions_log': models.ReportSubmissionsLog,
        'report_processing_steps_log': models.ReportProcessingStepsLog,
        'current_errors': models.CurrentError,
        'final_commission_data': models.FinalCommissionData
}



class TestCRUDFunctions(unittest.TestCase):

    def setUp(self):
        
        # set up database
        db_url = os.getenv("DATABASE_URL")
        self.db = create_engine(db_url)
        models.Base.metadata.create_all(self.db)
        self.db_services = db_services.DatabaseServices(engine=self.db)

        # load csv files
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
                    session.add(DB_TABLES[table](**row)) 
                    # col names in csv must match table schema
            session.commit()

        # insert expected postgreSQL table id values into each pandas DataFrame
        add_one = lambda val: val+1
        for dataset in self.tables.values():
            dataset.reset_index(inplace=True)
            dataset.rename(columns={"index":"id"}, inplace=True)
            dataset.id = dataset.id.apply(add_one)
            
        return

    
    def test_get_reps_to_cust_ref(self):
        customers_table: pd.DataFrame = self.tables["customers"]
        branches: pd.DataFrame = self.tables["customer_branches"]
        cities: pd.DataFrame = self.tables["cities"]
        states: pd.DataFrame = self.tables["states"]
        rep_map: pd.DataFrame = self.tables["map_reps_customers"]
        reps: pd.DataFrame = self.tables["representatives"]
        all_merged = customers_table \
            .merge(branches, left_on="id", right_on="customer_id", suffixes=(None,"_branches")) \
            .merge(cities, left_on="city_id", right_on="id", suffixes=(None,"_cities")) \
            .merge(states, left_on="state_id", right_on="id", suffixes=(None,"_states")) \
            .merge(rep_map, left_on="id_branches", right_on="customer_branch_id") \
            .merge(reps, left_on="rep_id", right_on="id")
        
        expected = all_merged.loc[:,["name", "name_cities", "name_states", "initials"]]
        expected.columns = ["customer_name", "city", "state", "rep"]

        # pd.read_sql produces different ordering than pd.merge
        # sorting both from left to right and resetting the index
        # simply to execute a comparison for equality
        expected.sort_values(by=expected.columns.tolist(), inplace=True)
        expected.reset_index(drop=True, inplace=True)

        result = self.db_services.get_reps_to_cust_ref()
        result.sort_values(by=result.columns.tolist(), inplace=True)
        result.reset_index(drop=True, inplace=True)

        assert_frame_equal(result,expected)



    def test_get_mapping_tables(self):
        result = self.db_services.get_mapping_tables()
        expected = {"map_customer_name","map_city_names","map_reps_customers"}
        self.assertEqual(result,expected)
        return

    def test_get_mappings(self):
        results = {
            "map_customer_name": self.db_services.get_mappings(
                                table="map_customer_name"),
            "map_city_names": self.db_services.get_mappings(
                                table="map_city_names"),
            "map_reps_customers": self.db_services.get_mappings(
                                table="map_reps_customers")
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
            "map_reps_customers": {"rep_id": [1], "customer_branch_id": [32]}
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

    def test_get_final_data(self):
        table = "final_commission_data"
        result = self.db_services.get_final_data()
        expected = self.tables[table]
        assert_frame_equal(result, expected)
        return

    def test_record_final_data(self):
        table = "final_commission_data"
        data_to_add = {
            "submission_id": [randint(101,200)]*2,
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

        record_result = self.db_services.record_final_data(pd.DataFrame(data_to_add))
        self.assertTrue(record_result)

        data_to_add["id"] = [len(self.tables[table])+num for num in range(1,3)]
        expected = pd.concat([self.tables[table],pd.DataFrame(data_to_add)], ignore_index=True)
        get_result = self.db_services.get_final_data()
        assert_frame_equal(get_result, expected)
        return

    def test_get_manufacturers_reports(self):
        table = "manufacturers_reports"
        rand_manf_id = choice(self.tables["manufacturers_reports"].loc[:,"manufacturer_id"].tolist())
        result = self.db_services.get_manufacturers_reports(manufacturer_id=rand_manf_id)
        expected = self.tables[table][self.tables[table].manufacturer_id == rand_manf_id].reset_index(drop=True)
        assert_frame_equal(result,expected)
        return

    def test_get_submissions_metadata(self):
        table = "report_submissions_log"
        rand_manf_id = choice(self.tables["manufacturers_reports"].loc[:,"manufacturer_id"].tolist())
        result = self.db_services.get_submissions_metadata(manufacturer_id=rand_manf_id)
        report_ids = result.loc[:,"id"].tolist() 
        expected = self.tables[table][self.tables[table].report_id.isin(report_ids)].reset_index(drop=True)
        assert_frame_equal(result, expected)
        return

    def test_record_submissions_metadata(self):
        table = "report_submissions_log"
        data_to_add = {
            "submission_date": [datetime.datetime(2022,1,31), datetime.datetime(2022,3,15)],
            "reporting_month": [12,2],
            "reporting_year": [2021,2022]
        }
        data_df = pd.DataFrame(data_to_add)
        report_ids = [choice(self.tables["manufacturers_reports"].loc[:,"id"].tolist()) for _ in range(2)]
        for i,rid in enumerate(report_ids):
            record_result = self.db_services.record_submission_metadata(report_id=rid, data=data_df.iloc[i])
            self.assertGreater(record_result,0)
            manf_id = self.tables["manufacturers_reports"] \
                .loc[self.tables["manufacturers_reports"].id == rid] \
                .manufacturer_id.iat[0]
            manf_id = manf_id.item()
            get_result = self.db_services.get_submissions_metadata(manf_id)
            get_result_filtered = get_result[get_result.id == record_result].reset_index(drop=True)
            ### MAJOR ISSUES WITH TYPING AND FRAME STRUCTURE RECREATION
            # TODO: REWORK HOW THE EXPECTED COND. IS GENERATED
            expected = pd.concat([data_df.iloc[i],pd.Series({"report_id": rid})]).to_frame().T
            expected["reporting_month"] = expected["reporting_month"].astype("int64")
            expected["reporting_year"] = expected["reporting_year"].astype("int64")
            expected["report_id"] = expected["report_id"].astype("int64")
            assert_frame_equal(
                get_result_filtered.loc[:,~get_result_filtered.columns.isin(["id"])],
                expected
            )
        return

    def test_del_submission(self):
        metadata_table = 'report_submissions_log'
        rec_to_del = choice(self.tables[metadata_table].loc[:,"id"].tolist())
        
        del_result = self.db_services.del_submission(rec_to_del)
        self.assertTrue(del_result)

        report_id_of_sub = self.tables[metadata_table]["report_id"]\
                .loc[self.tables[metadata_table]["id"] == rec_to_del].item()
        manf_id_of_sub = self.tables["manufacturers_reports"]["manufacturer_id"]\
                .loc[self.tables["manufacturers_reports"]["id"] == report_id_of_sub].item()

        get_result = self.db_services.get_submissions_metadata(manufacturer_id=manf_id_of_sub)

        manufacturers_reports = self.db_services.get_manufacturers_reports(manf_id_of_sub)
        manf_report_ids = manufacturers_reports.loc[:,"id"].tolist()
        expected = self.tables[metadata_table]\
                .loc[(self.tables[metadata_table]["id"] != rec_to_del)
                & (self.tables[metadata_table]["id"].isin(manf_report_ids))]
        expected = expected.reset_index(drop=True)

        # Index mismatch error arises when DFs are empty
        # TODO: FIX expected case to account for this, only one assert test
        if get_result.empty and expected.empty:
            self.assertTrue(get_result.empty)
            self.assertTrue(expected.empty)
        else:
            assert_frame_equal(get_result, expected)
        return

    def test_get_processing_steps(self):
        table = 'report_processing_steps_log'
        rand_sub_id = choice(self.tables[table].loc[:,"submission_id"].tolist())
        result = self.db_services.get_processing_steps(submission_id=rand_sub_id)
        expected = self.tables[table].loc[
            self.tables[table].submission_id == rand_sub_id
        ]
        expected.reset_index(drop=True,inplace=True)
        assert_frame_equal(result,expected)
        return
   
    def test_record_processing_steps(self):
        table = "report_processing_steps_log"
        data_to_add = {
            "step_num": [randint(1,100) for _ in range(3)],
            "description": ["removed rows with blank values", "replaced blanks with zeros", "selected columns"]
        }
        sub_id = randint(1,1000)
        record_result = self.db_services.record_processing_steps(sub_id, data=pd.DataFrame(data_to_add))
        self.assertTrue(record_result)

        data_to_add["id"] = [len(self.tables[table])+num for num in range(1,4)]
        expected = pd.DataFrame(data_to_add)
        expected["submission_id"] = sub_id
        expected.insert(0,"submission_id",expected.pop("submission_id"))
        expected.insert(0,"id",expected.pop("id"))
        get_result = self.db_services.get_processing_steps(sub_id)
        assert_frame_equal(get_result, expected)
        return      

    def test_del_processing_steps(self):
        table = "report_processing_steps_log"
        rec_to_del = choice(self.tables[table].loc[:,"submission_id"].tolist())
        del_result = self.db_services.del_processing_steps(rec_to_del)
        self.assertTrue(del_result)

        get_result = self.db_services.get_processing_steps(rec_to_del)
        expected = pd.DataFrame(columns=self.tables[table].columns)
        assert_frame_equal(get_result,expected)
        return
     
    def test_get_errors(self):
        table = 'current_errors'
        rand_sub_id = choice(self.tables[table].loc[:,"submission_id"].tolist())
        result = self.db_services.get_errors(rand_sub_id)
        expected = self.tables[table].loc[
            self.tables[table].submission_id == rand_sub_id
        ]
        expected.reset_index(drop=True,inplace=True)
        assert_frame_equal(result,expected)
        return

    def test_record_errors(self):
        table = "current_errors"
        data_to_add = {
            "row_index": [randint(1,1000) for _ in range(2)],
            "field": ["customer_name", "city"],
            "value_type": ["str", "str"],
            "value_content": ["mingledi", "dalers supply"],
            "reason": ["name not in mapping"]*2,
            "row_data": [dumps({"customer_name": "mingledi", "OTHER": 0.1234}),
                    dumps({"city": "dalers supply", "OTHER": 1234})]
        }
        submission_id = randint(1,1000)
        data_add_df = pd.DataFrame(data_to_add)
        result = self.db_services.record_errors(submission_id, data_add_df)
        self.assertTrue(result)

        get_result = self.db_services.get_errors(submission_id)
        
        expected_new = data_add_df.copy()
        expected_new.insert(0,"submission_id", submission_id)
        expected_new.insert(0,"id",[len(self.tables[table])+num for num in range(1,3)])
        expected = pd.concat([self.tables[table], expected_new])
        expected = expected[expected.submission_id == submission_id].reset_index(drop=True)
        
        assert_frame_equal(get_result, expected)
        return


    def test_del_error(self):
        setup_df = self.tables["current_errors"]
        rec_to_del = choice(setup_df["id"].tolist())
        result = self.db_services.del_error(rec_to_del)
        self.assertTrue(result)

        sub_id_of_rec = setup_df["submission_id"].loc[setup_df.id == rec_to_del].item()
        get_result = self.db_services.get_errors(sub_id_of_rec)
        expected = setup_df.loc[setup_df.id != rec_to_del]
        expected = expected.loc[expected.submission_id == sub_id_of_rec]

        #TODO: address typing issues causing comp of empty dfs to fail
        if get_result.empty:
            self.assertTrue(get_result.empty)
            self.assertTrue(expected.empty)
        else:
            assert_frame_equal(get_result, expected)



    def test_get_submission_files(self): ...
    def test_record_submission_file(self): ...
    def test_del_submission_file(self): ...

    def tearDown(self):
        models.Base.metadata.drop_all(self.db)
        return