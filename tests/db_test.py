import unittest
import dotenv
import os
import datetime
from json import dumps
from typing import Dict
from random import randint, choice, sample

import pandas as pd
from pandas.testing import assert_frame_equal
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import models
from app.db import db_services

dotenv.load_dotenv()

class TestCRUDFunctions(unittest.TestCase):

    def setUp(self):
        make_date = lambda *args: datetime.datetime(*args)
        entries_by_table: Dict[str,dict] = {
            "map_customer_name": {
                "recorded_name": ["MINGLEDROFFS", "EDSSUPPLYCO", "DSC"],
                "standard_name": ["MINGLEDORFFS", "EDS SUPPLY COMPANY","DEALERS SUPPLY COMPANY"]
            },
            "map_reps_customers": {
                "rep_id": [1,2,3],
                "customer_branch_id": [1,2,3],
            },
            "map_city_names": {
                "recorded_name": ["CHATNOGA", "PT_ST_LUCIE", "BLUERIDGE"],
                "standard_name": ["CHATTANOOGA", "PORT SAINT LUCIE", "BLUE RIDGE"]
            },
            "final_commission_data": {
                "submission_id": [randint(1,100)]*4,
                "year": [2019,2020,2021,2022],
                "month": ["January", "November", "April", "July"],
                "manufacturer": ["ADP", "Berry", "Allied", "Atco"],
                "salesman": ["mwr","sca", "jdc", "red"],
                "customer_name": ["Coastal Supply","Baker Distributing","Dealers Supply","Hinkle Metals"],
                "city": ["Knoxville","Jacksonville","Forest Park","Birmingham"],
                "state": ["TN","FL","GA","AL"]
            },
            "manufacturers_reports": {
                "report_name": ["Famco Commission Report", "Baker POS Report", "ADP Salesman Report"],
                "yearly_frequency": [12,4,12],
                "POS_report": [False, True, False]
            },
            "report_submissions_log": {
                "submission_date": [make_date(2022, 1, 31), make_date(2022,2,15,13),
                        make_date(2022,3,3,8,19,49)],
                "reporting_month": [12,1,2],
                "reporting_year": [2021,2022,2022]
            },
            "report_processing_steps_log": {
                "step_num": [1,2,3],
                "description": ["removed blank rows", "corrected customer names",
                        "added rep assignments"]
            },
            "current_errors": {
                "submission_id": [randint(1,1000) for _ in range(5)],
                "row_index": [randint(1,1000) for _ in range(5)],
                "field": ["customer_name", "city", "inv_amt", "comm_amt", "customer_name"],
                "value_type": ["str","str","None","float","str"],
                "value_content": ["Mingledroff", "forrrest park", "NaN", "-999999", "trane supplies"],
                "reason": ["customer name not in the mapping table", "city name not in the mapping table",
                        "expected value to be a number", "value is a high negative number",
                        "custome name not in the mapping table"],
                "row_data": [
                        dumps({"Customer": "Mingledroff", "other_data": "other data"}),
                        dumps({"City": "forrrest park", "other data": "other data"}),
                        dumps({"inv_amt": None, "other data": 1354}),
                        dumps({"comm_amt": -999999, "other data": "other data"}),
                        dumps({"customer_name": "trane supplies", "other_data": "other data"})

                ]
            },
            "customers": {
                "name" : ["WITTICHEN", "ED'S SUPPLY", "MINGLEDORFF'S"]
            },
            "customer_branches": {
                "customer_id": [1,2,3],
                "city": ["Birmingham", "Nashville", "Norcross"],
                "state": ["AL","TN","GA"],
                "zip": [35233, 37203, 30092]
            },
            "representatives": {
                "first_name": ["Roger", "Matt", "Joe"],
                "last_name": ["Daniel", "Reiners", "Carboni"],
                "initials": ["red", "mwr", "jdc"],
                "date_joined": [make_date(1997,6,23), make_date(2004,9,2), 
                        make_date(2014,1,1)]
            }
        }

        ## ADDITIONS TO THE ENTRIES_BY_TABLE DICT
        # final commission data
        inv_amts = [randint(100,50000000)/100 for _ in range(0,4)]
        entries_by_table["final_commission_data"]["inv_amt"] = inv_amts
        comm_amts = [round(num*3/100, 2) for num in inv_amts]
        entries_by_table["final_commission_data"]["comm_amt"] = comm_amts

        # manufacturer's reports
        # dynamically range by length of an existing 'column'
        manufacturer_ids = [
            randint(1,20) for _ in range(
                    len(entries_by_table["manufacturers_reports"]["report_name"])
                )
        ]
        entries_by_table["manufacturers_reports"]["manufacturer_id"] = manufacturer_ids

        # report submissions log
        # make report_ids the same len and number as the id col in "manufacturers_reports"
        report_ids = [num+1 for num in range(len(manufacturer_ids))]
        entries_by_table["report_submissions_log"]["report_id"] = report_ids

        # report processing steps
        sub_ids = sample(report_ids,k=len(report_ids)) # needs to match id num range of report_submissions_log
        entries_by_table["report_processing_steps_log"]["submission_id"] = sub_ids

        ## CREATE DATAFRAMES FOR EACH DICT TABLE
        self.entries_dfs = {tbl_name:pd.DataFrame(data) for tbl_name,data in entries_by_table.items()}
        ## ADD ID COLUMNS BASED ON THE LENGTH OF THE TABLE - PGSQL INDEX STARTS AT 1
        for tbl,df in self.entries_dfs.items():
             self.entries_dfs[tbl].insert(0,"id",list(range(1,len(df)+1))) 

        # reorder columns where needed
        self.entries_dfs["manufacturers_reports"].insert(1,"manufacturer_id", self.entries_dfs["manufacturers_reports"].pop("manufacturer_id"))
        self.entries_dfs["report_processing_steps_log"].insert(
            1, "submission_id", self.entries_dfs["report_processing_steps_log"].pop("submission_id")
        )

        ## WRITE DATA TO TESTING DB IN PGSQL
        db_url = os.getenv("DATABASE_URL")
        self.db = create_engine(db_url)
        models.Base.metadata.create_all(self.db)
        with Session(self.db) as session:
            for name, table in self.entries_dfs.items():
                tables = {
                    'map_customer_name': models.MapCustomerName,
                    'map_city_names': models.MapCityName,
                    'map_reps_customers': models.MapRepsToCustomer,
                    'manufacturers': models.Manufacturer,
                    'manufacturers_reports': models.ManufacturersReport,
                    'report_submissions_log': models.ReportSubmissionsLog,
                    'final_commission_data': models.FinalCommissionData,
                    'report_processing_steps_log': models.ReportProcessingStepsLog,
                    'current_errors': models.CurrentError,
                    "customers": models.Customer,
                    "customer_branches": models.CustomerBranch,
                    "representatives": models.Representative
                }
                table_no_id: pd.DataFrame = table.loc[:,(table.columns != "id")]
                for row in table_no_id.to_dict("records"):
                    session.add(tables[name](**row))
            session.commit()

        self.db_services = db_services.DatabaseServices(engine=self.db)
        return

    
    def test_get_reps_to_cust_ref(self):
        result = self.db_services.get_reps_to_cust_ref()
        expected = pd.DataFrame(
            {'name': ['WITTICHEN', "ED'S SUPPLY", "MINGLEDORFF'S"],
            'city': ['Birmingham', 'Nashville', 'Norcross'],
            'state': ['AL', 'TN', 'GA'],
            'initials': ['red', 'mwr', 'jdc']})
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
            expected = self.entries_dfs[table]
            assert_frame_equal(result,expected)
            return

    def test_set_mapping(self):
        data_to_add = {
            "map_customer_name": {"recorded_name": ["WICHITTEN"], "standard_name": ["WITTICHEN SUPPLY COMPANY"]},
            "map_city_names": {"recorded_name": ["FORREST PARK"], "standard_name": ["FOREST PARK"]},
            "map_reps_customers": {"rep_id": [1], "customer_branch_id": [32]}
        }

        mapping_tbls = self.db_services.get_mapping_tables()
        for tbl in mapping_tbls:
            set_result = self.db_services.set_mapping(tbl, pd.DataFrame(data_to_add[tbl]))
            self.assertTrue(set_result)

        for tbl in data_to_add:
            data_to_add[tbl]["id"] = len(self.entries_dfs[tbl])+1
        
        for mapping_tbl in mapping_tbls:
            expected = pd.concat([
                self.entries_dfs[mapping_tbl],
                pd.DataFrame(data_to_add[mapping_tbl])
                ], ignore_index=True)

            get_result = self.db_services.get_mappings(mapping_tbl)
            assert_frame_equal(get_result, expected)
        return

    def test_del_mapping(self):
        mapping_tables = self.db_services.get_mapping_tables()
        
        for tbl, data in self.entries_dfs.items():
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
        expected = self.entries_dfs[table]
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

        data_to_add["id"] = [len(self.entries_dfs[table])+num for num in range(1,3)]
        expected = pd.concat([self.entries_dfs[table],pd.DataFrame(data_to_add)], ignore_index=True)
        get_result = self.db_services.get_final_data()
        assert_frame_equal(get_result, expected)
        return

    def test_get_manufacturers_reports(self):
        table = "manufacturers_reports"
        rand_manf_id = choice(self.entries_dfs["manufacturers_reports"].loc[:,"manufacturer_id"].tolist())
        result = self.db_services.get_manufacturers_reports(manufacturer_id=rand_manf_id)
        expected = self.entries_dfs[table][self.entries_dfs[table].manufacturer_id == rand_manf_id].reset_index(drop=True)
        assert_frame_equal(result,expected)
        return

    def test_get_submissions_metadata(self):
        table = "report_submissions_log"
        rand_manf_id = choice(self.entries_dfs["manufacturers_reports"].loc[:,"manufacturer_id"].tolist())
        result = self.db_services.get_submissions_metadata(manufacturer_id=rand_manf_id)
        report_ids = result.loc[:,"id"].tolist() 
        expected = self.entries_dfs[table][self.entries_dfs[table].report_id.isin(report_ids)].reset_index(drop=True)
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
        report_ids = [choice(self.entries_dfs["manufacturers_reports"].loc[:,"id"].tolist()) for _ in range(2)]
        for i,rid in enumerate(report_ids):
            record_result = self.db_services.record_submission_metadata(report_id=rid, data=data_df.iloc[i])
            self.assertGreater(record_result,0)
            manf_id = self.entries_dfs["manufacturers_reports"] \
                .loc[self.entries_dfs["manufacturers_reports"].id == rid] \
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
        rec_to_del = choice(self.entries_dfs[metadata_table].loc[:,"id"].tolist())
        
        del_result = self.db_services.del_submission(rec_to_del)
        self.assertTrue(del_result)

        report_id_of_sub = self.entries_dfs[metadata_table]["report_id"]\
                .loc[self.entries_dfs[metadata_table]["id"] == rec_to_del].item()
        manf_id_of_sub = self.entries_dfs["manufacturers_reports"]["manufacturer_id"]\
                .loc[self.entries_dfs["manufacturers_reports"]["id"] == report_id_of_sub].item()

        get_result = self.db_services.get_submissions_metadata(manufacturer_id=manf_id_of_sub)

        manufacturers_reports = self.db_services.get_manufacturers_reports(manf_id_of_sub)
        manf_report_ids = manufacturers_reports.loc[:,"id"].tolist()
        expected = self.entries_dfs[metadata_table]\
                .loc[(self.entries_dfs[metadata_table]["id"] != rec_to_del)
                & (self.entries_dfs[metadata_table]["id"].isin(manf_report_ids))]
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
        rand_sub_id = choice(self.entries_dfs[table].loc[:,"submission_id"].tolist())
        result = self.db_services.get_processing_steps(submission_id=rand_sub_id)
        expected = self.entries_dfs[table].loc[
            self.entries_dfs[table].submission_id == rand_sub_id
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

        data_to_add["id"] = [len(self.entries_dfs[table])+num for num in range(1,4)]
        expected = pd.DataFrame(data_to_add)
        expected["submission_id"] = sub_id
        expected.insert(0,"submission_id",expected.pop("submission_id"))
        expected.insert(0,"id",expected.pop("id"))
        get_result = self.db_services.get_processing_steps(sub_id)
        assert_frame_equal(get_result, expected)
        return      

    def test_del_processing_steps(self):
        table = "report_processing_steps_log"
        rec_to_del = choice(self.entries_dfs[table].loc[:,"submission_id"].tolist())
        del_result = self.db_services.del_processing_steps(rec_to_del)
        self.assertTrue(del_result)

        get_result = self.db_services.get_processing_steps(rec_to_del)
        expected = pd.DataFrame(columns=self.entries_dfs[table].columns)
        assert_frame_equal(get_result,expected)
        return
     
    def test_get_errors(self):
        table = 'current_errors'
        rand_sub_id = choice(self.entries_dfs[table].loc[:,"submission_id"].tolist())
        result = self.db_services.get_errors(rand_sub_id)
        expected = self.entries_dfs[table].loc[
            self.entries_dfs[table].submission_id == rand_sub_id
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
        expected_new.insert(0,"id",[len(self.entries_dfs[table])+num for num in range(1,3)])
        expected = pd.concat([self.entries_dfs[table], expected_new])
        expected = expected[expected.submission_id == submission_id].reset_index(drop=True)
        
        assert_frame_equal(get_result, expected)
        return


    def test_del_error(self):
        setup_df = self.entries_dfs["current_errors"]
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