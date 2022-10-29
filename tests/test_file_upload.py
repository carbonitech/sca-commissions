"""
    TODO: If this file is large enough, tests fail out on hitting maximum recursion depth. Needs fix to test larger files.
    TODO: tests are specific to the RE Michel file layout. should be generalized to test all file configurations
"""
import os
import pandas as pd
from fastapi.testclient import TestClient
from app.main import app
from tests.pytest_fixtures import set_overrides, clear_overrides, database

test_client = TestClient(app)

FILE_PATH = os.getenv("TEST_ADP_FILE")
FILE_NAME = os.path.basename(FILE_PATH)

def test_file_upload_returns_jsonapi_obj(database: database):
    """see that response json object matches the expected object structure for a JSON:API GET response"""
    set_overrides()
    form_body = {
        "reporting_month": 5,
        "reporting_year": 2022,
        "manufacturer_id": 1,
        "report_id": 4,
    }
    file = {"file": (FILE_NAME, open(FILE_PATH, 'rb'))}
    response = test_client.post("/commission-data", data=form_body, files=file)
    response_json = response.json()
    try:
        for table in ["processing_steps","commission_data", "errors", "submissions"]:
            database.execute(f"DELETE FROM {table};")
        database.commit()
    finally:
        database.close()

    json_api_obj_returned = False
    match response_json:
        case {
            "jsonapi": a, 
            "meta": b, 
            "included": c, 
            "data": {
                "id": id, 
                "type": "submissions",
            }
        }:
            json_api_obj_returned = True

    assert json_api_obj_returned


def test_file_upload_commits_data(database: database):
    """
        test for
            1. the existance of a submission in the submissions table with the id returned in the response
            2. that the row counts between the file uploaded (blanks removed) matches the combined number in 
                commission_data and errors table.
    """
    set_overrides()
    form_body = {
        "reporting_month": 5,
        "reporting_year": 2022,
        "manufacturer_id": 1,
        "report_id": 4,
    }
    file = {"file": (FILE_NAME, open(FILE_PATH, 'rb'))}
    file_pandas = pd.read_excel(FILE_PATH).dropna(subset=["Branch#"])
    response = test_client.post("/commission-data", data=form_body, files=file)
    submission_id = {"id": response.json()["data"]["id"]}

    submissions_sql = "SELECT * FROM submissions WHERE id = :id;", submission_id
    commissions_data_sql = "SELECT * FROM commission_data WHERE submission_id = :id;", submission_id
    errors_sql = "SELECT row_data FROM errors WHERE submission_id = :id;", submission_id
    try:
        submission = database.execute(*submissions_sql).one()
        commissions_data = database.execute(*commissions_data_sql).all()
        errors = database.execute(*errors_sql).all()
        for table in ["processing_steps","commission_data", "errors", "submissions"]:
            database.execute(f"DELETE FROM {table};")
        database.commit()
    finally:
        database.close()

    assert commissions_data or errors, "Both commission_data and errors tables are empty"

    data_len_same_as_commissions_plus_errors = (len(file_pandas) == (len(commissions_data) + len(errors)))
    assert submission and data_len_same_as_commissions_plus_errors


def test_postprocessing_correct_sales_and_comm_amounts(database: database):
    """make sure the sales/inv and commission values are equal"""
    set_overrides()
    form_body = {
        "reporting_month": 5,
        "reporting_year": 2022,
        "manufacturer_id": 1,
        "report_id": 4,
    }
    file = {"file": (FILE_NAME, open(FILE_PATH, 'rb'))}
    file_pandas = pd.read_excel(FILE_PATH).dropna(subset=["Branch#"])
    total_sales = file_pandas["Cost"].sum()*0.75
    total_comm = total_sales*0.03

    response = test_client.post("/commission-data", data=form_body, files=file)
    submission_id = {"id": response.json()["data"]["id"]}

    commissions_data_sql = "SELECT * FROM commission_data WHERE submission_id = :id;", submission_id
    errors_sql = "SELECT row_data FROM errors WHERE submission_id = :id;", submission_id
    try:
        commissions_data = database.execute(*commissions_data_sql).all()
        errors = database.execute(*errors_sql).all()
        for table in ["processing_steps","commission_data", "errors", "submissions"]:
            database.execute(f"DELETE FROM {table};")
        database.commit()
    finally:
        database.close()

    if commissions_data:
        commissions_data = pd.DataFrame(list(commissions_data))
    if errors:
        errors = pd.DataFrame(list(errors))
    
    assert total_sales == commissions_data.iloc[:,-2].sum()/100
    assert total_comm == commissions_data.iloc[:,-2].sum()/100*0.03
    
    