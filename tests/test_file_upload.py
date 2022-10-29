import os
from io import BytesIO
from fastapi.testclient import TestClient
from app.main import app
from json import dumps
from tests.pytest_fixtures import (
        set_overrides, clear_overrides, database, 
        RequestBody, RequestBodyRelationship)

test_client = TestClient(app)

FILE_PATH = os.getenv("TEST_ADP_FILE")
FILE_NAME = os.path.basename(FILE_PATH)

def test_file_upload(database: database):
    """
    TODO: If this file is large enough, this test fails out on hitting maximum recursion depth. Need to account for this
    """
    set_overrides()
    form_body = {
        "reporting_month":5,
        "reporting_year":2022,
        "manufacturer_id":1,
        "report_id":4,
    }
    file = {"file":(FILE_NAME, open(FILE_PATH, 'rb'))}
    response = test_client.post("/commission-data", data=form_body, files=file)
    hit = False
    match response.json():
        case {
            "jsonapi": a, 
            "meta": b, 
            "included": c, 
            "data": {
                "id": id, 
                "type": "submissions",
            }
        }:
            hit = True

    assert hit, f"{response.json()}"