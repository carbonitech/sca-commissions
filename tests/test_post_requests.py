from fastapi.testclient import TestClient
from app.main import app
from json import dumps
from tests.pytest_fixtures import (
        set_overrides, clear_overrides, database, 
        RequestBody, RequestBodyRelationship)

test_client = TestClient(app)

def test_new_customer_name_mapping(database: database):
    set_overrides()
    resource = "map-customer-names"
    relationship = RequestBodyRelationship("customers")
    attributes = {"recorded_name": "DEALERS SUPPLY COMP"}
    body = RequestBody(resource,attributes,relationship)

    response = test_client.post(f"/{resource}", data=dumps(body.dict()))
    clear_overrides()

    successful_response = False
    match response.json():
        case {"jsonapi": a, "meta": b, "data": c}:
            successful_response = True

    assert successful_response, f"{response.json()}"