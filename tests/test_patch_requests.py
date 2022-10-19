from fastapi.testclient import TestClient
from random import randint
from json import dumps
from app.main import app
from tests.pytest_fixtures import set_overrides, clear_overrides, database


test_client = TestClient(app)


def test_customers_patch_returns_jsonapi_obj(database: database):
    set_overrides()
    test_id = randint(1,50)
    request_body = {
        "data": {
            "type": "customers",
            "id": test_id,
            "attributes": {
                "name": "SHASCO INC"
            }
        }
    }
    response = test_client.patch(f"/customers/{test_id}", data=dumps(request_body))
    hit = False
    match response.json():
        case {"jsonapi": a, "meta": b, "data": c}:
            hit = True

    assert hit, f"{response.json()}"

def test_customers_patch_bad_body(database: database):
    set_overrides()
    test_id = randint(1,50)
    
    # ids different
    id_mismatch = False
    request_body = {
        "data": {
            "type": "customers",
            "id": test_id,
            "attributes": {
                "name": "SHASCO INC"
            }
        }
    }
    response = test_client.patch(f"/customers/{test_id+1}", data=dumps(request_body))
    match response.json():
        case {"errors": [{"status": 400, 'detail': 'IDs do not match', **error_objs}]}:
            id_mismatch = True
    assert id_mismatch

    # invalid attribute
    attribute_nonexistant = False
    request_body = {
        "data": {
            "type": "customers",
            "id": test_id,
            "attributes": {
                "names": "SHASCO INC"
            }
        }
    }
    response = test_client.patch(f"/customers/{test_id}", data=dumps(request_body))
    match response.json():
        case {"errors": [{"status": 422, 'detail': 'field required', 'field': 'name'}]}:
            attribute_nonexistant = True
    assert attribute_nonexistant

    # no body
    request_body = {}
    response = test_client.patch(f"/customers/{test_id}", data=dumps(request_body))
    match response.json():
        case {"errors": [*error_objs]}:
            no_body = [False for x in range(len(error_objs))]
            for i, error in enumerate(error_objs):
                match error:
                    case {"status": 422, "detail": "field required", "field": field}:
                        no_body[i] = True
    assert all(no_body)

def test_customers_patch_makes_change_in_target(database: database):
    set_overrides()
    test_id = randint(1,50)
    request_body = {
        "data": {
            "type": "customers",
            "id": test_id,
            "attributes": {
                "name": "NEW AWESOME BUSINESS, LLC"
            }
        }
    }
    test_client.patch(f"/customers/{test_id}", data=dumps(request_body))
    hit = False
    try:
        new_name = database.execute("SELECT name FROM customers WHERE id = :id", {"id": test_id}).scalar()
        hit = True if new_name == "NEW AWESOME BUSINESS, LLC" else False
    finally:
        database.close() # need this to prevent the test from hanging indefinitely
    assert hit, f"name in database with id: {str(test_id)} is {str(new_name)}. Expected NEW AWESOME BUSINESS, LLC"


def test_customers_patch_records_new_name_in_mapping(database: database):
    set_overrides()
    new_value = "NEW AWESOME SIDE BUSINESS, LLC"
    test_id = randint(1,50)
    request_body = {
        "data": {
            "type": "customers",
            "id": test_id,
            "attributes": {
                "name": new_value
            }
        }
    }
    test_client.patch(f"/customers/{test_id}", data=dumps(request_body))
    hit = False
    try:
        new_mapping_name = database.execute("SELECT recorded_name FROM map_customer_name WHERE customer_id = :id ORDER BY id DESC LIMIT 1", {"id": test_id}).scalar()  
        hit = True if new_mapping_name == new_value else False
    finally:
        database.close() # need this to prevent the test from hanging indefinitely

    assert hit, f"last mapping name in database with customer_id: {str(test_id)} is {str(new_mapping_name)}. Expected {new_value}."