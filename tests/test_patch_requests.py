from fastapi.testclient import TestClient
from json import dumps
from app.main import app
from tests.pytest_fixtures import set_overrides, clear_overrides, database, RequestBody

test_client = TestClient(app)

def test_customers_patch_returns_jsonapi_obj(database: database):
    set_overrides()
    body = RequestBody("customers", {"name": "SHASCO INC"})
    response = test_client.patch(f"/customers/{body.id}", data=dumps(body.dict()))
    hit = False
    match response.json():
        case {"jsonapi": a, "meta": b, "data": c}:
            hit = True

    assert hit, f"{response.json()}"

def test_customers_patch_bad_body(database: database):
    set_overrides()
    # ids different
    id_mismatch = False
    body = RequestBody("customers",{"name": "SHASCO INC"})
    response = test_client.patch(f"/customers/{body.id+1}", data=dumps(body.dict()))
    match response.json():
        case {"errors": [{"status": 400, 'detail': 'IDs do not match', **error_objs}]}:
            id_mismatch = True
    assert id_mismatch

    # invalid attribute
    attribute_nonexistant = False
    body = RequestBody("customers",{"names": "SHASCO INC"})
    response = test_client.patch(f"/customers/{body.id}", data=dumps(body.dict()))
    match response.json():
        case {"errors": [{"status": 422, 'detail': 'field required', 'field': 'name'}]}:
            attribute_nonexistant = True
    assert attribute_nonexistant

    # no body
    body = RequestBody("customers",{"name": "SHASCO INC"})
    empty_body = {}
    response = test_client.patch(f"/customers/{body.id}", data=dumps(empty_body))
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
    body = RequestBody("customers", {"name": "NEW AWESOME BUSINESS, LLC"})
    test_client.patch(f"/customers/{body.id}", data=dumps(body.dict()))
    hit = False
    try:
        new_name = database.execute("SELECT name FROM customers WHERE id = :id", {"id": body.id}).scalar()
        hit = True if new_name == "NEW AWESOME BUSINESS, LLC" else False
    finally:
        database.close() # need this to prevent the test from hanging indefinitely
    assert hit, f"name in database with id: {str(body.id)} is {str(new_name)}. Expected NEW AWESOME BUSINESS, LLC"


def test_customers_patch_records_new_name_in_mapping(database: database):
    set_overrides()
    new_value = "NEW AWESOME SIDE BUSINESS, LLC"
    body = RequestBody("customers",{"name": new_value})
    test_client.patch(f"/customers/{body.id}", data=dumps(body.dict()))
    hit = False
    try:
        new_mapping_name = database.execute("SELECT recorded_name FROM map_customer_names WHERE customer_id = :id ORDER BY id DESC LIMIT 1", {"id": body.id}).scalar()  
        hit = True if new_mapping_name == new_value else False
    finally:
        database.close() # need this to prevent the test from hanging indefinitely

    assert hit, f"last mapping name in database with customer_id: {str(body.id)} is {str(new_mapping_name)}. Expected {new_value}."

