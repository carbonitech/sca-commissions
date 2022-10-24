from app.main import app
from fastapi.testclient import TestClient
from tests.pytest_fixtures import set_overrides, clear_overrides, database
from random import randint

test_client = TestClient(app)

def test_delete_map_customer_name(database: database):
    set_overrides()
    id_to_del = randint(1,100)
    response_no_id = test_client.delete(f"/map-customer-names")
    response_w_id = test_client.delete(f"/map-customer-names/{id_to_del}")
    clear_overrides()

    expected_response = False
    match response_w_id.json():
        case {"status_code": 204, "data": {"jsonapi": a, "meta": b}}:
            expected_response = True
    assert expected_response

    expected_response = False
    match response_no_id.json():
        case {"errors": [{"detail": msg, "status": 405}]}:
            expected_response = True
    assert expected_response, f"{response_no_id.json()}"


