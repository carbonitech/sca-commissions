from fastapi.testclient import TestClient
from random import randint
from json import dumps
from app.main import app
from tests.pytest_fixtures import set_overrides, clear_overrides, database


test_client = TestClient(app)


def test_customers_patch(database: database):
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
