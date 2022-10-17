from fastapi.testclient import TestClient
from app.main import app, authenticate_auth0_token
from random import choices
from string import ascii_letters, digits, punctuation

test_client = TestClient(app)

def test_no_token():
    response = test_client.get("/commission-data")
    test_result = False
    match response.json():
        case {"errors": [{"status": 403, **error_objs}]}:
            test_result = True
    
    assert test_result, f"response is {response.json()}"

def test_random_string_as_token():
    random_token = "".join(choices(ascii_letters+digits+punctuation, k=1000))
    test_client.headers.update(
        {"Authorization": f"Bearer {random_token}"}
        )
    response = test_client.get("/commission-data")
    test_result = False
    match response.json():
        case {"errors": [{"status": 401, **error_objs}]}:
            test_result = True
    assert test_result, f"response is {response.json()}"