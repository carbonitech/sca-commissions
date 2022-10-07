from fastapi.testclient import TestClient
from app.main import app

test_client = TestClient(app)

def test_no_token():
    response = test_client.get("/commission-data")
    test_result = False
    match response.json():
        case {"errors": [{"status": 403, **error_objs}]}:
            test_result = True
    
    assert test_result, f"response is {response.json()}"
