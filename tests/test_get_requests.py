from fastapi.testclient import TestClient
from app.main import app
from tests.pytest_fixtures import set_overrides, clear_overrides, database

test_client = TestClient(app)

def test_jsonapi_object_response_from_all_top_level(database: database):
    resources = ["customers","cities","states","commission-data","manufacturers","representatives","submissions"]
    for resource in resources:
        set_overrides()
        response = test_client.get(f"/{resource}")
        clear_overrides()
        hit = False
        match response.status_code, response.json():
            case 200, {"jsonapi": version, "meta": metadata, "data": jsonapi_obj}:
                hit = True
        fail_msg = f"Object returned is by /{resource} is not a JSON:API Object with a 200 status code."\
            f"Response contained {response.json()} with status code {response.status_code}"
        assert hit, fail_msg
    
def test_get_customers_with_filter_parameter(database: database):
    set_overrides()
    filter_field = "name"
    filter_value = "DEA"
    filter_param = 'filter={"'+filter_field+'": "'+filter_value+'"}'
    response = test_client.get(f"/customers?{filter_param}")
    clear_overrides()
    
    for data_obj in response.json().get("data"):
        assert filter_field in data_obj["attributes"].keys(), f"Attribute \"{filter_field}\" is not present in object with id {data_obj['id']}"
        assert filter_value in data_obj["attributes"].get(filter_field), f"Filter value \"{filter_value}\" is not present in object with id {data_obj['id']}"

def test_jsonapi_param_handling(database: database):
    set_overrides()
    filter_param = "filter[name] = MINGLEDORFF"
    page_params = "page[size]=5&page[number]=1"
    headers_content_type = {"Content-Type": "application/vnd.api+json"}

    response = test_client.get(f"/customers?{page_params}", headers=headers_content_type)
    assert len(response.json()["data"]) == 5

    response = test_client.get(f"/customers?{filter_param}", headers=headers_content_type)
    # assert len(response.json()["data"]) == 1, f"{response.json()}"
    assert response.json()["data"][0]["attributes"]["name"].startswith("MINGLEDORFF")


    
