from pydantic import BaseModel
import json

class Query(BaseModel):
    include: str|None = None
    sort: str|None = None
    fields: str|None = None
    filter: str|None = None
    page: str|None = None

def convert_to_jsonapi(query: dict) -> dict:
    jsonapi_query = {}
    bracketed_params = ['fields','filter','page']
    for param_name, param_value in query.dict(exclude_none=True).items():
        param_name: str
        param_value: str
        if param_name in bracketed_params:
            param_value_dict: dict = json.loads(param_value)
            jsonapi_query.update({f"{param_name}[{param}]":value for param,value in param_value_dict.items()})
        else:
            jsonapi_query[param_name] = param_value
    return jsonapi_query