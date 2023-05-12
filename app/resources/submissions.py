from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from services.api_adapter import ApiAdapter, get_db, User, get_user
from jsonapi.jsonapi import Query, convert_to_jsonapi, JSONAPIRoute
import json
import math

api = ApiAdapter()
router = APIRouter(prefix="/submissions", route_class=JSONAPIRoute)

@router.get("", tags=["submissions"])
async def get_all_submissions(query: Query=Depends(), db: Session=Depends(get_db), user: User=Depends(get_user)):
    jsonapi_query = convert_to_jsonapi(query)
    return api.get_submissions(db,jsonapi_query,user)

@router.get("/{submission_id}", tags=["submissions"])
async def get_submission_by_id(submission_id: int, query: Query=Depends(), db: Session=Depends(get_db), user: User=Depends(get_user)):
    jsonapi_query = convert_to_jsonapi(query)
    raw_result = api.get_submissions(db,jsonapi_query,user,submission_id)
    
    ## converting row-data from a string of a dict to an actual dict before sending back in the included object
    def convert_row_data_to_dict(json_obj: dict):
        if json_obj.get("type") != "errors":
            return json_obj
        row_data_dict: dict = json.loads(json_obj["attributes"]["row-data"])
        corrected_row_data = {}
        for k,v in row_data_dict.items():
            if isinstance(v, float) and math.isnan(v):
                continue
            corrected_row_data[k] = v
        json_obj["attributes"]["row-data"] = corrected_row_data
        return json_obj

    new_included = list(map(convert_row_data_to_dict, raw_result["included"]))
    raw_result.update({"included": new_included})
    return raw_result

@router.put("/{submission_id}", tags=["submissions"])
async def modify_submission_by_id():
    ...

@router.delete("/{submission_id}", tags=["submissions"])
async def delete_submission_by_id(submission_id: int, db: Session=Depends(get_db), user: User=Depends(get_user)):
    # hard delete
    # hard deletes commission data, errors, and steps along with it
    api.delete_submission(submission_id, session=db, user=user)