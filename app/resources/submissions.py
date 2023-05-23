from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from services import get, post, patch, delete
from jsonapi.jsonapi import Query, convert_to_jsonapi, JSONAPIRoute
from jsonapi.request_models import RequestModels
import json
import math

from services.utils import User, get_db, get_user

router = APIRouter(prefix="/submissions", route_class=JSONAPIRoute)

@router.get("", tags=["submissions"])
async def get_all_submissions(
        query: Query=Depends(),
        db: Session=Depends(get_db),
        user: User=Depends(get_user)
    ):
    jsonapi_query = convert_to_jsonapi(query)
    return get.submissions(db,jsonapi_query,user)

@router.get("/{submission_id}", tags=["submissions"])
async def get_submission_by_id(
        submission_id: int,
        query: Query=Depends(),
        db: Session=Depends(get_db),
        user: User=Depends(get_user)
    ):
    jsonapi_query = convert_to_jsonapi(query)
    raw_result = get.submissions(db,jsonapi_query,user,submission_id)
    
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

@router.patch("/{submission_id}", tags=["submissions"])
async def modify_submission_by_id(
        submission_id: int,
        submission: RequestModels.submission_modification,
        db: Session=Depends(get_db),
        user: User=Depends(get_user)
    ):
    return patch.submission(db, submission_id, submission.dict(), user)

@router.delete("/{submission_id}", tags=["submissions"])
async def delete_submission_by_id(
        submission_id: int,
        db: Session=Depends(get_db),
        user: User=Depends(get_user)
    ):
    # hard deletes commission data and errors along with it
    delete.submission(submission_id, session=db, user=user)