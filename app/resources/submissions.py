from fastapi import APIRouter, HTTPException, Form, Depends
from sqlalchemy.orm import Session
from services.api_adapter import ApiAdapter, get_db
from app.jsonapi import Query, convert_to_jsonapi, JSONAPIRoute

api = ApiAdapter()
router = APIRouter(prefix="/submissions", route_class=JSONAPIRoute)

@router.get("", tags=["submissions"])
async def get_all_submissions(query: Query=Depends(), db: Session=Depends(get_db)):
    try:
        jsonapi_query = convert_to_jsonapi(query)
    except Exception as err:
        raise HTTPException(status_code=400,detail=str(err))
    return api.get_many_submissions_jsonapi(db,jsonapi_query)

@router.get("/{submission_id}", tags=["submissions"])
async def get_submission_by_id(submission_id: int, query: Query=Depends(), db: Session=Depends(get_db)):
    try:
        jsonapi_query = convert_to_jsonapi(query)
    except Exception as err:
        raise HTTPException(status_code=400,detail=str(err))
    return api.get_submission_jsonapi(db, submission_id, jsonapi_query)

@router.put("/{submission_id}", tags=["submissions"])
async def modify_submission_by_id():
    ...

@router.delete("/{submission_id}", tags=["submissions"])
async def delete_submission_by_id(submission_id: int):
    # hard delete
    # hard deletes commission data, errors, and steps along with it
    api.delete_submission(submission_id)