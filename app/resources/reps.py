from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.jsonapi import Query, convert_to_jsonapi, JSONAPIRoute

from services.api_adapter import ApiAdapter, get_db

api = ApiAdapter()
router = APIRouter(prefix="/representatives", route_class=JSONAPIRoute)

@router.get("", tags=["reps"])
async def all_reps(query: Query=Depends(), db: Session=Depends(get_db)):
    try:
        jsonapi_query = convert_to_jsonapi(query)
    except Exception as err:
        raise HTTPException(status_code=400,detail=str(err))
    return api.get_many_reps_jsonapi(db,jsonapi_query)

@router.get("/{rep_id}", tags=["reps"])
async def rep_by_id(rep_id: int, query: Query=Depends(), db: Session=Depends(get_db)):
    try:
        jsonapi_query = convert_to_jsonapi(query)
    except Exception as err:
        raise HTTPException(status_code=400,detail=str(err))
    return api.get_rep_jsonapi(db, rep_id, jsonapi_query)


@router.post("", tags=["reps"])
async def add_new_rep():
    ...

@router.put("/{rep_id}", tags=["reps"])
async def modify_a_rep():
    ...

@router.delete("/{rep_id}", tags=["reps"])
async def delete_rep(rep_id: int):
    # soft delete
    ...