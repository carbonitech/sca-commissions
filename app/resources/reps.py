from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.jsonapi import Query, convert_to_jsonapi, JSONAPIRoute

from services.api_adapter import ApiAdapter, get_db, User, get_user

api = ApiAdapter()
router = APIRouter(prefix="/representatives", route_class=JSONAPIRoute)

@router.get("", tags=["reps"])
async def all_reps(query: Query=Depends(), db: Session=Depends(get_db), user: User=Depends(get_user)):
    jsonapi_query = convert_to_jsonapi(query)
    return api.get_reps(db,jsonapi_query,user)

@router.get("/{rep_id}", tags=["reps"])
async def rep_by_id(rep_id: int, query: Query=Depends(), db: Session=Depends(get_db), user: User=Depends(get_user)):
    jsonapi_query = convert_to_jsonapi(query)
    return api.get_reps(db,rep_id,jsonapi_query,user)


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