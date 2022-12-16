from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from services.api_adapter import ApiAdapter, get_db, User, get_user
from app.jsonapi import Query, convert_to_jsonapi, JSONAPIRoute, RequestModels

api = ApiAdapter()
router = APIRouter(prefix="/states", route_class=JSONAPIRoute)

@router.get("", tags=["states"])
async def all_states(query: Query=Depends(), db: Session=Depends(get_db), user: User=Depends(get_user)):
    jsonapi_query = convert_to_jsonapi(query)
    return api.get_many_states_jsonapi(db,jsonapi_query,user)

@router.get("/{state_id}", tags=["states"])
async def state_by_id(state_id: int, query: Query=Depends(), db: Session=Depends(get_db), user: User=Depends(get_user)):
    jsonapi_query = convert_to_jsonapi(query)
    return api.get_state_jsonapi(db,state_id,jsonapi_query,user)

@router.post("", tags=["states"])
async def add_a_state(jsonapi_obj: RequestModels.new_state, db: Session=Depends(get_db), user: User=Depends(get_user)):
    api.create_state(db=db, json_data=jsonapi_obj.dict(), user=user)

@router.put("/{state_id}", tags=["states"])
async def modify_a_state():
    ...

@router.delete("/{state_id}", tags=["states"])
async def delete_a_state(state_id: int):
    # soft delete
    ...