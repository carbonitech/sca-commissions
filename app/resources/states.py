from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from services.api_adapter import ApiAdapter, get_db
from app.jsonapi import Query, convert_to_jsonapi, JSONAPIRoute

api = ApiAdapter()
router = APIRouter(prefix="/states", route_class=JSONAPIRoute)

@router.get("", tags=["states"])
async def all_states(query: Query=Depends(), db: Session=Depends(get_db)):
    try:
        jsonapi_query = convert_to_jsonapi(query)
    except Exception as err:
        raise HTTPException(status_code=400,detail=str(err))
    return api.get_many_states_jsonapi(db,jsonapi_query)

@router.get("/{state_id}", tags=["states"])
async def state_by_id(state_id: int, query: Query=Depends(), db: Session=Depends(get_db)):
    try:
        jsonapi_query = convert_to_jsonapi(query)
    except Exception as err:
        raise HTTPException(status_code=400,detail=str(err))
    return api.get_state_jsonapi(db, state_id, jsonapi_query)

@router.post("/", tags=["states"])
async def add_a_state():
    ...

@router.put("/{state_id}", tags=["states"])
async def modify_a_state():
    ...

@router.delete("/{state_id}", tags=["states"])
async def delete_a_state(state_id: int):
    # soft delete
    ...