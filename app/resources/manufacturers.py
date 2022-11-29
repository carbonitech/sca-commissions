from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from services.api_adapter import ApiAdapter, get_db, User, get_user
from app.jsonapi import Query, convert_to_jsonapi, JSONAPIRoute

api = ApiAdapter()
router = APIRouter(prefix="/manufacturers", route_class=JSONAPIRoute)

@router.get("", tags=["manufacturers"])
async def all_manufacturers(query: Query=Depends(), db: Session=Depends(get_db), user: User=Depends(get_user)):
    jsonapi_query = convert_to_jsonapi(query)
    return api.get_many_manufacturers_jsonapi(db,jsonapi_query,user)

@router.get("/{manuf_id}", tags=["manufacturers"])
async def manufacturer_by_id(manuf_id: int, query: Query=Depends(), db: Session=Depends(get_db), user: User=Depends(get_user)):
    jsonapi_query = convert_to_jsonapi(query)
    return api.get_manufacturer_jsonapi(db,manuf_id,jsonapi_query, user)
    
@router.post("/", tags=["manufacturers"])
async def add_a_manufacturer():
    ...

@router.delete("/{manuf_id}", tags=["manufacturers"])
async def delete_manufacturer_by_id(manuf_id: int):
    ...