from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from services.api_adapter import ApiAdapter, get_db
from app.jsonapi import Query, convert_to_jsonapi, JSONAPIRoute

api = ApiAdapter()
router = APIRouter(prefix="/cities", route_class=JSONAPIRoute)

@router.get("", tags=["cities"])
async def get_all_cities(query: Query=Depends(), db: Session=Depends(get_db)):
    try:
        jsonapi_query = convert_to_jsonapi(query)
    except Exception as err:
        raise HTTPException(status_code=400,detail=str(err))
    cities = api.get_many_cities_jsonapi(db,jsonapi_query)
    return cities

@router.get("/{city_id}", tags=["cities"])
async def get_city_by_id(city_id: int, query: Query=Depends(), db: Session=Depends(get_db)):
    try:
        jsonapi_query = convert_to_jsonapi(query)
    except Exception as err:
        raise HTTPException(status_code=400,detail=str(err))
    customer = api.get_city_jsonapi(db, city_id, jsonapi_query)
    return customer

@router.post("/", tags=["cities"])
async def add_a_city():
    ...

@router.put("/{city_id}", tags=["cities"])
async def modify_a_city():
    ...

@router.delete("/{city_id}", tags=["cities"])
async def delete_a_city(city_id: int):
    # soft delete
    ...