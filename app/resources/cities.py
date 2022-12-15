from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from services.api_adapter import ApiAdapter, get_db, User, get_user
from app.jsonapi import Query, convert_to_jsonapi, JSONAPIRoute, RequestModels

api = ApiAdapter()
router = APIRouter(prefix="/cities", route_class=JSONAPIRoute)


@router.get("", tags=["cities"])
async def get_all_cities(query: Query=Depends(), db: Session=Depends(get_db), user: User=Depends(get_user)):
    jsonapi_query = convert_to_jsonapi(query)
    return api.get_many_cities_jsonapi(db,jsonapi_query, user)


@router.get("/{city_id}", tags=["cities"])
async def get_city_by_id(city_id: int, query: Query=Depends(), db: Session=Depends(get_db), user: User=Depends(get_user)):
    jsonapi_query = convert_to_jsonapi(query)
    return api.get_city_jsonapi(db, city_id, jsonapi_query, user)


@router.patch("/{city_id}", tags=["cities"])
async def modify_a_city():
    ...

@router.post("", tags=["cities"])
async def add_a_city(jsonapi_obj: RequestModels.new_city, db: Session=Depends(get_db), user: User=Depends(get_user)):
    return api.create_city(db=db, json_data=jsonapi_obj.dict(), user=user)

@router.delete("/{city_id}", tags=["cities"])
async def delete_a_city(city_id: int):
    # soft delete
    ...