"""Access locations table provided by Geonames. 
    No modification of the table is allowed, so only GET methods are allowed"""
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from services.api_adapter import ApiAdapter, get_db, User, get_user
from jsonapi.jsonapi import convert_to_jsonapi, Query, JSONAPIRoute


api = ApiAdapter()
router = APIRouter(prefix="/locations", route_class=JSONAPIRoute)

@router.get("", tags=["locations"])
async def all_locations(
        query: Query=Depends(),
        db: Session=Depends(get_db),
        user: User=Depends(get_user)
    ):
    jsonapi_query = convert_to_jsonapi(query)
    return api.get_location(db,jsonapi_query,user)

@router.get("/{location_id}", tags=["locations"])
async def all_locations(
        location_id: int,
        query: Query=Depends(),
        db: Session=Depends(get_db),
        user: User=Depends(get_user)
    ):
    jsonapi_query = convert_to_jsonapi(query)
    return api.get_location(db,jsonapi_query,user,location_id)