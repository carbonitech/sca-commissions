from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.jsonapi import JSONAPIRoute, RequestModels, Query, convert_to_jsonapi

from services.api_adapter import ApiAdapter, get_db, User, get_user

api = ApiAdapter()
router_customers = APIRouter(prefix="/map-customer-names", route_class=JSONAPIRoute)
router_cities = APIRouter(prefix="/map-city-names", route_class=JSONAPIRoute)
router_states = APIRouter(prefix="/map-state-names", route_class=JSONAPIRoute)

@router_customers.get("", tags=["mappings"])
async def map_customer_names(db: Session=Depends(get_db), query: Query=Depends(), user: User=Depends(get_user)):
    jsonapi_query = convert_to_jsonapi(query)
    return api.get_all_customer_name_mappings(db,jsonapi_query,user)

@router_customers.get("/{map_customer_name_id}", tags=["mappings"])
async def map_customer_name(map_customer_name_id: int, db: Session=Depends(get_db), query: Query=Depends(), user: User=Depends(get_user)):
    jsonapi_query = convert_to_jsonapi(query)
    return api.get_customer_name_mapping_by_id(db,map_customer_name_id,jsonapi_query,user)

@router_customers.post("", tags=['mappings'])
async def new_map_customer_name(name_mapping: RequestModels.new_customer_name_mapping, db: Session=Depends(get_db), user: User=Depends(get_user)):
    return api.create_customer_name_mapping(db=db, json_data=name_mapping.dict(), user=user)

@router_customers.delete("/{id}", tags=['mappings'])
async def delete_map_customer_name(id: int, db: Session=Depends(get_db), user: User=Depends(get_user)):
    return api.delete_map_customer_name(db=db, id_=id, user=user)


@router_cities.get("", tags=["mappings"])
async def map_city_names(db: Session=Depends(get_db), query: Query=Depends(), user: User=Depends(get_user)):
    jsonapi_query = convert_to_jsonapi(query)
    return api.get_city_name_mappings(db,jsonapi_query,user)

@router_cities.get("/{map_city_name_id}", tags=["mappings"])
async def map_city_name(map_city_name_id: int, db: Session=Depends(get_db), query: Query=Depends(), user: User=Depends(get_user)):
    jsonapi_query = convert_to_jsonapi(query)
    return api.get_city_name_mappings(db,jsonapi_query,user,map_city_name_id)

@router_cities.post("", tags=["mappings"])
async def new_map_city_name(name_mapping: RequestModels.new_city_name_mapping, db: Session=Depends(get_db), user: User=Depends(get_user)):
    return api.create_city_name_mapping(db=db, json_data=name_mapping.dict(), user=user)

@router_cities.delete("/{id}", tags=["mappings"])
async def delete_map_city_name(id: int, db: Session=Depends(get_db), user: User=Depends(get_user)):
    return api.delete_map_city_name(db=db, id_=id, user=user)


@router_states.get("", tags=["mappings"])
async def map_state_names(db: Session=Depends(get_db), query: Query=Depends(), user: User=Depends(get_user)):
    jsonapi_query = convert_to_jsonapi(query)
    return api.get_state_name_mappings(db,jsonapi_query,user)

@router_states.get("/{map_state_name_id}", tags=["mappings"])
async def map_state_name(map_state_name_id: int, db: Session=Depends(get_db), query: Query=Depends(), user: User=Depends(get_user)):
    jsonapi_query = convert_to_jsonapi(query)
    return api.get_state_name_mappings(db,jsonapi_query,user,map_state_name_id)

@router_states.post("", tags=["mappings"])
async def new_map_state_name(name_mapping: RequestModels.new_state_name_mapping, db: Session=Depends(get_db), user: User=Depends(get_user)):
    return api.create_state_name_mapping(db=db, json_data=name_mapping.dict(), user=user)

@router_states.delete("/{id}", tags=["mappings"])
async def delete_map_state_name(id: int, db: Session=Depends(get_db), user: User=Depends(get_user)):
    return api.delete_map_state_name(db=db, id_=id, user=user)