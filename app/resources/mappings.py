from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session
from app.jsonapi import JSONAPIRoute, NewCustomerNameMappingRequest, Query, convert_to_jsonapi, format_error, BaseError

from services.api_adapter import ApiAdapter, get_db

api = ApiAdapter()
router_customers = APIRouter(prefix="/map-customer-names", route_class=JSONAPIRoute)
router_cities = APIRouter(prefix="/map-city-names", route_class=JSONAPIRoute)
router_states = APIRouter(prefix="/map-state-names", route_class=JSONAPIRoute)

@router_customers.get("", tags=["mappings"])
async def map_customer_names(db: Session=Depends(get_db), query: Query=Depends()):
    try:
        jsonapi_query = convert_to_jsonapi(query)
        return api.get_all_customer_name_mappings(db,jsonapi_query)
    except BaseError as err:
        raise HTTPException(**format_error(err))
    except Exception as err:
        raise HTTPException(status_code=400,detail=str(err))

@router_customers.get("/{map_customer_name_id}", tags=["mappings"])
async def map_customer_names(map_customer_name_id: int, db: Session=Depends(get_db), query: Query=Depends()):
    try:
        jsonapi_query = convert_to_jsonapi(query)
        return api.get_customer_name_mapping_by_id(db,map_customer_name_id,jsonapi_query)
    except BaseError as err:
        raise HTTPException(**format_error(err))
    except Exception as err:
        raise HTTPException(status_code=400,detail=str(err))

@router_customers.post("", tags=['mappings'])
async def new_map_customer_name(name_mapping: NewCustomerNameMappingRequest, db: Session=Depends(get_db)):
    return api.create_customer_name_mapping(db=db, json_data=name_mapping.dict())