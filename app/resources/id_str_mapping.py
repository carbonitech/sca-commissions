from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from services.api_adapter import ApiAdapter, get_db, User, get_user
from jsonapi.jsonapi import convert_to_jsonapi, Query, JSONAPIRoute
from jsonapi.request_models import RequestModels

api = ApiAdapter()
router = APIRouter(prefix="/mappings", route_class=JSONAPIRoute)

@router.get("", tags=["mappings"])
async def all_mappings(query: Query=Depends(), db: Session=Depends(get_db), user: User=Depends(get_user)):
    jsonapi_query = convert_to_jsonapi(query)
    return api.get_mappings(db, jsonapi_query, user)

@router.get("/{mapping_id}", tags=["mappings"])
async def mapping_by_id(mapping_id: int, query: Query=Depends(), db: Session=Depends(get_db), user: User=Depends(get_user)):
    jsonapi_query = convert_to_jsonapi(query)
    return api.get_mappings(db, jsonapi_query, user, _id=mapping_id)

@router.post("", tags=['mappings'])
async def new_mapping(jsonapi_obj: RequestModels.new_mapping, db: Session=Depends(get_db), user: User=Depends(get_user)):
    return api.create_mapping(db=db, json_data=jsonapi_obj.dict(), user=user)

@router.delete("/{mapping_id}", tags=['mappings'])
async def delete_mapping_by_id(mapping_id: int, db: Session=Depends(get_db), user: User=Depends(get_user)):
    # soft delete
    return api.delete_mapping(db, mapping_id=mapping_id)