from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from services import get, post, patch, delete
from jsonapi.jsonapi import convert_to_jsonapi, Query, JSONAPIRoute
from jsonapi.request_models import RequestModels
from services.utils import User, get_db, get_user

router = APIRouter(prefix="/mappings", route_class=JSONAPIRoute)

@router.get("", tags=["mappings"])
async def all_mappings(
        query: Query=Depends(),
        db: Session=Depends(get_db),
        user: User=Depends(get_user)
    ):
    jsonapi_query = convert_to_jsonapi(query)
    return get.mappings(db, jsonapi_query, user)

@router.get("/{mapping_id}", tags=["mappings"])
async def mapping_by_id(
        mapping_id: int,
        query: Query=Depends(),
        db: Session=Depends(get_db),
        user: User=Depends(get_user)
    ):
    jsonapi_query = convert_to_jsonapi(query)
    return get.mappings(db, jsonapi_query, user, _id=mapping_id)

@router.post("", tags=['mappings'])
async def new_mapping(
        jsonapi_obj: RequestModels.new_mapping,
        db: Session=Depends(get_db),
        user: User=Depends(get_user)
    ):
    return post.mapping(db=db, json_data=jsonapi_obj.dict(), user=user)

@router.delete("/{mapping_id}", tags=['mappings'])
async def delete_mapping_by_id(
        mapping_id: int,
        db: Session=Depends(get_db),
        user: User=Depends(get_user)
    ):
    return delete.mapping(db, mapping_id=mapping_id)