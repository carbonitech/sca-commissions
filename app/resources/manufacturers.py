from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from services import get, post, patch, delete
from jsonapi.jsonapi import Query, convert_to_jsonapi, JSONAPIRoute
from jsonapi.request_models import RequestModels
from services.utils import User, get_db, get_user

router = APIRouter(prefix="/manufacturers", route_class=JSONAPIRoute)

@router.get("", tags=["manufacturers"])
async def all_manufacturers(
        query: Query=Depends(),
        db: Session=Depends(get_db),
        user: User=Depends(get_user)
    ):
    jsonapi_query = convert_to_jsonapi(query)
    return get.manufacturers(db,jsonapi_query,user)

@router.get("/{manuf_id}", tags=["manufacturers"])
async def manufacturer_by_id(
        manuf_id: int,
        query: Query=Depends(),
        db: Session=Depends(get_db),
        user: User=Depends(get_user)
    ):
    jsonapi_query = convert_to_jsonapi(query)
    return get.manufacturers(db,jsonapi_query,user,manuf_id)
    
@router.post("", tags=["manufacturers"])
async def add_a_manufacturer(
        jsonapi_obj: RequestModels.new_manufacturer,
        db: Session=Depends(get_db),
        user: User=Depends(get_user)
    ):
    return post.manufacturer(db,json_data=jsonapi_obj.dict(exclude_none=True), user=user)

@router.delete("/{manuf_id}", tags=["manufacturers"])
async def delete_manufacturer_by_id(
        manuf_id: int,
        db: Session=Depends(get_db),
        user: User=Depends(get_user)
    ):
    # soft delete
    return delete.manufacturer(db=db, manuf_id=manuf_id, user=user)