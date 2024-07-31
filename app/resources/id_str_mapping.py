from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from services import get, post, patch, delete
from jsonapi.jsonapi import convert_to_jsonapi, Query, JSONAPIRoute
from jsonapi.request_models import RequestModels
from services.utils import User, get_db, get_user

router = APIRouter(prefix="/mappings", route_class=JSONAPIRoute, tags=["mappings"])


@router.get("")
async def all_mappings(
    query: Query = Depends(),
    db: Session = Depends(get_db),
    user: User = Depends(get_user),
):
    jsonapi_query = convert_to_jsonapi(query)
    return get.mappings(db, jsonapi_query, user)


@router.get("/{mapping_id}")
async def mapping_by_id(
    mapping_id: int,
    query: Query = Depends(),
    db: Session = Depends(get_db),
    user: User = Depends(get_user),
):
    jsonapi_query = convert_to_jsonapi(query)
    return get.mappings(db, jsonapi_query, user, _id=mapping_id)


@router.post("")
async def new_mapping(
    jsonapi_obj: RequestModels.new_mapping,
    db: Session = Depends(get_db),
    user: User = Depends(get_user),
):
    return post.mapping(db=db, json_data=jsonapi_obj.dict(), user=user)


@router.patch("/{mapping_id}")
async def change_mapping(
    mapping_id: int,
    jsonapi_obj: RequestModels.mapping_modification,
    db: Session = Depends(get_db),
    user: User = Depends(get_user),
):
    additions: dict = {"verified": True, "model_successful": False}
    modified_obj = jsonapi_obj.model_dump(exclude_unset=True, exclude_none=True)
    modified_obj["data"]["attributes"] = additions
    resp_obj = patch.mapping(db, mapping_id, modified_obj, user)
    branch_id_in_patch = jsonapi_obj.data.relationships.branches.data.id
    patch.change_commission_data_customer_branches(db, mapping_id, branch_id_in_patch)
    return resp_obj


@router.delete("/{mapping_id}")
async def delete_mapping_by_id(
    mapping_id: int, db: Session = Depends(get_db), user: User = Depends(get_user)
):
    return delete.mapping(db, mapping_id=mapping_id)
