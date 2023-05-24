from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from services import get, post, patch, delete
from jsonapi.jsonapi import convert_to_jsonapi, Query, JSONAPIRoute 
from jsonapi.request_models import RequestModels
from services.utils import User, get_db, get_user
from jsonapi.branch_models import BranchResponse

router = APIRouter(prefix="/branches", route_class=JSONAPIRoute)

@router.get("", tags=["branches"], response_model=BranchResponse)
async def all_branches(
        query: Query=Depends(),
        db: Session=Depends(get_db),
        user: User=Depends(get_user)
    ):
    jsonapi_query = convert_to_jsonapi(query)
    return get.branch(db,jsonapi_query, user)

@router.get("/{branch_id}", tags=["branches"], response_model=BranchResponse)
async def branch_by_id(
        branch_id: int,
        query: Query=Depends(),
        db: Session=Depends(get_db),
        user: User=Depends(get_user)
    ):
    jsonapi_query = convert_to_jsonapi(query)
    return get.branch(db, jsonapi_query, user, branch_id)

@router.patch("/{branch_id}", tags=["branches"])
async def modify_branch(
        branch_id: int,
        branch: RequestModels.branch_modification,
        db: Session=Depends(get_db),
        user: User=Depends(get_user)
    ):
    return patch.branch(db, branch_id, branch.dict(), user)

@router.post("", tags=['branches'])
async def new_branch(
        jsonapi_obj: RequestModels.new_branch,
        db: Session=Depends(get_db),
        user: User=Depends(get_user)
    ):
    return post.branch(db=db, json_data=jsonapi_obj.dict(), user=user)

@router.delete("/{branch_id}", tags=['branches'])
async def delete_branch_by_id(
        branch_id: int,
        db: Session=Depends(get_db),
        user: User=Depends(get_user)
    ):
    # soft delete
    return delete.branch(db, branch_id=branch_id)