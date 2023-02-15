from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from services.api_adapter import ApiAdapter, get_db, User, get_user
from app.jsonapi import convert_to_jsonapi, Query, JSONAPIRoute, BranchModificationRequest, RequestModels

api = ApiAdapter()
router = APIRouter(prefix="/branches", route_class=JSONAPIRoute)

@router.get("", tags=["branches"])
async def all_branches(query: Query=Depends(), db: Session=Depends(get_db), user: User=Depends(get_user)):
    jsonapi_query = convert_to_jsonapi(query)
    return api.get_many_branches_jsonapi(db,jsonapi_query, user)

@router.get("/{branch_id}", tags=["branches"])
async def branch_by_id(branch_id: int, query: Query=Depends(), db: Session=Depends(get_db), user: User=Depends(get_user)):
    jsonapi_query = convert_to_jsonapi(query)
    return api.get_branch(db, branch_id, jsonapi_query, user)

@router.patch("/{branch_id}", tags=["branches"])
async def modify_branch(branch_id: int, branch: BranchModificationRequest, db: Session=Depends(get_db), user: User=Depends(get_user)):
    return api.modify_branch(db, branch_id, branch.dict(), user)

@router.post("", tags=['branches'])
async def new_branch(jsonapi_obj: RequestModels.new_branch, db: Session=Depends(get_db), user: User=Depends(get_user)):
    return api.create_branch(db=db, json_data=jsonapi_obj.dict(), user=user)

@router.delete("/{branch_id}", tags=['branches'])
async def delete_branch_by_id(branch_id: int, db: Session=Depends(get_db), user: User=Depends(get_user)):
    # soft delete
    return api.delete_a_branch(db, branch_id=branch_id)