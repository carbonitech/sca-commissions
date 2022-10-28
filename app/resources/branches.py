from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from services.api_adapter import ApiAdapter,get_db
from app.jsonapi import convert_to_jsonapi, Query, JSONAPIRoute, BranchModificationRequest

api = ApiAdapter()
router = APIRouter(prefix="/branches", route_class=JSONAPIRoute)

@router.get("", tags=["branches"])
async def all_branches(query: Query=Depends(), db: Session=Depends(get_db)):
    jsonapi_query = convert_to_jsonapi(query)
    return api.get_many_branches_jsonapi(db,jsonapi_query)

@router.get("/{branch_id}", tags=["branches"])
async def branch_by_id(branch_id: int, query: Query=Depends(), db: Session=Depends(get_db)):
    jsonapi_query = convert_to_jsonapi(query)
    return api.get_branch(db, branch_id, jsonapi_query)

@router.patch("/{branch_id}", tags=["branches"])
async def modify_branch(branch_id: int, branch: BranchModificationRequest, db: Session=Depends(get_db)):
    return api.modify_branch(db, branch_id, branch.dict())

@router.post("", tags=['branches'], status_code=204)
async def new_branch_by_customer_id():
    return

@router.delete("/{branch_id}", tags=['branches'])
async def delete_branch_by_id(branch_id: int, db: Session=Depends(get_db)):
    # soft delete
    return api.delete_a_branch(db, branch_id=branch_id)