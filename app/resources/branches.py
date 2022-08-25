from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from services.api_adapter import ApiAdapter

api = ApiAdapter()
router = APIRouter(prefix="/branches")

class Branch(BaseModel):
    customer_id: int
    city_id: int
    state_id: int

@router.post("/", tags=['branches'])
async def new_branch_by_customer_id(new_branch: Branch):
    existing_branches = api.get_customer_branches_raw(new_branch.customer_id)
    existing_branch = existing_branches[
        (existing_branches["customer_id"] == new_branch.customer_id)
        & (existing_branches["city_id"] == new_branch.city_id)
        & (existing_branches["state_id"] == new_branch.state_id)
    ]
    
    if existing_branch.empty:
        return api.set_new_customer_branch_raw(**new_branch.dict())
    elif existing_branch["deleted"].squeeze():
        return api.reactivate_branch(int(existing_branch["id"].squeeze()))
    existing_id = int(existing_branch["id"].squeeze())
    raise HTTPException(status_code=400, detail=f"Customer Branch already exists with id {existing_id}")

@router.delete("/{branch_id}", tags=['branches'])
async def delete_branch_by_id(branch_id: int):
    return api.delete_a_branch_by_id(branch_id=branch_id)