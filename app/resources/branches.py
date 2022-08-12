from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from db import db_services

db = db_services.DatabaseServices()
router = APIRouter(prefix="/branches")

class Branch(BaseModel):
    customer_id: int
    city_id: int
    state_id: int

@router.post("/", tags=['branches'])
async def new_branch_by_customer_id(new_branch: Branch):
    existing_branches = db.get_customer_branches_raw(new_branch.customer)
    exist_check = existing_branches[
        (existing_branches["customer_id"] == new_branch.customer_id)
        & (existing_branches["city_id"] == new_branch.city_id)
        & (existing_branches["state_id"] == new_branch.state_id)
    ].empty
    if exist_check:
        return db.set_new_customer_branch_raw(**new_branch.dict())
    else:
        raise HTTPException(status_code=400, detail="Customer Branch already exists")

@router.delete("/{branch_id}", tags=['branches'])
async def delete_branch_by_id(branch_id: int):
    return db.delete_a_branch_by_id(branch_id=branch_id)