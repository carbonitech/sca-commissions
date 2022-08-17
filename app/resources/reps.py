import json
from datetime import datetime
from fastapi import APIRouter, HTTPException, Form
from pydantic import BaseModel, validator

from services.api_adapter import ApiAdapter

api = ApiAdapter()
router = APIRouter(prefix="/reps")

class Representative(BaseModel):
    first_name: str
    last_name: str
    initials: str
    date_joined: datetime = datetime.now()

    @validator('last_name', allow_reuse=True)
    @validator('first_name', allow_reuse=True)
    def name_uppercase(cls, value: str):
        return value.strip().upper()


@router.get("/", tags=["reps"])
async def get_all_reps():
    all_reps = api.get_all_reps().to_json(orient="records", date_format="iso")
    return {"data": json.loads(all_reps)}

@router.get("/{rep_id}",tags=["reps"])
async def get_rep_by_id(rep_id: int):
    rep_and_branches = api.get_rep_and_branches(rep_id).to_json(orient="records")
    return {"data": json.loads(rep_and_branches)}

@router.post("/", tags=["reps"])
async def add_new_rep(new_rep: Representative):
    existing_reps = api.get_all_reps()
    existing_rep = existing_reps.loc[
        (existing_reps.first_name == new_rep.first_name)
        &(existing_reps.last_name == new_rep.last_name),"id"
    ]
    if not existing_rep.empty:
        raise HTTPException(400, f"rep already exists with id {existing_rep.squeeze()}")
    return {"new id": api.set_new_rep(**new_rep.dict())}

@router.put("/{rep_id}", tags=["reps"])
async def modify_a_rep(rep_id: int, representative: Representative):
    raise NotImplementedError

@router.delete("/{rep_id}", tags=["reps"])
async def delete_rep(rep_id: int):
    # soft delete
    api.delete_rep(rep_id)