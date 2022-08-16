from fastapi import APIRouter, HTTPException, Form
from pydantic import BaseModel
import json
from services.api_adapter import ApiAdapter

api = ApiAdapter()
router = APIRouter(prefix="/states")

@router.get("/", tags=["states"])
async def get_all_states():
    raise NotImplementedError

@router.post("/", tags=["states"])
async def add_a_state(state_name: str):
    state_name = state_name.upper()
    raise NotImplementedError

@router.put("/{state_id}", tags=["states"])
async def modify_a_state(state_id: int):
    raise NotImplementedError

@router.delete("/{state_id}", tags=["states"])
async def delete_a_state(state_id: int):
    raise NotImplementedError