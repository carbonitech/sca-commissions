from fastapi import APIRouter, HTTPException, Form
from pydantic import BaseModel
import json
from db import db_services

db = db_services.DatabaseServices()
router = APIRouter(prefix="/states")

@router.get("/", tags=["states"])
async def get_all_states():
    pass

@router.post("/", tags=["states"])
async def add_a_state(state_name: str):
    state_name = state_name.upper()
    pass

@router.put("/{state_id}", tags=["states"])
async def modify_a_state(state_id: int):
    pass

@router.delete("/{state_id}", tags=["states"])
async def delete_a_state(state_id: int):
    pass