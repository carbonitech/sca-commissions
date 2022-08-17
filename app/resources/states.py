from fastapi import APIRouter, HTTPException, Form
from pydantic import BaseModel, validator
import json
from services.api_adapter import ApiAdapter

api = ApiAdapter()
router = APIRouter(prefix="/states")

class State(BaseModel):
    name: str
    @validator('name')
    def name_upper(cls,val:str):
        return val.strip().upper()


@router.get("/", tags=["states"])
async def get_all_states():
    states = api.get_states().to_json(orient="records",date_format="iso")
    return json.loads(states)

@router.get("/{state_id}", tags=["states"])
async def get_a_state(state_id: int):
    states = api.get_states(state_id).to_json(orient="records",date_format="iso")
    return json.loads(states)

@router.post("/", tags=["states"])
async def add_a_state(new_state: State):
    existing_states = api.get_states()
    existing_state = existing_states.loc[
        existing_states.name == new_state.name
    ]
    if not existing_state.empty:
        raise HTTPException(400, detail=f"State already exists with id {existing_state.id.squeeze()}")
    return {"new id": api.set_new_state(**new_state.dict())}

@router.put("/{state_id}", tags=["states"])
async def modify_a_state(state_id: int, state_info: State):
    existing_state = api.get_states(state_id)
    if existing_state.empty:
        raise HTTPException(400, detail=f"State with id {state_id} does not exists")
    api.modify_state(state_id, **state_info.dict())

@router.delete("/{state_id}", tags=["states"])
async def delete_a_state(state_id: int):
    # soft delete
    api.delete_state(state_id)