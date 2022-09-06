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
async def add_a_state(new_state: str = Form()):
    # TODO ENABLE REACTIVATION OF DELETED STATE IF INPUT MATCHES
    existing_states = api.get_states()
    existing_state = existing_states.loc[
        existing_states.name == new_state
    ]
    if not existing_state.empty:
        raise HTTPException(400, detail=f"State already exists with id {existing_state.id.squeeze()}")
    value = {"name": new_state}
    return {"new id": api.set_new_state(**value)}

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


@router.get("/{state_id}/mappings", tags=["states"])
async def get_all_mappings_for_a_state(state_id: int):
    return json.loads(api.get_all_state_name_mappings(state_id).to_json(orient="records"))

@router.post("/{state_id}/mappings", tags=["states"])
async def create_new_mapping_for_a_state(state_id: int, new_mapping: str = Form()):
    current_state = not api.get_states(new_mapping.state_id).empty
    if not current_state:
        raise HTTPException(400, detail="State does not exist")
    value = {"state_id": state_id, "recorded_name": new_mapping}
    api.set_state_name_mapping(**value)

@router.put("/{state_id}/mappings/{mapping_id}", tags=["states"])
async def modify_mapping_for_a_state(state_id: int, mapping_id: int, modified_mapping: str = Form()):
    state = api.get_states(state_id)
    if state.empty:
        raise HTTPException(400, detail=f"state with id {state_id} does not exist")
    all_mappings = api.get_all_state_name_mappings(state_id)
    mapping_ids = all_mappings.loc[:,"mapping_id"]
    if mapping_id not in mapping_ids.values:
        raise HTTPException(400, detail=f"this mapping does not exist for state name {state.name.squeeze()}")
    value = {"state_id": state_id, "recorded_name": modified_mapping}
    api.modify_state_name_mapping(mapping_id,**value)