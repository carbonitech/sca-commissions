import json
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from db import db_services

router = APIRouter(prefix="/mappings")
db = db_services.DatabaseServices()

class CustomerNameMapping(BaseModel):
    customer_id: int
    recorded_name: str

class CityNameMapping(BaseModel):
    city_id: int
    recorded_name: str

class StateNameMapping(BaseModel):
    state_id: int
    recorded_name: str

class RepCustomerMapping(BaseModel):
    rep_id: int
    customer_branch_id: int


@router.get("/customers")
async def get_all_mappings_related_to_customers():
    return json.loads(db.get_all_customer_name_mappings().to_json(orient="records"))

@router.get("/customers/{customer_id}")
async def get_all_mappings_related_to_a_specific_customer(customer_id: int):
    rep_mappings = json.loads(db.get_rep_to_customer_full(customer_id).to_json(orient="records"))
    name_mappings = json.loads(db.get_all_customer_name_mappings(customer_id).to_json(orient="records"))
    return {"names": name_mappings, "reps_by_branch": rep_mappings}

@router.get("/customers/{customer_id}/names")
async def get_all_mappings_related_to_a_specific_customer(customer_id: int):
    name_mappings = json.loads(db.get_all_customer_name_mappings(customer_id).to_json(orient="records"))
    return {"names": name_mappings}

@router.get("/customers/{customer_id}/reps")
async def get_all_mappings_related_to_a_specific_customer(customer_id: int):
    rep_mappings = json.loads(db.get_rep_to_customer_full(customer_id).to_json(orient="records"))
    return {"reps_by_branch": rep_mappings}

@router.post("/customers/{customer_id}/names")
async def create_new_name_mapping(new_mapping: CustomerNameMapping):
    current_customer = not db.get_customer(new_mapping.customer_id).empty
    if not current_customer:
        raise HTTPException(400, detail="Customer does not exist")
    db.set_customer_name_mapping(**new_mapping.dict())

@router.post("/customers/{customer_id}/reps")
async def create_new_rep_mapping(customer_id: int, new_mapping: RepCustomerMapping):
    customer_branches = db.get_customer_branches_raw(customer_id)
    if customer_branches.loc[customer_branches["id"] == new_mapping.customer_branch_id].empty:
        raise HTTPException(400, detail=f"Branch does not exist for customer: {db.get_customer(customer_id).name.values[0]}")
    branches_currently_mapped = db.get_rep_to_customer_full(customer_id)
    branch_mapping = branches_currently_mapped.loc[
        (branches_currently_mapped["Branch ID"] == new_mapping.customer_branch_id),"Rep"]
    if not branch_mapping.empty:
        raise HTTPException(400, detail=f"Branch is already assigned to {branch_mapping.values[0]}")
    db.set_rep_to_customer_mapping(**new_mapping.dict())

@router.get("/cities")
async def get_all_mappings_for_city_names():
    return json.loads(db.get_all_city_name_mappings().to_json(orient="records"))

@router.get("/cities/{city_id}")
async def get_all_mappings_a_city(city_id: int):
    return json.loads(db.get_all_city_name_mappings(city_id).to_json(orient="records"))

@router.post("/cities")
async def create_new_mapping_for_a_city(new_mapping: CityNameMapping):
    current_city = not db.get_cities(new_mapping.city_id).empty
    if not current_city:
        raise HTTPException(400, detail="City name does not exist")
    db.set_city_name_mapping(**new_mapping.dict())

@router.put("/cities/{city_id}")
async def modify_mapping_for_a_city(): ...

@router.get("/states")
async def get_all_mappings_for_state_names():
    return json.loads(db.get_all_state_name_mappings().to_json(orient="records"))

@router.get("/states/{state_id}")
async def get_all_mappings_a_city(state_id: int):
    return json.loads(db.get_all_state_name_mappings(state_id).to_json(orient="records"))

@router.post("/states")
async def create_new_mapping_for_a_state(new_mapping: StateNameMapping):
    current_state = not db.get_states(new_mapping.state_id).empty
    if not current_state:
        raise HTTPException(400, detail="State does not exist")
    db.set_state_name_mapping(**new_mapping.dict())

@router.put("/states/{state_id}")
async def modify_mapping_for_a_state(): ...