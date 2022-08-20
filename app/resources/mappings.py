import json
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Extra

from services.api_adapter import ApiAdapter

api = ApiAdapter()
router = APIRouter(prefix="/mappings")

class CustomerNameMapping(BaseModel, extra=Extra.allow):
    recorded_name: str

class CityNameMapping(BaseModel, extra=Extra.allow):
    city_id: int
    recorded_name: str

class StateNameMapping(BaseModel, extra=Extra.allow):
    state_id: int
    recorded_name: str

class RepCustomerMapping(BaseModel, extra=Extra.allow):
    rep_id: int
    customer_branch_id: int


@router.get("/customers", tags=["customers"])
async def get_all_mappings_related_to_customers():
    return json.loads(api.get_all_customer_name_mappings().to_json(orient="records"))

@router.get("/customers/{customer_id}", tags=["customers"])
async def get_all_mappings_related_to_a_specific_customer(customer_id: int):
    rep_mappings = json.loads(api.get_rep_to_customer_full(customer_id).to_json(orient="records"))
    name_mappings = json.loads(api.get_all_customer_name_mappings(customer_id).to_json(orient="records"))
    return {"names": name_mappings, "reps_by_branch": rep_mappings}

@router.get("/customers/{customer_id}/names", tags=["customers"])
async def get_name_mappings_for_a_customer(customer_id: int):
    name_mappings = json.loads(api.get_all_customer_name_mappings(customer_id).to_json(orient="records"))
    return {"names": name_mappings}

@router.get("/customers/{customer_id}/reps", tags=["customers"])
async def get_rep_mappings_for_a_customer(customer_id: int):
    rep_mappings = json.loads(api.get_rep_to_customer_full(customer_id).to_json(orient="records"))
    return {"reps_by_branch": rep_mappings}

@router.post("/customers/{customer_id}/names", tags=["customers"])
async def create_new_name_mapping(customer_id: int, new_mapping: CustomerNameMapping):
    current_customer = not api.get_customer(customer_id).empty
    if not current_customer:
        raise HTTPException(400, detail="Customer does not exist")
    new_mapping.customer_id = customer_id
    api.set_customer_name_mapping(**new_mapping.dict(exclude_none=True))

@router.post("/customers/{customer_id}/reps", tags=["customers"])
async def create_new_rep_mapping(customer_id: int, new_mapping: RepCustomerMapping):
    customer_branches = api.get_customer_branches_raw(customer_id)
    if customer_branches.loc[customer_branches["id"] == new_mapping.customer_branch_id].empty:
        raise HTTPException(400, detail=f"Branch does not exist for customer: {api.get_customer(customer_id).name.values[0]}")
    branches_currently_mapped = api.get_rep_to_customer_full(customer_id)
    branch_mapping = branches_currently_mapped.loc[
        (branches_currently_mapped["Branch ID"] == new_mapping.customer_branch_id),"Rep"]
    if not branch_mapping.empty:
        raise HTTPException(400, detail=f"Branch is already assigned to {branch_mapping.values[0]}")
    api.set_rep_to_customer_mapping(**new_mapping.dict(exclude_none=True))

@router.put("/customers/{customer_id}/reps/{mapping_id}", tags=["customers"])
async def modify_customer_rep_mapping(customer_id: int, mapping_id: int, new_mapping: RepCustomerMapping):
    if api.get_customer(customer_id).empty:
        raise HTTPException(400, detail="Customer does not exist")
    all_mappings = api.get_rep_to_customer_full(customer_id)
    if mapping_id not in all_mappings["Rep to Customer ID"].values:
        raise HTTPException(400, detail="Rep to customer mapping does not exist")
    existing_branch_id = all_mappings.loc[all_mappings["Rep to Customer ID"] == mapping_id, "Branch ID"].values[0]
    if existing_branch_id != new_mapping.customer_branch_id:
        raise HTTPException(400, detail="You are not allowed to change the branch id of an existing rep mapping.")
    if new_mapping.rep_id not in api.get_all_reps()["id"].values:
        raise HTTPException(400, detail="Rep does not exist")
    api.update_rep_to_customer_mapping(mapping_id, **new_mapping.dict(exclude_none=True))

@router.delete("/customers/{customer_id}/names", tags=["customers"])
async def delete_customer_name_mapping(customer_id: int, mapping_id: int):
    # hard delete
    api.delete_customer_name_mapping(mapping_id)

@router.get("/cities", tags=["cities"])
async def get_all_mappings_for_city_names():
    return json.loads(api.get_all_city_name_mappings().to_json(orient="records"))

@router.get("/cities/{city_id}", tags=["cities"])
async def get_all_mappings_for_a_city(city_id: int):
    return json.loads(api.get_all_city_name_mappings(city_id).to_json(orient="records"))

@router.post("/cities", tags=["cities"])
async def create_new_mapping_for_a_city(new_mapping: CityNameMapping):
    current_city = not api.get_cities(new_mapping.city_id).empty
    if not current_city:
        raise HTTPException(400, detail="City name does not exist")
    api.set_city_name_mapping(**new_mapping.dict(exclude_none=True))

@router.put("/cities/{city_id}", tags=["cities"])
async def modify_mapping_for_a_city(city_id: int):
    raise NotImplementedError

@router.get("/states", tags=["states"])
async def get_all_mappings_for_state_names():
    return json.loads(api.get_all_state_name_mappings().to_json(orient="records"))

@router.get("/states/{state_id}", tags=["states"])
async def get_all_mappings_a_city(state_id: int):
    return json.loads(api.get_all_state_name_mappings(state_id).to_json(orient="records"))

@router.post("/states", tags=["states"])
async def create_new_mapping_for_a_state(new_mapping: StateNameMapping):
    current_state = not api.get_states(new_mapping.state_id).empty
    if not current_state:
        raise HTTPException(400, detail="State does not exist")
    api.set_state_name_mapping(**new_mapping.dict(exclude_none=True))

@router.put("/states/{state_id}", tags=["states"])
async def modify_mapping_for_a_state(state_id: int):
    raise NotImplementedError