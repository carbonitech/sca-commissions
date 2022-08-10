import json
from fastapi import APIRouter
from db import db_services

router = APIRouter(prefix="/mappings")
db = db_services.DatabaseServices()


@router.get("/customers")
async def get_all_mappings_related_to_customers():
    return json.loads(db.get_all_customer_name_mappings().to_json(orient="records"))

@router.get("/customers/{customer_id}")
async def get_all_mappings_related_to_a_specific_customer(customer_id: int):
    return json.loads(db.get_all_customer_name_mappings(customer_id).to_json(orient="records"))

@router.get("/cities")
async def get_all_mappings_for_city_names():
    return json.loads(db.get_all_city_name_mappings().to_json(orient="records"))

@router.get("/cities/{city_id}")
async def get_all_mappings_a_city(city_id: int):
    return json.loads(db.get_all_city_name_mappings(city_id).to_json(orient="records"))

@router.get("/states")
async def get_all_mappings_for_state_names():
    return json.loads(db.get_all_state_name_mappings().to_json(orient="records"))

@router.get("/states/{state_id}")
async def get_all_mappings_a_city(state_id: int):
    return json.loads(db.get_all_state_name_mappings(state_id).to_json(orient="records"))

@router.post("/customers")
async def create_new_mapping_for_a_customer(): ...

@router.post("/cities")
async def create_new_mapping_for_a_city(): ...

@router.post("/states")
async def create_new_mapping_for_a_state(): ...

@router.put("/customers")
async def modify_mapping_for_a_customer(): ...

@router.put("/cities")
async def modify_mapping_for_a_city(): ...

@router.put("/states")
async def modify_mapping_for_a_state(): ...