from fastapi import APIRouter, HTTPException, Form
from pydantic import BaseModel
import json
from services.api_adapter import ApiAdapter

api = ApiAdapter()
router = APIRouter(prefix="/customers")

class Customer(BaseModel):
    name: str

@router.get("/", tags=["customers"])
async def all_customers():
    customers = api.get_customers().to_json(orient="records")
    return({"customers": json.loads(customers)})

@router.post("/", tags=["customers"])
async def new_customer(customer_name: str = Form()):
    customer_name = customer_name.strip().upper()
    current_customers = api.get_customers()
    matches = current_customers.loc[current_customers.name == customer_name]
    if not matches.empty:
        raise HTTPException(status_code=400, detail="Customer already exists")
    return {"customer_id": api.new_customer(customer_fastapi=customer_name)}

@router.get("/{customer_id}", tags=["customers"])
async def customer_by_id(customer_id: int):
    customer = api.get_customer(customer_id).to_json(orient="records")
    return({"customer": json.loads(customer)})

@router.put("/{customer_id}", tags=["customers"])
async def modify_customer(customer_id: int, new_data: Customer):
    new_data.name = new_data.name.strip().upper()
    current_customer = api.check_customer_exists_by_name(**new_data.dict())
    if current_customer:
        raise HTTPException(status_code=400, detail="New Customer Name already exists")
    return api.modify_customer(customer_id, **new_data.dict())

@router.delete("/{customer_id}", tags=["customers"])
async def delete_customer(customer_id: int):
    raise NotImplementedError

@router.get("/{customer_id}/branches", tags=["customers"])
async def customer_branches_by_id(customer_id: int):
    branches = api.get_branches_by_customer(customer_id).to_json(orient="records")
    return({"branches": json.loads(branches)})