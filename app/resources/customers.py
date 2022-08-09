from fastapi import APIRouter, HTTPException, Form
from pydantic import BaseModel
import json
from db import db_services

db = db_services.DatabaseServices()
router = APIRouter(prefix="/customers")

class Customer(BaseModel):
    name: str

@router.get("/")
async def all_customers():
    customers = db.get_customers().to_json(orient="records")
    return({"customers": json.loads(customers)})

@router.post("/")
async def new_customer(customer_name: str = Form()):
    customer_name = customer_name.strip().upper()
    current_customers = db.get_customers()
    matches = current_customers.loc[current_customers.name == customer_name]
    if not matches.empty:
        raise HTTPException(status_code=400, detail="Customer already exists")
    return {"customer_id": db.new_customer(customer_fastapi=customer_name)}

@router.get("/{customer_id}")
async def customer_by_id(customer_id: int):
    customer = db.get_customer(customer_id).to_json(orient="records")
    return({"customer": json.loads(customer)})

@router.put("/{customer_id}")
async def modify_customer(customer_id: int, new_data: Customer):
    new_data.name = new_data.name.strip().upper()
    current_customer = db.check_customer_exists_by_name(**new_data.dict())
    if current_customer:
        raise HTTPException(status_code=400, detail="New Customer Name already exists")
    
    return db.modify_customer(customer_id, **new_data.dict())

@router.get("/{customer_id}/branches")
async def customer_branches_by_id(customer_id: int):
    branches = db.get_branches_by_customer(customer_id).to_json(orient="records")
    return({"branches": json.loads(branches)})