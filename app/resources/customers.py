from fastapi import APIRouter, HTTPException, Form, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session
import json
from services.api_adapter import ApiAdapter

api = ApiAdapter()
router = APIRouter(prefix="/customers")

def get_db():
    db = api.SessionLocal()
    try:
        yield db
    finally:
        db.close()

class Customer(BaseModel):
    name: str

# class CustomerBranch(BaseModel):
#     customer_id: int
#     city_id: int
#     state_id: int

#     class Config:
#         orm_mode = True

# class Customer(CustomerBase):
#     id: int
#     branches: list[CustomerBranch] = []

#     class Config:
#         orm_mode = True

#### JSON:API Spec ####
@router.get("/{customer_id}/jsonapi", tags=["jsonapi"],
        # response_model=Customer
        )
async def customer_by_id_jsonapi(customer_id: int, db: Session=Depends(get_db)):
    customer = api.get_customer_jsonapi(db, customer_id)
    return customer
##################

@router.get("/", tags=["customers"])
async def all_customers():
    customers = api.get_customers().to_json(orient="records", date_format="iso")
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
    customer = api.get_customer(customer_id).to_json(orient="records", date_format="iso")
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
    api.delete_customer(customer_id)

@router.get("/{customer_id}/branches", tags=["customers"])
async def customer_branches_by_id(customer_id: int):
    branches = api.get_branches_by_customer(customer_id).to_json(orient="records", date_format="iso")
    return({"branches": json.loads(branches)})
