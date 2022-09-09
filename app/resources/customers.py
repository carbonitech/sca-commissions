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

class QueryArgs(BaseModel):
    # TODO : the json:api specification has parameters like fields using a bracket notation (i.e. fields[some_field] instead of fields=some_field) 
    # meaning that the default behavior, using equals, is causing most of these parameters to not work
    include: str|None = None
    fields: str|None = None
    sort: str|None = None
    page: str|None = None
    filter: str|None = None

### JSON API ###
@router.get("/", tags=["jsonapi"])
async def all_customers(db: Session=Depends(get_db), query: QueryArgs = Depends()):
    query = query.dict(exclude_none=True)
    customers = api.get_many_customers_jsonapi(db,query)
    return customers

@router.get("/{customer_id}", tags=["jsonapi"])
async def customer_by_id(customer_id: int,db: Session=Depends(get_db), query: QueryArgs = Depends()):
    query = query.dict(exclude_none=True)
    customer = api.get_customer(db, customer_id, query)
    return customer
##################


@router.post("/", tags=["customers"])
async def new_customer(customer_name: str = Form()):
    customer_name = customer_name.strip().upper()
    current_customers = api.get_customers()
    matches = current_customers.loc[current_customers.name == customer_name]
    if not matches.empty:
        raise HTTPException(status_code=400, detail="Customer already exists")
    return {"customer_id": api.new_customer(customer_fastapi=customer_name)}

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
