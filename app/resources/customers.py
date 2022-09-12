from fastapi import APIRouter, HTTPException, Form, Depends, Request
from pydantic import BaseModel
from sqlalchemy.orm import Session
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


@router.get("", tags=["customers"])
async def all_customers(request: Request, db: Session=Depends(get_db)):
    query = request.query_params
    customers = api.get_many_customers_jsonapi(db,query)
    return customers

@router.get("/{customer_id}", tags=["customers"])
async def customer_by_id(customer_id: int, request: Request, db: Session=Depends(get_db)):
    query = request.query_params
    customer = api.get_customer_jsonapi(db, customer_id, query)
    return customer

# TODO implement these routes in JSON:API

# @router.post("/", tags=["customers"])
# async def new_customer(customer_name: str = Form()):
#     customer_name = customer_name.strip().upper()
#     current_customers = api.get_customers()
#     matches = current_customers.loc[current_customers.name == customer_name]
#     if not matches.empty:
#         raise HTTPException(status_code=400, detail="Customer already exists")
#     return {"customer_id": api.new_customer(customer_fastapi=customer_name)}

# @router.put("/{customer_id}", tags=["customers"])
# async def modify_customer(customer_id: int, new_data: Customer):
#     new_data.name = new_data.name.strip().upper()
#     current_customer = api.check_customer_exists_by_name(**new_data.dict())
#     if current_customer:
#         raise HTTPException(status_code=400, detail="New Customer Name already exists")
#     return api.modify_customer(customer_id, **new_data.dict())

# @router.delete("/{customer_id}", tags=["customers"])
# async def delete_customer(customer_id: int):
#     api.delete_customer(customer_id)

