from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from services.api_adapter import ApiAdapter
from app.jsonapi import Query, convert_to_jsonapi
from sqlalchemy_jsonapi.errors import BaseError

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

class JSONAPIModificationBodyModel(BaseModel):
    type: str
    id: int
    attributes: Customer

class CustomerModificationRequest(BaseModel):
    data: JSONAPIModificationBodyModel

@router.get("", tags=["customers"])
async def all_customers(query: Query=Depends(), db: Session=Depends(get_db)):
    try:
        jsonapi_query = convert_to_jsonapi(query)
    except Exception as err:
        raise HTTPException(status_code=400,detail=str(err))
    return api.get_many_customers_jsonapi(db,jsonapi_query)

@router.get("/{customer_id}", tags=["customers"])
async def customer_by_id(customer_id: int, query: Query=Depends(), db: Session=Depends(get_db)):
    try:
        jsonapi_query = convert_to_jsonapi(query)
    except Exception as err:
        raise HTTPException(status_code=400,detail=str(err))
    return api.get_customer_jsonapi(db, customer_id, jsonapi_query)

@router.patch("/{customer_id}", tags=["customers"])
async def modify_customer(customer_id: int, customer: CustomerModificationRequest, db: Session=Depends(get_db)):
    try:
        return api.modify_customer_jsonapi(db, customer_id, customer.dict())
    except BaseError as err:
        """raise an HTTPException instead of this HTTP-like error so FastAPI can handle it properly in the middleware"""
        data: dict = err.data
        status_code: int = err.status_code #subclass of BaseError actually raised will have this attr
        data["errors"][0]["id"] = str(data["errors"][0]["id"]) # 
        raise HTTPException(status_code=status_code, detail=data)

# TODO implement these routes in JSON:API

# @router.post("/", tags=["customers"])
# async def new_customer(customer_name: str = Form()):
#     customer_name = customer_name.strip().upper()
#     current_customers = api.get_customers()
#     matches = current_customers.loc[current_customers.name == customer_name]
#     if not matches.empty:
#         raise HTTPException(status_code=400, detail="Customer already exists")
#     return {"customer_id": api.new_customer(customer_fastapi=customer_name)}

# @router.delete("/{customer_id}", tags=["customers"])
# async def delete_customer(customer_id: int):
#     api.delete_customer(customer_id)

