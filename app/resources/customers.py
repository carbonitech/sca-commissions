from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy_jsonapi.errors import BaseError
from services.api_adapter import ApiAdapter, get_db
from app.jsonapi import Query, convert_to_jsonapi, format_error, JSONAPIRoute, CustomerModificationRequest

api = ApiAdapter()
router = APIRouter(prefix="/customers", route_class=JSONAPIRoute)

@router.get("", tags=["customers"])
async def all_customers(query: Query=Depends(), db: Session=Depends(get_db)):
    try:
        jsonapi_query = convert_to_jsonapi(query)
        return api.get_many_customers_jsonapi(db,jsonapi_query)
    except BaseError as err:
        raise HTTPException(**format_error(err))
    except Exception as err:
        raise HTTPException(status_code=400,detail=str(err))

@router.get("/{customer_id}", tags=["customers"])
async def customer_by_id(customer_id: int, query: Query=Depends(), db: Session=Depends(get_db)):
    try:
        jsonapi_query = convert_to_jsonapi(query)
        return api.get_customer_jsonapi(db, customer_id, jsonapi_query)
    except BaseError as err:
        raise HTTPException(**format_error(err))
    except Exception as err:
        raise HTTPException(status_code=400,detail=str(err))
   

@router.patch("/{customer_id}", tags=["customers"])
async def modify_customer(customer_id: int, customer: CustomerModificationRequest, db: Session=Depends(get_db)):
    try:
        return api.modify_customer_jsonapi(db, customer_id, customer.dict())
    except BaseError as err:
        raise HTTPException(**format_error(err))


@router.post("", tags=["customers"])
async def new_customer():
    ...

@router.delete("/{customer_id}", tags=["customers"])
async def delete_customer(customer_id: int):
    ...
