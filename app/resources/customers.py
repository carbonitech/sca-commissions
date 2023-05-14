from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from services.api_adapter import ApiAdapter, get_db, get_user, User
from jsonapi.jsonapi import Query, convert_to_jsonapi, JSONAPIRoute
from jsonapi.request_models import RequestModels

api = ApiAdapter()
router = APIRouter(prefix="/customers", route_class=JSONAPIRoute)

@router.get("", tags=["customers"])
async def all_customers(query: Query=Depends(), db: Session=Depends(get_db), user: User=Depends(get_user)):
    jsonapi_query = convert_to_jsonapi(query)
    return api.get_customers(db,jsonapi_query, user)

@router.get("/{customer_id}", tags=["customers"])
async def customer_by_id(customer_id: int, query: Query=Depends(), db: Session=Depends(get_db), user: User=Depends(get_user)):
    jsonapi_query = convert_to_jsonapi(query)
    return api.get_customers(db, jsonapi_query, user, customer_id)
   
@router.patch("/{customer_id}", tags=["customers"])
async def modify_customer(customer_id: int,
        customer: RequestModels.customer_modification,
        db: Session=Depends(get_db),
        user: User=Depends(get_user)):
    return api.modify_customer_jsonapi(db, customer_id, customer.dict(), user)

@router.post("", tags=["customers"])
async def new_customer(jsonapi_obj: RequestModels.new_customer, db: Session=Depends(get_db), user: User=Depends(get_user)):
    return api.create_customer(db=db, json_data=jsonapi_obj.dict(), user=user)

@router.delete("/{customer_id}", tags=["customers"])
async def delete_customer(customer_id: int, db: Session=Depends(get_db), user: User=Depends(get_user)):
    # soft delete
    return api.delete_customer(db=db,customer_id=customer_id)
