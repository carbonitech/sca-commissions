from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from services.api_adapter import ApiAdapter, get_db, get_user, User
from app.jsonapi import Query, convert_to_jsonapi, JSONAPIRoute, CustomerModificationRequest, RequestModels

api = ApiAdapter()
router = APIRouter(prefix="/customers", route_class=JSONAPIRoute)

@router.get("", tags=["customers"])
async def all_customers(query: Query=Depends(), db: Session=Depends(get_db), user: User=Depends(get_user)):
    jsonapi_query = convert_to_jsonapi(query)
    return api.get_many_customers_jsonapi(db,jsonapi_query, user)

@router.get("/{customer_id}", tags=["customers"])
async def customer_by_id(customer_id: int, query: Query=Depends(), db: Session=Depends(get_db), user: User=Depends(get_user)):
    jsonapi_query = convert_to_jsonapi(query)
    return api.get_customer_jsonapi(db, customer_id, jsonapi_query, user)
   
@router.patch("/{customer_id}", tags=["customers"])
async def modify_customer(customer_id: int,
        customer: CustomerModificationRequest,
        db: Session=Depends(get_db),
        user: User=Depends(get_user)):
        
    return api.modify_customer_jsonapi(db, customer_id, customer.dict(), user)

@router.post("", tags=["customers"])
async def new_customer(name_mapping: RequestModels.new_customer, db: Session=Depends(get_db), user: User=Depends(get_user)):
    return api.create_customer(db=db, json_data=name_mapping.dict(), user=user)

@router.delete("/{customer_id}", tags=["customers"])
async def delete_customer(customer_id: int, db: Session=Depends(get_db), user: User=Depends(get_user)):
    user.id(db=db)
    ...
