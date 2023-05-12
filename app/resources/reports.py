from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from services.api_adapter import ApiAdapter, get_db, User, get_user
from jsonapi.jsonapi import Query, convert_to_jsonapi, JSONAPIRoute

api = ApiAdapter()
router = APIRouter(prefix="/reports", route_class=JSONAPIRoute)

@router.get("", tags=["form fields"])
async def fields(query: Query=Depends(), db: Session=Depends(get_db), user: User=Depends(get_user)):
    jsonapi_query = convert_to_jsonapi(query)
    return api.get_reports(db,jsonapi_query, user)

@router.get("/{report_id}", tags=["customers"])
async def customer_by_id(report_id: int, query: Query=Depends(), db: Session=Depends(get_db), user: User=Depends(get_user)):
    jsonapi_query = convert_to_jsonapi(query)
    return api.get_reports(db, jsonapi_query, user, report_id)