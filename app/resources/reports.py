from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from services import get, post, patch, delete
from jsonapi.jsonapi import Query, convert_to_jsonapi, JSONAPIRoute
from services.utils import User, get_db, get_user

router = APIRouter(prefix="/reports", route_class=JSONAPIRoute)

@router.get("", tags=["form fields"])
async def fields(
        query: Query=Depends(),
        db: Session=Depends(get_db),
        user: User=Depends(get_user)
    ):
    jsonapi_query = convert_to_jsonapi(query)
    return get.reports(db,jsonapi_query, user)

@router.get("/{report_id}", tags=["reports"])
async def customer_by_id(
        report_id: int,
        query: Query=Depends(),
        db: Session=Depends(get_db),
        user: User=Depends(get_user)
    ):
    jsonapi_query = convert_to_jsonapi(query)
    return get.reports(db, jsonapi_query, user, report_id)

@router.post("", tags=['reports'])
async def new_report():
    ...

@router.patch("/{report_id}", tags=['reports'])
async def modify_report():
    ...

@router.delete("/{report_id}", tags=['reports'])
async def delete_report():
    # NOTE use this route to test out use of Auth0 permissions
    ...