from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from services.api_adapter import ApiAdapter
from app.jsonapi import Query, convert_to_jsonapi

api = ApiAdapter()
router = APIRouter(prefix="/manufacturers")

def get_db():
    db = api.SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("", tags=["manufacturers"])
async def all_manufacturers(query: Query=Depends(), db: Session=Depends(get_db)):
    try:
        jsonapi_query = convert_to_jsonapi(query)
    except Exception as err:
        raise HTTPException(status_code=400,detail=str(err))
    return api.get_many_manufacturers_jsonapi(db,jsonapi_query)
@router.get("/{manuf_id}", tags=["manufacturers"])
async def manufacturer_by_id(manuf_id: int, query: Query=Depends(), db: Session=Depends(get_db)):
    try:
        jsonapi_query = convert_to_jsonapi(query)
    except Exception as err:
        raise HTTPException(status_code=400,detail=str(err))
    return api.get_manufacturer_jsonapi(db,manuf_id,jsonapi_query)
    
# @router.post("/", tags=["manufacturers"])
# async def add_a_manufacturer(manuf_name: Manufacturer):
#     current_manufacturers = api.get_all_manufacturers()
#     if not current_manufacturers.loc[current_manufacturers.name == manuf_name].empty:
#         raise HTTPException(400, detail="manufacturer with that name already exists")
#     return {"new id": api.set_new_manufacturer(**manuf_name.dict())}

# @router.delete("/{manuf_id}", tags=["manufacturers"])
# async def delete_manufacturer_by_id(manuf_id: int):
#     api.delete_manufacturer(manuf_id)

# @router.post("/{manuf_id}/reports", tags=["manufacturers"])
# async def create_new_report(manuf_id: int):
#     ...
# @router.put("/{manuf_id}/reports/{report_id}", tags=["manufacturers"])
# async def modify_report_details(manuf_id: int, report_id: int, **kwargs):
#     ...
# @router.delete("/{manuf_id}/reports/{report_id}", tags=["manufacturers"])
# async def delete_report(manuf_id: int, report_id: int):
#     # soft delete
#     ...