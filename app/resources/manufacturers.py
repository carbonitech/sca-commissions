from fastapi import APIRouter, HTTPException, Form
from pydantic import BaseModel, validator
import json
from services.api_adapter import ApiAdapter

api = ApiAdapter()
router = APIRouter(prefix="/manufacturers")

class Manufacturer(BaseModel):
    name: str

    @validator('name')
    def name_uppercase(cls, value: str):
        return value.strip().upper()


@router.get("/", tags=["manufacturers"])
async def all_manufacturers():
    manufacturers_ = api.get_all_manufacturers().to_json(orient="records")
    return({"manufacturers": json.loads(manufacturers_)})

@router.get("/{manuf_id}", tags=["manufacturers"])
async def manufacturer_by_id(manuf_id: int):
    manufacturer, submissions = api.get_manufacturer_by_id(manuf_id)
    manufacturer_json = json.loads(manufacturer.to_json(orient="records"))
    # reports_json = json.loads(reports.to_json(orient="records"))
    submissions_json = json.loads(submissions.to_json(orient="records", date_format="iso"))
    return({"manufacturer_details": manufacturer_json,
            # "reports": reports_json,
            "submissions": submissions_json})

@router.post("/", tags=["manufacturers"])
async def add_a_manufacturer(manuf_name: Manufacturer):
    current_manufacturers = api.get_all_manufacturers()
    if not current_manufacturers.loc[current_manufacturers.name == manuf_name].empty:
        raise HTTPException(400, detail="manufacturer with that name already exists")
    return {"new id": api.set_new_manufacturer(**manuf_name.dict())}

@router.delete("/{manuf_id}", tags=["manufacturers"])
async def delete_manufacturer_by_id(manuf_id: int):
    api.delete_manufacturer(manuf_id)


@router.get("/{manuf_id}/reports")
async def get_all_reports_by_manufacturer_id(manuf_id: int):
    ...
@router.post("/{manuf_id}/reports")
async def create_new_report(manuf_id: int):
    ...
@router.put("/{manuf_id}/reports/{report_id}")
async def modify_report_details(manuf_id: int, report_id: int, **kwargs):
    ...
@router.delete("/{manuf_id}/reports/{report_id}")
async def delete_report(manuf_id: int, report_id: int):
    # soft delete
    ...