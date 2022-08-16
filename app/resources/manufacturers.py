from fastapi import APIRouter, HTTPException, Form
from pydantic import BaseModel
import json
from services.api_adapter import ApiAdapter

api = ApiAdapter()
router = APIRouter(prefix="/manufacturers")

@router.get("/", tags=["manufacturers"])
async def all_manufacturers():
    manufacturers_ = api.get_all_manufacturers().to_json(orient="records")
    return({"manufacturers": json.loads(manufacturers_)})


@router.get("/{manuf_id}", tags=["manufacturers"])
async def manufacturer_by_id(manuf_id: int):
    manufacturer, reports, submissions = api.get_manufacturer_by_id(manuf_id)
    manufacturer_json = json.loads(manufacturer.to_json(orient="records"))
    reports_json = json.loads(reports.to_json(orient="records"))
    submissions_json = json.loads(submissions.to_json(orient="records", date_format="iso"))
    return({"manufacturer_details": manufacturer_json,
            "reports": reports_json,
            "submissions": submissions_json})

@router.post("/", tags=["manufacturers"])
async def add_a_manufacturer(manuf_name: str):
    manuf_name = manuf_name.strip().upper()
    raise NotImplementedError


@router.delete("/{manuf_id}", tags=["manufacturers"])
async def delete_manufacturer_by_id(manuf_id: int):
    raise NotImplementedError
