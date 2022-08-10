from fastapi import APIRouter, HTTPException, Form
from pydantic import BaseModel
import json
from db import db_services

db = db_services.DatabaseServices()
router = APIRouter(prefix="/manufacturers")

@router.get("/")
async def all_manufacturers():
    manufacturers_ = db.get_all_manufacturers().to_json(orient="records")
    return({"manufacturers": json.loads(manufacturers_)})


@router.get("/{manuf_id}")
async def manufacturer_by_id(manuf_id: int):
    manufacturer, reports, submissions = db.get_manufacturer_by_id(manuf_id)
    manufacturer_json = json.loads(manufacturer.to_json(orient="records"))
    reports_json = json.loads(reports.to_json(orient="records"))
    submissions_json = json.loads(submissions.to_json(orient="records", date_format="iso"))
    return({"manufacturer_details": manufacturer_json,
            "reports": reports_json,
            "submissions": submissions_json})
