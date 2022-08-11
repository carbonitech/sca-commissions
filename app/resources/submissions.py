from fastapi import APIRouter, HTTPException, Form
from pydantic import BaseModel
import json
from db import db_services

db = db_services.DatabaseServices()
router = APIRouter(prefix="/submissions")

@router.get("/", tags=["submissions"])
async def get_all_submissions():
    all_subs = db.get_all_submissions().to_json(orient="records", date_format="iso")
    return {"data": json.loads(all_subs)}

@router.get("/{submission_id}", tags=["submissions"])
async def get_submission_by_id(submission_id: int):
    sub_data, process_steps, current_errors = db.get_submission_by_id(submission_id)
    return {
        "submission": json.loads(sub_data.to_json(orient="records", date_format="iso"))[0],
        "processing_steps": json.loads(process_steps.to_json(orient="records", date_format="iso")),
        "current_errors": json.loads(current_errors.to_json(orient="records", date_format="iso"))
    }