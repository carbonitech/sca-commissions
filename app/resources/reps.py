from fastapi import APIRouter, HTTPException, Form
from pydantic import BaseModel
import json
from db import db_services

db = db_services.DatabaseServices()
router = APIRouter(prefix="/reps")

@router.get("/", tags=["reps"])
async def get_all_reps():
    all_reps = db.get_all_reps().to_json(orient="records", date_format="iso")
    return {"data": json.loads(all_reps)}

@router.get("/{rep_id}",tags=["reps"])
async def get_rep_by_id(rep_id: int):
    rep_and_branches = db.get_rep_and_branches(rep_id).to_json(orient="records")
    return {"data": json.loads(rep_and_branches)}
