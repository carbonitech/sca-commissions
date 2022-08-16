from fastapi import APIRouter, HTTPException, Form
from pydantic import BaseModel
import json
from services.api_adapter import ApiAdapter

api = ApiAdapter()
router = APIRouter(prefix="/submissions")

@router.get("/", tags=["submissions"])
async def get_all_submissions():
    all_subs = api.get_all_submissions().to_json(orient="records", date_format="iso")
    return {"data": json.loads(all_subs)}

@router.get("/{submission_id}", tags=["submissions"])
async def get_submission_by_id(submission_id: int):
    if not api.submission_exists(submission_id):
        raise HTTPException(400, detail="submission does not exist")
    sub_data, process_steps, current_errors = api.get_submission_by_id(submission_id)
    comm_data = api.commission_data_with_all_names(submission_id)
    return {
        "submission": json.loads(sub_data.to_json(orient="records", date_format="iso"))[0],
        "processing_steps": json.loads(process_steps.to_json(orient="records", date_format="iso")),
        "current_errors": json.loads(current_errors.to_json(orient="records", date_format="iso")),
        "data": json.loads(comm_data.to_json(orient="records", date_format="iso"))
    }

@router.put("/{submission_id}", tags=["submissions"])
async def modify_submission_by_id(submission_id: int):
    raise NotImplementedError

@router.delete("/{submission_id}", tags=["submissions"])
async def delete_submission_by_id(submission_id: int):
    raise NotImplementedError