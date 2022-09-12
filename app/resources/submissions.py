from fastapi import APIRouter, HTTPException, Form, Request, Depends
from pydantic import BaseModel
import json
from sqlalchemy.orm import Session
from services.api_adapter import ApiAdapter

api = ApiAdapter()
router = APIRouter(prefix="/submissions")

def get_db():
    db = api.SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("", tags=["submissions"])
async def all_submisisons(request: Request, db: Session=Depends(get_db)):
    query = request.query_params
    return api.get_many_submissions_jsonapi(db,query)

@router.get("/{submission_id}", tags=["submissions"])
async def submisison_by_id(submission_id: int, request: Request, db: Session=Depends(get_db)):
    query = request.query_params
    return api.get_submission_jsonapi(db, submission_id, query)

# @router.get("/", tags=["submissions"])
# async def get_all_submissions():
#     all_subs = api.get_all_submissions().to_json(orient="records", date_format="iso")
#     return {"data": json.loads(all_subs)}

# @router.get("/{submission_id}", tags=["submissions"])
# async def get_submission_by_id(submission_id: int):
#     if not api.submission_exists(submission_id):
#         raise HTTPException(400, detail="submission does not exist")
#     sub_data, process_steps, current_errors = api.get_submission_by_id(submission_id)
#     comm_data = api.commission_data_with_all_names(submission_id)
#     return {
#         "submission": json.loads(sub_data.to_json(orient="records", date_format="iso"))[0],
#         "processing_steps": json.loads(process_steps.to_json(orient="records", date_format="iso")),
#         "current_errors": json.loads(current_errors.to_json(orient="records", date_format="iso")),
#         "data": json.loads(comm_data.to_json(orient="records", date_format="iso"))
#     }
### NOT JSON: API BELOW HERE ###
@router.put("/{submission_id}", tags=["submissions"])
async def modify_submission_by_id(submission_id: int,
        reporting_month: int = Form(), reporting_year: int = Form()):
    if not api.submission_exists(submission_id):
        raise HTTPException(400, detail=f"submission with id {submission_id} does not exist")
    if not isinstance(reporting_month, int) or not isinstance(reporting_year, int):
        raise HTTPException(400, detail="values must be (whole) numbers")
    if reporting_month < 0 or reporting_year < 0:
        raise HTTPException(400, detail="values must be positive numbers")
    if not (1 <= reporting_month <= 12):
        raise HTTPException(400, detail="month values must be between 1 and 12")
    if not reporting_year >= 1000:
        raise HTTPException(400, detail="year must be at least 4 digits")
    form_data = {"reporting_month": reporting_month, "reporting_year": reporting_year}

    api.modify_submission_metadata(submission_id, **form_data)

@router.delete("/{submission_id}", tags=["submissions"])
async def delete_submission_by_id(submission_id: int):
    # hard delete
    # hard deletes commission data, errors, and steps along with it
    api.delete_submission(submission_id)