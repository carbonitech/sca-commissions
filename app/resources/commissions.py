import calendar
import secrets
import json
from os import getenv
from typing import Optional
from datetime import datetime, timedelta

from dotenv import load_dotenv
from sqlalchemy.orm import Session
from pydantic import BaseModel, validator
from fastapi import APIRouter, HTTPException, File, Depends, Form

from app import report_processor
from entities import submission
from entities.manufacturers import MFG_PREPROCESSORS
from entities.commission_file import CommissionFile
from services.api_adapter import ApiAdapter, get_db
from app.resources.pydantic_form import as_form
from app.jsonapi import Query, convert_to_jsonapi, JSONAPIRoute

load_dotenv()
api = ApiAdapter()
router = APIRouter(prefix="/commission-data", route_class=JSONAPIRoute)

@as_form
class CustomCommissionData(BaseModel):
    inv_amt: float
    comm_amt: float
    description: str

    @validator('inv_amt')
    def scale_up_inv_amt(cls, value):
        return value*100

    @validator('comm_amt')
    def scale_up_comm_amt(cls, value):
        return value*100

class CommissionDataDownloadParameters(BaseModel):
    filename: str|None = "commissions"
    startDate: str|None = None
    endDate: str|None = None
    manufacturer_id: int|None = None
    customer_id: int|None = None
    city_id: int|None = None
    state_id: int|None = None
    representative_id: int|None = None

@router.get("", tags=['commissions'])
async def commission_data(query: Query=Depends(), db: Session=Depends(get_db)):
    jsonapi_query = convert_to_jsonapi(query)
    return api.get_all_commission_data_jsonapi(db,jsonapi_query)

@router.get("/{row_id}", tags=['commissions'])
async def get_commission_data_row(row_id: int, query: Query=Depends(), db: Session=Depends(get_db)):
    jsonapi_query = convert_to_jsonapi(query)
    return api.get_commission_data_by_id_jsonapi(db,row_id,jsonapi_query)

@router.post("/download", tags=['commissions'])
async def commission_data_file_link(
        query_params: Optional[CommissionDataDownloadParameters] = None,
        db: Session=Depends(get_db)
    ):
    """
    Generates a download link for the commission data
    """
    duration = float(getenv('FILE_LINK_DURATION'))
    _now = datetime.now()
    hash_ = secrets.token_urlsafe()
    record = {
        "hash": hash_,
        "type": "commission_data",
        "query_args": json.dumps(query_params.dict(exclude_none=True)) if query_params else json.dumps({"filename": "commissions"}),
        "created_at": _now,
        "expires_at": _now + timedelta(seconds=60*duration)
    }
    api.generate_file_record(db, record)
    return {"downloadLink": f"/download?file={hash_}"}


async def process_commissions_file(
        file: bytes,
        report_id: int,
        reporting_month: int,
        reporting_year: int,
        manufacturer_id: int,
        total_commission_amount: float|None,
        session: Session,
    ):
    file_obj = CommissionFile(file)
    new_sub = submission.NewSubmission(
            file_obj,
            reporting_month,
            reporting_year,
            report_id,
            manufacturer_id,
            total_commission_amount
        )
    mfg_preprocessor = MFG_PREPROCESSORS.get(manufacturer_id)
    mfg_report_processor = report_processor.ReportProcessor(
        preprocessor=mfg_preprocessor,
        submission=new_sub,
        session=session
    )

    try:
        return mfg_report_processor.process_and_commit()
    except report_processor.FileProcessingError as err:
        import traceback
        tb = traceback.format_exc()
        api.delete_submission(err.submission_id, session=session)
        status_code = 400
        detail = "There was an error processing the file. Please contact joe@carbonitech.com with the id number {id_}"
        error_obj = {"errors":[{"status": status_code, "detail": detail, "title": "processing_error", "traceback": tb}]}
        raise HTTPException(status_code, detail=error_obj)


@router.post("", tags=['commissions'])
async def process_data_from_a_file(
        file: bytes = File(),
        report_id: int = Form(),
        reporting_month: int = Form(),
        reporting_year: int = Form(),
        manufacturer_id: int = Form(),
        total_commission_amount: Optional[float] = Form(None),
        db: Session=Depends(get_db)
    ):

    # TODO: should I do this check?
    existing_submissions = api.get_all_submissions(db=db)
    existing_submission = existing_submissions.loc[
        (existing_submissions["reporting_month"] == reporting_month)
        & (existing_submissions["reporting_year"] == reporting_year)
        & (existing_submissions["report_id"] == report_id)
    ]

    if not existing_submission.empty:
        report_name = existing_submission['report_name'].squeeze()
        manuf = existing_submission['name'].squeeze()
        date_ = datetime.strftime(existing_submission['submission_date'].squeeze(),"%m/%d/%Y %I:%M %p")
        report_month = calendar.month_name[existing_submission['reporting_month'].squeeze()]
        report_year = str(existing_submission['reporting_year'].squeeze())
        id_ = str(existing_submission['id'].squeeze())
        msg = f"The {report_name} report for {manuf} for reporting period " \
            f"{report_month} {report_year} was already submitted at " \
            f"{date_} with id {id_}"
        raise HTTPException(400, detail=msg)

    new_submission_id = await process_commissions_file(
            file,
            report_id,
            reporting_month,
            reporting_year,
            manufacturer_id,
            total_commission_amount,
            db,
        )
    return api.get_submission_jsonapi(db=db, submission_id=new_submission_id,query={})

@router.post("/{submission_id}", tags=['commissions'])
async def add_custom_entry_to_commission_data(
        submission_id: int,
        map_rep_customer_id: int,
        data: CustomCommissionData = Depends(CustomCommissionData.as_form)
    ):
    if not api.submission_exists(submission_id):
        raise HTTPException(400, detail="report submission does not exist")
    payload = {"map_rep_customer_id": map_rep_customer_id} | data.dict(exclude={"description"})
    row_id = api.set_new_commission_data_entry(submission_id=submission_id, **payload)
    return {"Success": f"line written to row {row_id}"}

@router.put("/{row_id}", tags=['commissions'])
async def modify_an_entry_in_commission_data(
        row_id: int, data:
        CustomCommissionData = Depends(CustomCommissionData.as_form)
    ):
    row_exists = api.get_commission_data_by_row(row_id)
    if not row_exists:
        raise HTTPException(400, detail="row does not exist")
    api.modify_commission_data_row(row_id, **data.dict(exclude={"description"}))

@router.delete("/{row_id}", tags=['commissions'])
async def remove_a_line_in_commission_data(row_id: int):
    # hard delete
    api.delete_commission_data_line(row_id)
