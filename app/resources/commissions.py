import calendar
import secrets
import json
from os import getenv
from typing import Optional
from datetime import datetime, timedelta

from dotenv import load_dotenv
from sqlalchemy.orm import Session
from pydantic import BaseModel, validator
from fastapi import APIRouter, HTTPException, File, Depends, Form, BackgroundTasks

from app import report_processor
from entities import submission
from entities.manufacturers import MFG_PREPROCESSORS
from entities.commission_file import CommissionFile
from services.api_adapter import ApiAdapter, get_db, User, get_user
from app.resources.pydantic_form import as_form
from jsonapi.jsonapi import Query, convert_to_jsonapi, JSONAPIRoute

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
async def commission_data(
        query: Query=Depends(),
        db: Session=Depends(get_db),
        user: User=Depends(get_user)
    ):
    jsonapi_query = convert_to_jsonapi(query)
    return api.get_commission_data(db,jsonapi_query, user)

@router.get("/{row_id}", tags=['commissions'])
async def get_commission_data_row(
        row_id: int,
        query: Query=Depends(),
        db: Session=Depends(get_db),
        user: User=Depends(get_user)
    ):
    jsonapi_query = convert_to_jsonapi(query)
    return api.get_commission_data(db,row_id,jsonapi_query, user)

@router.post("/download", tags=['commissions'])
async def commission_data_file_link(
        query_params: Optional[CommissionDataDownloadParameters] = None,
        db: Session=Depends(get_db),
        user: User=Depends(get_user)
    ):
    """
    Generates a download link for the commission data
    """
    user_id = user.id(db=db)
    duration = float(getenv('FILE_LINK_DURATION'))
    _now = datetime.now()
    hash_ = secrets.token_urlsafe()
    default_query = {"filename": "commissions", "user_id": user_id}
    record = {
        "hash": hash_,
        "type": "commission_data",
        "query_args": (
            json.dumps(query_params.dict(exclude_none=True)|{"user_id": user_id})
            if query_params 
            else json.dumps(default_query)
        ),
        "created_at": _now,
        "expires_at": _now + timedelta(seconds=60*duration),
        "user_id": user_id
    }
    api.generate_file_record(db, record)
    return {"downloadLink": f"/download?file={hash_}"}



@router.post("", tags=['commissions'])
async def process_data_from_a_file(
        bg_tasks: BackgroundTasks,
        file: bytes = File(),
        report_id: int = Form(),
        reporting_month: int = Form(),
        reporting_year: int = Form(),
        manufacturer_id: int = Form(),
        total_commission_amount: Optional[float] = Form(None),
        file_password: Optional[str] = Form(None),
        total_freight_amount: Optional[float] = Form(None),
        additional_file_1: Optional[bytes] = Form(None),
        db: Session=Depends(get_db),
        user: User=Depends(get_user),
    ):

    # TODO: should I do this check?
    existing_submissions = api.get_all_submissions(db=db, user=user)
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
            file_password,
            total_freight_amount,
            additional_file_1,
            db,
            user,
            bg_tasks
        )
    return api.get_submissions(db=db, submission_id=new_submission_id,query={}, user=user)

async def process_commissions_file(
        file: bytes,
        report_id: int,
        reporting_month: int,
        reporting_year: int,
        manufacturer_id: int,
        total_commission_amount: float|None,
        file_password: str|None,
        total_freight_amount: float|None,
        additional_file_1: bytes|None,
        session: Session,
        user: User,
        bg_tasks: BackgroundTasks       # passed directly from the calling route
    ) -> int:

    file_obj = CommissionFile(file_data=file, file_password=file_password)
    new_sub = submission.NewSubmission(
            file_obj,
            reporting_month,
            reporting_year,
            report_id,
            manufacturer_id,
            user.id(session),
            total_commission_amount,
            total_freight_amount,
            additional_file_1
        )

    mfg_preprocessor = MFG_PREPROCESSORS.get(manufacturer_id)
    submission_id = api.record_submission(db=session, submission=new_sub)
    mfg_report_processor = report_processor.NewReportStrategy(
        session=session,
        user=user,
        preprocessor=mfg_preprocessor,
        submission=new_sub,
        submission_id=submission_id
    )
    bg_tasks.add_task(mfg_report_processor.process_and_commit)
    return submission_id

# TODO MAKE THIS A JSONAPI RESPONSE
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

# TODO MAKE THIS A JSONAPI RESPONSE
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
async def remove_a_line_in_commission_data(row_id: int) -> None:
    # hard delete
    api.delete_commission_data_line(row_id)
