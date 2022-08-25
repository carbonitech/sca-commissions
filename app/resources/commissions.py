from io import BytesIO
import typing
import json
import calendar
from datetime import datetime
from fastapi import APIRouter, HTTPException, Form, File, Depends
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask
from pydantic import BaseModel, validator
from pandas import ExcelWriter

from db import db_services
from app import report_processor
from entities import submission
from entities.manufacturers import adp
from entities.commission_file import CommissionFile
from services.api_adapter import ApiAdapter
from app.resources.pydantic_form import as_form

db = db_services.DatabaseServices()
api = ApiAdapter()
router = APIRouter(prefix="/commissions")

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


class ExcelFileResponse(StreamingResponse):
    media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    def __init__(self, 
            content: typing.Any, 
            status_code: int = 200, 
            headers: typing.Optional[typing.Mapping[str, str]] = None, 
            media_type: typing.Optional[str] = None, 
            background: typing.Optional[BackgroundTask] = None, 
            filename: str = "NoName") -> None:
        super().__init__(content, status_code, headers, media_type, background)
        self.raw_headers.append((b"Content-Disposition",f"attachment; filename={filename}.xlsx".encode('latin-1')))


@router.get("/", tags=['commissions'])
async def get_commission_data():
    return {"data": json.loads(api.commission_data_with_all_names().to_json(orient="records"))}

@router.get("/download", tags=['commissions'], response_class=ExcelFileResponse)
async def download_commission_data():
    bfile = BytesIO()
    with ExcelWriter(bfile) as file:
        api.commission_data_with_all_names().to_excel(file,sheet_name="data",index=False)
    bfile.seek(0)
    return ExcelFileResponse(content=bfile, filename="commissions")

@router.post("/", tags=['commissions'])
async def process_data_from_a_file(
        file: bytes = File(), 
        reporting_month: int = Form(),
        reporting_year: int = Form(), 
        report_id: int=Form(),
        manufacturer_id: int = Form()
    ):
    existing_submissions = api.get_all_submissions()
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
    file_obj = CommissionFile(file,"Detail")
    new_sub = submission.NewSubmission(file_obj,reporting_month,reporting_year,report_id,manufacturer_id)
    mfg_preprocessor = adp.ADPPreProcessor
    mfg_report_processor = report_processor.ReportProcessor(mfg_preprocessor,new_sub,db)
    await mfg_report_processor.process_and_commit()
    return {"sub_id": mfg_report_processor.submission_id,
        "steps":json.loads(api.get_processing_steps(mfg_report_processor.submission_id).to_json(orient="records")),
        "errors":json.loads(api.get_errors(mfg_report_processor.submission_id).to_json(orient="records"))}

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
