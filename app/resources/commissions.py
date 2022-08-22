from io import BytesIO
import typing
import json
from fastapi import APIRouter, HTTPException, Form, File
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

db = db_services.DatabaseServices()
api = ApiAdapter()
router = APIRouter(prefix="/commissions")

class CustomCommissionData(BaseModel):
    map_rep_customer_id: int
    inv_amt: float
    comm_amt: float

    @validator('map_rep_customer_id')
    def rep_customer_id_exists(cls, value):
        exists = api.rep_customer_id_exists(value)
        if not exists:
            raise ValueError('Rep-to-Customer Relationship Does Not Exist')
        return value

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
async def process_data_from_a_file(file: bytes = File(), reporting_month: int = Form(),
        reporting_year: int = Form(), report_id: int=Form(), manufacturer_id: int = Form()):
    file_obj = CommissionFile(file,"Detail")
    new_sub = submission.NewSubmission(file_obj,reporting_month,reporting_year,report_id,manufacturer_id)
    mfg_preprocessor = adp.ADPPreProcessor
    mfg_report_processor = report_processor.ReportProcessor(mfg_preprocessor,new_sub,db)
    await mfg_report_processor.process_and_commit()
    return {"sub_id": mfg_report_processor.submission_id,
        "steps":json.loads(api.get_processing_steps(mfg_report_processor.submission_id).to_json(orient="records")),
        "errors":json.loads(api.get_errors(mfg_report_processor.submission_id).to_json(orient="records"))}

@router.post("/{submission_id}", tags=['commissions'])
async def add_custom_entry_to_commission_data(submission_id: int, data: CustomCommissionData, description: str):
    if not api.submission_exists(submission_id):
        raise HTTPException(400, detail="report submission does not exist")
    row_id = api.set_new_commission_data_entry(submission_id=submission_id, **data.dict())
    return {row_id: description}
    

@router.put("/{row_id}", tags=['commissions'])
async def modify_an_entry_in_commission_data(row_id: int, data: CustomCommissionData):
    row_exists = api.get_commission_data_by_row(row_id)
    if not row_exists:
        raise HTTPException(400, detail="row does not exist")
    api.modify_commission_data_row(row_id, **data.dict())

@router.delete("/{row_id}", tags=['commissions'])
async def remove_a_line_in_commission_data(row_id: int):
    # hard delete
    api.delete_commission_data_line(row_id)
