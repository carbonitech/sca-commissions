from fastapi import APIRouter, HTTPException, Form, File
import json

from db import db_services
from app import report_processor
from entities import submission
from entities.manufacturers import adp
from entities.commission_file import CommissionFile

db = db_services.DatabaseServices()
db_views = db_services.TableViews()
router = APIRouter(prefix="/commissions")

@router.get("/")
async def get_commission_data():
    return {"data": json.loads(db_views.commission_data_with_all_names().to_json(orient="records"))}

@router.post("/")
async def process_data(file: bytes = File(), reporting_month: int = Form(),
        reporting_year: int = Form(), report_id: int=Form(), manufacturer_id: int = Form()):
    file_obj = CommissionFile(file,"Detail")
    new_sub = submission.NewSubmission(file_obj,reporting_month,reporting_year,report_id,manufacturer_id)
    mfg_preprocessor = adp.ADPPreProcessor
    mfg_report_processor = report_processor.ReportProcessor(mfg_preprocessor,new_sub,db)
    await mfg_report_processor.process_and_commit()
    return {"sub_id": mfg_report_processor.submission_id,
        "steps":json.loads(db.get_processing_steps(mfg_report_processor.submission_id).to_json(orient="records")),
        "errors":json.loads(db.get_errors(mfg_report_processor.submission_id).to_json(orient="records"))}
