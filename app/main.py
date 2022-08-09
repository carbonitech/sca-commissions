from fastapi import FastAPI, File, HTTPException, Form
from pydantic import BaseModel
from starlette.responses import RedirectResponse

import json
from app import error_listener, process_step_listener, report_processor
from db.db_services import DatabaseServices, TableViews
from db.models import Base
from entities import submission
from entities.manufacturers import adp
from entities.commission_file import CommissionFile

import os
import pandas as pd
from db import models
from sqlalchemy.orm import Session

from app.resources import customers, mappings

app = FastAPI()
db = DatabaseServices()
db_views = TableViews()

app.include_router(customers.router)
app.include_router(mappings.router)

class Branch(BaseModel):
    customer_id: int
    city_id: int
    state_id: int

class CommissionDataForm(BaseModel):
    reporting_month: int
    reporting_year: int
    report_id: int
    manufacturer_id: int
    sheet_name: str


error_listener.setup_error_event_handlers()
process_step_listener.setup_processing_step_handlers()

### HOME ###
@app.get("/")
async def home():
    return RedirectResponse("http://127.0.0.1:8000/docs")


### BRANCHES ###
@app.post("/branches")
async def new_branch_by_customer_id(new_branch: Branch):
    existing_branches = db.get_customer_branches_raw(new_branch.customer)
    exist_check = existing_branches[
        (existing_branches["customer_id"] == new_branch.customer_id)
        & (existing_branches["city_id"] == new_branch.city_id)
        & (existing_branches["state_id"] == new_branch.state_id)
    ].empty
    if exist_check:
        return db.set_new_customer_branch_raw(**new_branch.dict())
    else:
        raise HTTPException(status_code=400, detail="Customer Branch already exists")

@app.delete("/branches")
async def delete_branch_by_id(branch_id: int):
    return db.delete_a_branch_by_id(branch_id=branch_id)


### MANUFACTURERS ###
@app.get("/manufacturers")
async def all_manufacturers():
    manufacturers_ = db.get_all_manufacturers().to_json(orient="records")
    return({"manufacturers": json.loads(manufacturers_)})


@app.get("/manufacturers/{manuf_id}")
async def manufacturer_by_id(manuf_id: int):
    manufacturer, reports, submissions = db.get_manufacturer_by_id(manuf_id)
    manufacturer_json = json.loads(manufacturer.to_json(orient="records"))
    reports_json = json.loads(reports.to_json(orient="records"))
    submissions_json = json.loads(submissions.to_json(orient="records", date_format="iso"))
    return({"manufacturer_details": manufacturer_json,
            "reports": reports_json,
            "submissions": submissions_json})

### REPS ###
@app.get("/reps")
async def get_all_reps():
    all_reps = db.get_all_reps().to_json(orient="records", date_format="iso")
    return {"data": json.loads(all_reps)}

@app.get("/reps/{rep_id}")
async def get_rep_by_id(rep_id: int):
    rep_and_branches = db.get_rep_and_branches(rep_id).to_json(orient="records")
    return {"data": json.loads(rep_and_branches)}


### COMMISSIONS ###
@app.get("/commissions")
async def get_commission_data():
    return {"data": json.loads(db_views.commission_data_with_all_names().to_json(orient="records"))}

@app.post("/commissions")
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


### SUBMISSIONS ###
@app.get("/submissions")
async def get_all_submissions():
    all_subs = db.get_all_submissions().to_json(orient="records", date_format="iso")
    return {"data": json.loads(all_subs)}

@app.get("/submissions/{submission_id}")
async def get_submission_by_id(submission_id: int):
    sub_data, process_steps, current_errors = db.get_submission_by_id(submission_id)
    return {
        "submission": json.loads(sub_data.to_json(orient="records", date_format="iso"))[0],
        "processing_steps": json.loads(process_steps.to_json(orient="records", date_format="iso")),
        "current_errors": json.loads(current_errors.to_json(orient="records", date_format="iso"))
    }


async def delete_db():
    Base.metadata.drop_all(DatabaseServices.engine)
    return {"message": "tables deleted"}

async def create_db():
    Base.metadata.create_all(DatabaseServices.engine)
    DB_TABLES = {
            'cities': models.City,
            'states': models.State,
            'customers': models.Customer,
            'customer_branches': models.CustomerBranch,
            'manufacturers': models.ManufacturerDTO,
            'manufacturers_reports': models.ManufacturersReport,
            'representatives': models.Representative,
            'map_customer_name': models.MapCustomerName,
            'map_city_names': models.MapCityName,
            'map_state_names': models.MapStateName,
            'map_reps_customers': models.MapRepToCustomer,
            'report_submissions_log': models.SubmissionDTO,
            'report_processing_steps_log': models.ProcessingStepDTO,
            'current_errors': models.ErrorDTO,
            'final_commission_data': models.FinalCommissionDataDTO
    }
    # load csv files
    tables_dir = './tests/db_tables'
    files = os.listdir(tables_dir)
    tables = {
        file[:-4]: pd.read_csv(os.path.join(tables_dir,file))
        for file in files
    }

    # populate database with csv data
    with Session(DatabaseServices.engine) as session:
        for table, data in tables.items():
            for row in data.to_dict("records"):
                # col names in csv must match table schema
                session.add(DB_TABLES[table](**row)) 
        session.commit()
    return {"message": "tables created"}

@app.get("/resetdb")
async def reset_database():
    result_del = await delete_db()
    result_make = await create_db()
    return [result_del, result_make]