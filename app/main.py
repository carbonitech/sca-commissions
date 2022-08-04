from fastapi import FastAPI, File
from starlette.responses import RedirectResponse

import json
from app import error_listener, process_step_listener, report_processor
from db.db_services import DatabaseServices
from db.models import Base
from entities import submission
from entities import manufacturers
from entities.manufacturers import adp
from entities.commission_file import CommissionFile

import os
import pandas as pd
from db import models
from sqlalchemy.orm import Session

app = FastAPI()
db = DatabaseServices()

error_listener.setup_error_event_handlers()
process_step_listener.setup_processing_step_handlers()

@app.get("/")
async def home():
    return RedirectResponse("http://127.0.0.1:8000/docs")

@app.get("/customers")
async def all_customers():
    """Get all customers"""
    customers = db.get_customers().to_json(orient="records")
    return({"customers": json.loads(customers)})

@app.get("/customers/{customer_id}")
async def customer_by_id(customer_id):
    """Get customer by id"""
    customer = db.get_customer(customer_id).to_json(orient="records")
    return({"customer": json.loads(customer)})

@app.get("/customers/{customer_id}/branches")
async def customer_branches_by_id(customer_id):
    """Get all branches for a customer by customer id"""
    branches = db.get_branches_by_customer(customer_id).to_json(orient="records")
    return({"branches": json.loads(branches)})

@app.post("/customers")
async def new_customer():
    """create a new customer"""
    pass

@app.get("/manufacturers")
async def all_manufacturers():
    manufacturers_ = db.get_all_manufacturers().to_json(orient="records")
    return({"manufacturers": json.loads(manufacturers_)})


@app.get("/manufacturers/{manuf_id}")
async def manufacturer_by_id(manuf_id: int):
    manufacturer_reports = db.get_manufacturer_by_id(manuf_id).to_json(orient="records")
    return({"manufacturer_details": json.loads(manufacturer_reports)})


@app.post("/commdata")
async def process_data(file: bytes = File()):
    file_obj = CommissionFile(file,"Detail")
    new_sub = submission.NewSubmission(file=file_obj,reporting_month=5,reporting_year=2022,report_id=1,manufacturer_id=1)
    mfg_preprocessor = adp.ADPPreProcessor
    db = DatabaseServices()
    mfg_report_processor = report_processor.ReportProcessor(mfg_preprocessor,new_sub,db)
    mfg_report_processor.process_and_commit()
    return {"sub_id": mfg_report_processor.submission_id,
        "steps":json.loads(db.get_processing_steps(mfg_report_processor.submission_id).to_json(orient="records")),
        "errors":json.loads(db.get_errors(mfg_report_processor.submission_id).to_json(orient="records"))}




@app.get("/deldb")
async def delete_db():
    Base.metadata.drop_all(DatabaseServices.engine)
    return {"message": "tables deleted"}

@app.get("/makedb")
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