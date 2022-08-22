from fastapi import FastAPI, Depends, HTTPException, Security, Request
from fastapi.security.api_key import APIKeyHeader
from starlette.responses import RedirectResponse

import os
import pandas as pd
from sqlalchemy.orm import Session
from passlib.hash import bcrypt_sha256

from app import error_listener, process_step_listener, resources
from db import models
from db.db_services import DatabaseServices
from db.models import Base


app = FastAPI()
db = DatabaseServices()
api_key_header = APIKeyHeader(name="access_token", auto_error=True)

API_KEY_HASH = os.getenv('API_KEY_HASH')
ALLOWED_HOSTS = os.getenv('ALLOWED_HOSTS', ['127.0.0.1'])

def verify_api_key(provided_key, hashed_key):
    return bcrypt_sha256.verify(provided_key, hashed_key)

def get_key_hash(api_key):
    return bcrypt_sha256.hash(api_key)

async def authenticate_header_and_host(request: Request, api_key: str = Security(api_key_header)):
    if verify_api_key(api_key, API_KEY_HASH) and request.client.host in ALLOWED_HOSTS:
        return True
    raise HTTPException(status_code=401, detail="Unauthorized")

PROTECTED = [Depends(authenticate_header_and_host)]

app.include_router(resources.customers, dependencies=PROTECTED)
app.include_router(resources.mappings, dependencies=PROTECTED)
app.include_router(resources.branches, dependencies=PROTECTED)
app.include_router(resources.manufacturers, dependencies=PROTECTED)
app.include_router(resources.reps, dependencies=PROTECTED)
app.include_router(resources.commissions, dependencies=PROTECTED)
app.include_router(resources.submissions, dependencies=PROTECTED)
app.include_router(resources.cities, dependencies=PROTECTED)
app.include_router(resources.states, dependencies=PROTECTED)

error_listener.setup_error_event_handlers()
process_step_listener.setup_processing_step_handlers()

@app.get("/")
async def home():
    return RedirectResponse("http://127.0.0.1:8000/docs")

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
            data = data.replace({nan: None})
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