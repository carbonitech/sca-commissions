from fastapi import FastAPI, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from starlette.responses import RedirectResponse

import os
import pandas as pd
from numpy import nan
from sqlalchemy.orm import Session

from app import error_listener, process_step_listener, resources
from db import models
from db.db_services import DatabaseServices
from db.models import Base


app = FastAPI()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

db = DatabaseServices()

app.include_router(resources.customers)
app.include_router(resources.mappings)
app.include_router(resources.branches)
app.include_router(resources.manufacturers)
app.include_router(resources.reps)
app.include_router(resources.commissions)
app.include_router(resources.submissions)
app.include_router(resources.cities)
app.include_router(resources.states)

error_listener.setup_error_event_handlers()
process_step_listener.setup_processing_step_handlers()


@app.post("/token")
async def authenticate_requestoor(some_data: OAuth2PasswordRequestForm = Depends()):
    if some_data.username == "example":
        if some_data.password == "password":
            return {'access_token': f"{some_data.username}+token", 'token_type': "bearer"}
    raise HTTPException(status_code=401, detail="Incorrect Credentials")

### HOME ###
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
async def reset_database(token: str = Depends(oauth2_scheme)):
    result_del = await delete_db()
    result_make = await create_db()
    return [result_del, result_make]