import os
import json
from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.security import HTTPBearer
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse, RedirectResponse, JSONResponse
from jose.jwt import get_unverified_header, decode

import requests
import pandas as pd
from numpy import nan
from sqlalchemy.orm import Session

from app import resources
from app.listeners import api_adapter_listener, error_listener, process_step_listener
from db import models
from db.db_services import DatabaseServices
from db.models import Base


app = FastAPI()
token_auth_scheme = HTTPBearer()
AUTH0_DOMAIN = os.getenv('AUTH0_DOMAIN')
ALGORITHMS = os.getenv('ALGORITHMS')
AUDIENCE = os.getenv('AUDIENCE')
ORIGINS = os.getenv('ORIGINS')
ORIGINS_REGEX = os.getenv('ORIGINS_REGEX')

app.add_middleware(
    CORSMiddleware,
    allow_origins=ORIGINS,
    allow_origin_regex=ORIGINS_REGEX,
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*']
)

@app.middleware('http')
async def format_errors_to_jsonapi_spec(request: Request, call_next):
    """
    middleware detecting whether an error response is coming back from the request call
    if an error code within the 400-599 range is detected, return a JSON response formatted
    to the JSON:API specification, otherwise return the response as-is
    """


    async def read_body(iterator) -> dict:
        """
        builds the full content body of the response from the StreamingResponse
        bytes -> dict
        Returns: dict
        """
        return json.loads("".join([data.decode() async for data in iterator]))
        
    response: StreamingResponse = await call_next(request)
    if 400 > response.status_code >= 200:
        return response
    
    resp_body = await read_body(response.body_iterator)
    resp_body.update({"status":response.status_code})
    jsonapi_err_response_content = {"errors":[]}

    # output of patch requests errors is already to spec
    # here it's extracted from the detail field supplied to HTTPException
    match resp_body:
        case {"detail":{"errors": [*error_objs]}}:
            # error object response
            jsonapi_err_response_content = resp_body["detail"]
            return JSONResponse(
                content=jsonapi_err_response_content,
                status_code=response.status_code,
                media_type="application/json"
            )
    

    if response.status_code == 422:
        for err in resp_body["detail"]:
            err_detail: str = err["msg"]
            if len(err["loc"]) > 1:
                err_field: str = err["loc"][1]
            else:
                err_field: str = err["loc"][0]
            err_detail = err_detail.replace("value", err_field)
            err_title = err["type"]
            jsonapi_err_response_content["errors"].append(
                {
                    "status": response.status_code,
                    "detail": err_detail,
                    "title": err_title,
                    "field": err_field
                })
    elif response.status_code >= 400:
        jsonapi_err_response_content["errors"].append(resp_body)

    return JSONResponse(
        content=jsonapi_err_response_content,
        status_code=response.status_code,
        media_type="application/json"
    )


def authenticate_auth0_token(token: str = Depends(token_auth_scheme)):
    error = None
    token_cred = token.credentials
    jwks = requests.get(AUTH0_DOMAIN+"/.well-known/jwks.json").json()
    try:
        unverified_header = get_unverified_header(token_cred)
    except Exception as err:
        error = err
    else:
        rsa_key = {}
        for key in jwks["keys"]:
            if key["kid"] == unverified_header.get("kid"):
                rsa_key = {
                    "kty": key["kty"],
                    "kid": key["kid"],
                    "use": key["use"],
                    "n": key["n"],
                    "e": key["e"]             
                }
        if rsa_key:
            try:
                payload = decode(
                    token_cred,
                    rsa_key,
                    algorithms=ALGORITHMS,
                    audience=AUDIENCE
                )
            except Exception as err:
                error = err
            else:
                return payload
        else:
            error = "No RSA key found in JWT Header"
    raise HTTPException(status_code=401, detail=str(error))

        

PROTECTED = [Depends(authenticate_auth0_token)]

app.include_router(resources.relationships, dependencies=PROTECTED)
app.include_router(resources.customers, dependencies=PROTECTED)
app.include_router(resources.reps, dependencies=PROTECTED)
app.include_router(resources.cities, dependencies=PROTECTED)
app.include_router(resources.states, dependencies=PROTECTED)
app.include_router(resources.branches, dependencies=PROTECTED)
app.include_router(resources.submissions, dependencies=PROTECTED)
app.include_router(resources.commissions, dependencies=PROTECTED)
app.include_router(resources.manufacturers, dependencies=PROTECTED)
app.include_router(resources.downloads)

error_listener.setup_error_event_handlers()
process_step_listener.setup_processing_step_handlers()
api_adapter_listener.setup_api_event_handlers()


@app.get("/")
async def home():
    return RedirectResponse("/docs")

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
            'manufacturers': models.Manufacturer,
            'manufacturers_reports': models.ManufacturersReport,
            'representatives': models.Representative,
            'map_customer_name': models.MapCustomerName,
            'map_city_names': models.MapCityName,
            'map_state_names': models.MapStateName,
            'report_submissions_log': models.Submission,
            'report_processing_steps_log': models.ProcessingStep,
            'current_errors': models.Error,
            'final_commission_data': models.CommissionData,
            'file_downloads': models.FileDownloads
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
async def reset_database(token: str = PROTECTED[0]):
    result_del = await delete_db()
    result_make = await create_db()
    return [result_del, result_make]