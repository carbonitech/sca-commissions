import os
import pytest
import pandas as pd
from numpy import nan
from fastapi.testclient import TestClient
from app.main import app, authenticate_auth0_token
from random import randint
from json import dumps
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db import models
from db.models import Base
from services.api_adapter import get_db
from dotenv import load_dotenv

load_dotenv()

TESTING_DATABASE_URL = os.getenv("TESTING_DATABASE_URL").replace("postgres://","postgresql://")

test_client = TestClient(app)

engine = create_engine(TESTING_DATABASE_URL)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base.metadata.create_all(bind=engine)

def override_get_db():
    try:
        db = TestingSessionLocal()
        yield db
    finally:
        db.close()

def skip_authentication():
    return

def set_overrides():
    app.dependency_overrides[authenticate_auth0_token] = skip_authentication
    app.dependency_overrides[get_db] = override_get_db

def clear_overrides():
    app.dependency_overrides = {}

@pytest.fixture
def database():
    """
    Note: pytest fixures do set-up up to the yield statement, run the test, and then run the rest.
    This fixtures sets up the database in a postgres database called "testing" using CSV files.
    All table data is dropped after testing
    """
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

    session = next(override_get_db())
    # populate database with csv data
    for table, data in tables.items():
        data = data.replace({nan: None})
        for row in data.to_dict("records"):
            # col names in csv must match table schema
            session.add(DB_TABLES[table](**row)) 
    session.commit()
    yield

    # cleanup
    Base.metadata.drop_all(bind=engine)


def test_customers_patch(database):
    set_overrides()
    test_id = randint(1,50)
    request_body = {
        "data": {
            "type": "customers",
            "id": test_id,
            "attributes": {
                "name": "SHASCO INC"
            }
        }
    }
    response = test_client.patch(f"/customers/{test_id}", data=dumps(request_body))
    hit = False
    match response.json():
        case {"jsonapi": a, "meta": b, "data": c}:
            hit = True

    assert hit, f"{response.json()}"



