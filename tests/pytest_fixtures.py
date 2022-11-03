import os
from pytest import fixture
from pandas import read_csv
from numpy import nan
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db import models
from db.models import Base
from app.main import app
from app.auth import authenticate_auth0_token
from services.api_adapter import get_db
from random import randint

load_dotenv()
TESTING_DATABASE_URL = os.getenv("TESTING_DATABASE_URL").replace("postgres://","postgresql://")

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


class RequestBodyRelationship:
    def __init__(self, resource_name: str):
        self.resource_name = resource_name
        setattr(self,resource_name,{"data": {"type": resource_name, "id": 0}})

    def keys(self) -> list:
        return list(self.__dict__.keys())

    def set_id(self, value: int):
        attr = self.__getitem__(self.resource_name)
        attr["data"]["id"] = value
    
    def __getitem__(self, key):
        return getattr(self, key, None)

    def dict(self) -> dict:
        return {k:self.__getitem__(k) for k in self.keys() if self.__getitem__(k)}


class RequestBody:
    def __init__(self, resource_type: str, attributes: dict, relationships: RequestBodyRelationship=None):
        self.id = randint(1,50)
        self.type = resource_type
        self.attributes = attributes
        self.relationships = relationships
        if relationships:
            self.relationships.set_id(self.id)
            self.relationships = self.relationships.dict()
    
    def keys(self) -> list:
        return list(self.__dict__.keys())
    
    def __getitem__(self, key):
        return getattr(self, key, None)

    def dict(self) -> dict:
        return {"data": {k:self.__getitem__(k) for k in self.keys() if self.__getitem__(k)}}

@fixture(scope="module")
def database():
    """
    Note: pytest fixures do set-up up to the yield statement, run the test, and then run the rest.
    This fixtures sets up the database in a postgres database called "testing" using CSV files.
    All table data is dropped after testing
    """
    Base.metadata.create_all(bind=engine)
    DB_TABLES = {
                'cities': models.City,
                'states': models.State,
                'customers': models.Customer,
                'customer_branches': models.CustomerBranch,
                'manufacturers': models.Manufacturer,
                'manufacturers_reports': models.ManufacturersReport,
                'representatives': models.Representative,
                "report_form_fields": models.ReportFormFields
        }
    # load csv files
    tables_dir = './tests/db_tables'
    files = os.listdir(tables_dir)
    tables = {
        file[:-4]: read_csv(os.path.join(tables_dir,file))
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
    map_customer_stmt = "INSERT INTO map_customer_names (recorded_name, customer_id) SELECT name,id FROM customers WHERE deleted IS NULL;"
    map_city_stmt = "INSERT INTO map_city_names (recorded_name, city_id) SELECT name,id FROM cities WHERE deleted IS NULL;"
    map_state_stmt = "INSERT INTO map_state_names (recorded_name, state_id) SELECT name,id FROM states WHERE deleted IS NULL;"
    for map_fill_stmt in [map_customer_stmt, map_city_stmt, map_state_stmt]:
        session.execute(map_fill_stmt)
    session.commit()

    yield session

    # cleanup
    Base.metadata.drop_all(bind=engine)

