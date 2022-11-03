import os

from sqlalchemy.orm import Session
from sqlalchemy.orm.session import close_all_sessions
import pandas as pd
from numpy import nan
from fastapi import APIRouter, Depends

from db import models
from db.models import Base
from services.api_adapter import get_db, ApiAdapter

router = APIRouter(prefix="/resetdb")

async def delete_db(engine):
    Base.metadata.drop_all(engine)
    return {"message": "tables deleted"}

async def create_db(engine, session: Session):
    close_all_sessions() # !!!! Only became a need when I added ReportFormFields, over a certain number of columns. adding 1 col at a time worked fine
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    DB_TABLES = {
            'cities': models.City,
            'states': models.State,
            'customers': models.Customer,
            'customer_branches': models.CustomerBranch,
            'manufacturers': models.Manufacturer,
            'manufacturers_reports': models.ManufacturersReport,
            'representatives': models.Representative,
            'report_form_fields': models.ReportFormFields
    }
    # load csv files 
    tables_dir = './tests/db_tables'
    files = os.listdir(tables_dir)[::-1] # not sure why the ordering changed
    tables = {
        file[:-4]: pd.read_csv(os.path.join(tables_dir,file))
        for file in files
    }

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
    return {"message": "tables created"}

@router.get("")
async def reset_database(db: Session=Depends(get_db)):
    engine = ApiAdapter.engine
    # result_del = await delete_db(engine)
    result_make = await create_db(engine,db)
    return result_make