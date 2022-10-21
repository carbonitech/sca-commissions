import os

from sqlalchemy.orm import Session
import pandas as pd
from numpy import nan
from fastapi import APIRouter

from db import models
from db.db_services import DatabaseServices
from db.models import Base

router = APIRouter(prefix="/resetdb")

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
        map_customer_stmt = "INSERT INTO map_customer_names (recorded_name, customer_id) SELECT name,id FROM customers WHERE deleted IS NULL;"
        map_city_stmt = "INSERT INTO map_city_names (recorded_name, city_id) SELECT name,id FROM cities WHERE deleted IS NULL;"
        map_state_stmt = "INSERT INTO map_state_names (recorded_name, state_id) SELECT name,id FROM states WHERE deleted IS NULL;"
        for map_fill_stmt in [map_customer_stmt, map_city_stmt, map_state_stmt]:
            session.execute(map_fill_stmt)
        session.commit()
    return {"message": "tables created"}

@router.get("")
async def reset_database():
    result_del = await delete_db()
    result_make = await create_db()
    return [result_del, result_make]