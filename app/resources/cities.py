from fastapi import APIRouter, HTTPException, Form
from pydantic import BaseModel
import json
from db import db_services

db = db_services.DatabaseServices()
router = APIRouter(prefix="/cities")

@router.get("/", tags=["cities"])
async def get_all_cities():
    result = db.get_cities().to_json(orient="records")
    return json.loads(result)

@router.get("/{city_id}", tags=["cities"])
async def get_city_by_id(city_id: int):
    result = db.get_cities(city_id).to_json(orient="records")
    return json.loads(result)

@router.post("/", tags=["cities"])
async def add_a_city(city_name: str):
    city_name = city_name.upper()
    pass

@router.put("/{city_id}", tags=["cities"])
async def modify_a_city(city_id: int):
    pass

@router.delete("/{city_id}", tags=["cities"])
async def delete_a_city(city_id: int):
    pass