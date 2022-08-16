from fastapi import APIRouter, HTTPException, Form
from pydantic import BaseModel
import json
from services.api_adapter import ApiAdapter

api = ApiAdapter()
router = APIRouter(prefix="/cities")

class City(BaseModel):
    name: str

@router.get("/", tags=["cities"])
async def get_all_cities():
    result = api.get_cities().to_json(orient="records")
    return json.loads(result)

@router.get("/{city_id}", tags=["cities"])
async def get_city_by_id(city_id: int):
    result = api.get_cities(city_id).to_json(orient="records")
    return json.loads(result)

@router.post("/", tags=["cities"])
async def add_a_city(city: City):
    city.name = city.name.upper()
    api.new_city(**city.dict())

@router.put("/{city_id}", tags=["cities"])
async def modify_a_city(city_id: int):
    raise NotImplementedError

@router.delete("/{city_id}", tags=["cities"])
async def delete_a_city(city_id: int):
    raise NotImplementedError