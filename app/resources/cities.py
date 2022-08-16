from fastapi import APIRouter, HTTPException, Form
from pydantic import BaseModel, validator
import json
from services.api_adapter import ApiAdapter

api = ApiAdapter()
router = APIRouter(prefix="/cities")

class City(BaseModel):
    name: str

    @validator('name')
    def name_to_uppercase(cls, value: str):
        return value.strip().upper()

@router.get("/", tags=["cities"])
async def get_all_cities():
    result = api.get_cities().to_json(orient="records", date_format="iso")
    return json.loads(result)

@router.get("/{city_id}", tags=["cities"])
async def get_city_by_id(city_id: int):
    result = api.get_cities(city_id).to_json(orient="records", date_format="iso")
    return json.loads(result)

@router.post("/", tags=["cities"])
async def add_a_city(city: City):
    existing_cities = api.get_cities()
    if not existing_cities.loc[existing_cities.name == city.name].empty:
        raise HTTPException(400, detail="City already in table")
    api.new_city(**city.dict())

@router.put("/{city_id}", tags=["cities"])
async def modify_a_city(city_id: int, updated_city: City):
    api.modify_city(city_id, **updated_city.dict())

@router.delete("/{city_id}", tags=["cities"])
async def delete_a_city(city_id: int):
    api.delete_city_by_id(city_id)