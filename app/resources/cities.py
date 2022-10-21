from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from services.api_adapter import ApiAdapter
from app.jsonapi import Query, convert_to_jsonapi, JSONAPIRoute

api = ApiAdapter()
router = APIRouter(prefix="/cities", route_class=JSONAPIRoute)

def get_db():
    db = api.SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("", tags=["cities"])
async def get_all_cities(query: Query=Depends(), db: Session=Depends(get_db)):
    try:
        jsonapi_query = convert_to_jsonapi(query)
    except Exception as err:
        raise HTTPException(status_code=400,detail=str(err))
    cities = api.get_many_cities_jsonapi(db,jsonapi_query)
    return cities

@router.get("/{city_id}", tags=["cities"])
async def get_city_by_id(city_id: int, query: Query=Depends(), db: Session=Depends(get_db)):
    try:
        jsonapi_query = convert_to_jsonapi(query)
    except Exception as err:
        raise HTTPException(status_code=400,detail=str(err))
    customer = api.get_city_jsonapi(db, city_id, jsonapi_query)
    return customer

# TODO implement these routes in JSON:API

# @router.post("/", tags=["cities"])
# async def add_a_city(city:City = Depends(City.as_form)):
#     all_cities = api.get_cities()
#     existing_city = all_cities.loc[all_cities.name == city.name]

#     if existing_city.empty:
#         return api.new_city(**city.dict())
#     elif existing_city["deleted"].squeeze():
#         existing_id = int(existing_city["id"].squeeze())
#         return api.reactivate_city(existing_id)
    
#     existing_id = int(existing_city["id"].squeeze())
#     raise HTTPException(400, detail=f"City exists with id {existing_id}")

# @router.put("/{city_id}", tags=["cities"])
# async def modify_a_city(city_id: int, updated_city: City = Depends(City.as_form)):
#     existing_city = api.get_cities(city_id)
#     if existing_city.empty:
#         raise HTTPException(400, detail=f"City with id {city_id} does not exist")
#     api.modify_city(city_id, **updated_city.dict())

# @router.delete("/{city_id}", tags=["cities"])
# async def delete_a_city(city_id: int):
#     # soft delete
#     api.delete_city_by_id(city_id)

# @router.post("/{city_id}/mappings", tags=["cities"])
# async def create_new_mapping_for_a_city(city_id: int, new_mapping: str = Form()):
#     current_city = api.get_cities(city_id)
#     if current_city.empty:
#         raise HTTPException(400, detail="City name does not exist")
#     value = {"city_id": city_id, "recorded_name": new_mapping}
#     api.set_city_name_mapping(**value)

# @router.put("/{city_id}/mappings/{mapping_id}", tags=["cities"])
# async def modify_mapping_for_a_city(city_id: int, mapping_id: int, modified_mapping: str = Form()):
#     city = api.get_cities(city_id)
#     if city.empty:
#         raise HTTPException(400, detail=f"City with id {city_id} does not exist")
#     all_mappings = api.get_all_city_name_mappings(city_id)
#     mapping_ids = all_mappings.loc[:,"mapping_id"]
#     if mapping_id not in mapping_ids.values:
#         raise HTTPException(400, detail=f"this mapping does not exist for city name {city.name.squeeze()}")
#     value = {"city_id": city_id, "recorded_name": modified_mapping}
#     api.modify_city_name_mapping(mapping_id,**value)
