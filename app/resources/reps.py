from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.jsonapi import Query, convert_to_jsonapi

from services.api_adapter import ApiAdapter

api = ApiAdapter()
router = APIRouter(prefix="/representatives")

def get_db():
    db = api.SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.get("", tags=["reps"])
async def all_reps(query: Query=Depends(), db: Session=Depends(get_db)):
    try:
        jsonapi_query = convert_to_jsonapi(query)
    except Exception as err:
        raise HTTPException(status_code=400,detail=str(err))
    return api.get_many_reps_jsonapi(db,jsonapi_query)

@router.get("/{rep_id}", tags=["reps"])
async def rep_by_id(rep_id: int, query: Query=Depends(), db: Session=Depends(get_db)):
    try:
        jsonapi_query = convert_to_jsonapi(query)
    except Exception as err:
        raise HTTPException(status_code=400,detail=str(err))
    return api.get_rep_jsonapi(db, rep_id, jsonapi_query)

# TODO implement these routes in JSON:API

# @router.post("/", tags=["reps"])
# async def add_new_rep(new_rep: NewRepresentative):
#     existing_reps = api.get_all_reps()
#     existing_rep = existing_reps.loc[
#         (existing_reps.first_name == new_rep.first_name)
#         &(existing_reps.last_name == new_rep.last_name),"id"
#     ]
#     if not existing_rep.empty:
#         raise HTTPException(400, f"rep already exists with id {existing_rep.squeeze()}")
#     return {"new id": api.set_new_rep(**new_rep.dict())}

# @router.put("/{rep_id}", tags=["reps"])
# async def modify_a_rep(rep_id: int, representative: ExistingRepresentative):
#     existing_rep = api.get_a_rep(rep_id)
#     if existing_rep.empty:
#         raise HTTPException(400, f"rep with id {rep_id} does not exist")
#     api.modify_rep(rep_id, **representative.dict(exclude_none=True))

# @router.delete("/{rep_id}", tags=["reps"])
# async def delete_rep(rep_id: int):
#     # soft delete
#     api.delete_rep(rep_id)