from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from services.api_adapter import ApiAdapter, get_db
from sqlalchemy_jsonapi.errors import BaseError
from app.jsonapi import format_error

api = ApiAdapter()
router = APIRouter()

@router.get("/{primary}/{id_}/{secondary}", tags=["relationships"])
def get_related_handler(primary:str, id_:int, secondary:str, db: Session=Depends(get_db)):
    try:
        return api.get_related(db,primary,id_,secondary)
    except BaseError as err:
        raise HTTPException(**format_error(err))   

@router.get("/{primary}/{id_}/relationships/{secondary}", tags=["relationships"])
def get_self_relationship_handler(primary:str, id_:int, secondary:str, db: Session=Depends(get_db)):
    try:
        return api.get_relationship(db,primary,id_,secondary)
    except BaseError as err:
        raise HTTPException(**format_error(err))