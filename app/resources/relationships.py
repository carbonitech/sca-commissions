from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from services.api_adapter import ApiAdapter, get_db, User, get_user

api = ApiAdapter()
router = APIRouter()

@router.get("/{primary}/{id_}/{secondary}", tags=["relationships"])
def get_related_handler(primary:str, id_:int, secondary:str, db: Session=Depends(get_db), user: User=Depends(get_user)):
    return api.get_related(db,primary,id_,secondary, user)

@router.get("/{primary}/{id_}/relationships/{secondary}", tags=["relationships"])
def get_self_relationship_handler(primary:str, id_:int, secondary:str, db: Session=Depends(get_db), user: User=Depends(get_user)):
    return api.get_relationship(db,primary,id_,secondary, user)