from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from services.api_adapter import ApiAdapter

api = ApiAdapter()
router = APIRouter()

def get_db():
    db = api.SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/{primary}/{id_}/{secondary}", tags=["relationships"])
def get_related_handler(primary:str, id_:int, secondary:str, db: Session=Depends(get_db)):
    return api.get_related(db,primary,id_,secondary)

@router.get("/{primary}/{id_}/relationships/{secondary}", tags=["relationships"])
def get_self_relationship_handler(primary:str, id_:int, secondary:str, db: Session=Depends(get_db)):
    raise HTTPException(status_code=501, detail="self relationship links not implemented")