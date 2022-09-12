from fastapi import APIRouter, Depends
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

@router.get("/{primary}/{id_}/{secondary}")
def get_related_handler(primary:str, id_:int, secondary:str, db: Session=Depends(get_db)):
    return api.get_related(db,primary,id_,secondary)