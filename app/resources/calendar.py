
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from services import get
from services.get import ReportCalendar
from services.utils import User, get_db, get_user

router = APIRouter(prefix="/report-calendar", tags=['report-calendar'])

@router.get("")
def report_calendar(
        db: Session=Depends(get_db),
        user: User=Depends(get_user)
    ) -> ReportCalendar:
    return get.report_calendar(db, user)