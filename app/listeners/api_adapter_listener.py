import db.models as model
from app import event, report_processor
from services.api_adapter import ApiAdapter, User
from sqlalchemy.orm import Session
from entities import error

api = ApiAdapter()

"""
BUG: session is referred to as 'session' in one method and 'db' in the other
"""

def trigger_reprocessing_of_errors(table: model.Base, *args, **kwargs):
    error_type = None
    if table == model.CustomerBranch or table == model.IDStringMatch:
        error_type = error.ErrorType(4)
    
    if error_type:
        session = kwargs.get("session")
        if not session:
            session = kwargs.get("db") # hot fix
        user = kwargs.get("user")
        errors = api.get_errors(session, user)
        report_processor.ErrorReintegrationStrategy(
            session=session,
            target_err=error_type,
            error_table=errors,
            user=user).process_and_commit()
    return

def setup_api_event_handlers():
    event.subscribe("New Record", trigger_reprocessing_of_errors)
    event.subscribe("Record Updated", trigger_reprocessing_of_errors)
