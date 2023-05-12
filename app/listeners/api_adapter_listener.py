import db.models as model
from app import event, report_processor
from services.api_adapter import ApiAdapter, User
from sqlalchemy.orm import Session
from entities import error

api = ApiAdapter()

"""
BUG: session is referred to as 'session' in one method and 'db' in the other
"""

def update_new_customer_branch_with_loc_id(session: Session, branch_id: int) -> bool:
    stmt = """
        UPDATE customer_branches
        SET location_id = subq.id
        FROM (
            SELECT loc.id
            FROM customer_branches as cb
            JOIN cities as ci
            ON ci.id = cb.city_id
            JOIN states as st
            ON st.id = cb.state_id
            JOIN locations as loc on loc.city = ci.name and loc.state = st.name
            WHERE cb.id = :branch_id
            LIMIT 1) as subq
        WHERE customer_branches.id = :branch_id;
    """
    try:
        session.execute(stmt, params={"branch_id": branch_id})
        session.commit()
    except:
        return False
    else:
        return True

def trigger_reprocessing_of_errors(table: model.Base, *args, **kwargs):
    error_type = None
    session = kwargs.get("session")
    if not session:
        session = kwargs.get("db") # hot fix
    user = kwargs.get("user")
    if table == model.CustomerBranch:
        error_type = error.ErrorType(4)

    if table == model.IDStringMatch:
        error_type = error.ErrorType(4)
    
    if error_type:
        errors = api.get_errors(session, user)
        processor = report_processor.ReportProcessor(session=session,target_err=error_type, error_table=errors, user=user)
        processor.process_and_commit()
    return

def setup_api_event_handlers():
    event.subscribe("New Record", trigger_reprocessing_of_errors)
    event.subscribe("Record Updated", trigger_reprocessing_of_errors)
