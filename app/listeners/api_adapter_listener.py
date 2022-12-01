import db.models as model
from app import event, report_processor
from services.api_adapter import ApiAdapter, User
from entities import error

api = ApiAdapter()

"""
BUG: session is referred to as 'session' in one method and 'db' in the other
"""

def entity_default_name_mapping(table: model.Base, *args, **kwargs):
    """
    sets a default mapping for a new customer or a current customer with a modified name
    sets new mapping equal to the name
    TODO simplify the if-elif
    """
    if (new_name := kwargs.get("name")):
        db = kwargs.get("db")
        user: User = kwargs.get("user")
        user_id = user.id(db=db)
        new_name: str
        if table == model.Customer:
            data = {"customer_id": kwargs["id_"], "recorded_name": new_name.upper().strip(), "user_id": user_id}
            api.set_customer_name_mapping(db=db, user=user, **data)
        elif table == model.City:
            data = {"city_id": kwargs["id_"], "recorded_name": new_name.upper().strip(), "user_id": user_id}
            api.set_city_name_mapping(db=db, user=user, **data)
        elif table == model.State:
            data = {"state_id": kwargs["id_"], "recorded_name": new_name.upper().strip(), "user_id": user_id}
            api.set_state_name_mapping(db=db, user=user, **data)

def trigger_reprocessing_of_errors(table: model.Base, *args, **kwargs):
    error_type = None
    session = kwargs.get("session")
    if not session:
        session = kwargs.get("db") # hot fix
    user = kwargs.get("user")
    if table == model.MapCustomerName:
        error_type = error.ErrorType(1)
    elif table == model.MapCityName:
        error_type = error.ErrorType(2)
    elif table == model.MapStateName:
        error_type = error.ErrorType(3)
    elif table == model.CustomerBranch:
        error_type = error.ErrorType(4)
    
    if error_type:
        errors = api.get_errors(session, user)
        processor = report_processor.ReportProcessor(session=session,target_err=error_type, error_table=errors, user=user)
        processor.process_and_commit()
    return

def setup_api_event_handlers():
    event.subscribe("New Record", entity_default_name_mapping)
    event.subscribe("New Record", trigger_reprocessing_of_errors)
    event.subscribe("Record Updated", entity_default_name_mapping)
    event.subscribe("Record Updated", trigger_reprocessing_of_errors)
