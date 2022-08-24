from app import event, error_reintegration
from entities import error
import db.models as model
from db.db_services import DatabaseServices
from services.api_adapter import ApiAdapter

api = ApiAdapter()

def new_entity_default_name_mapping(table: model.Base, *args, **kwargs):
    """sets a default mapping for the new customer, set equal to the name"""
    if table == model.Customer:
        data = {"customer_id": kwargs["id"], "recorded_name": kwargs["name"]}
        api.set_customer_name_mapping(**data)
    elif table == model.City:
        data = {"city_id": kwargs["id"], "recorded_name": kwargs["name"]}
        api.set_city_name_mapping(**data)
    elif table == model.State:
        data = {"state_id": kwargs["id"], "recorded_name": kwargs["name"]}
        api.set_state_name_mapping(**data)

def trigger_reprocessing_of_errors(table: model.Base, *args, **kwargs):
    error_type = None
    if table == model.MapRepToCustomer:
        error_type = error.ErrorType(5)
    elif table == model.MapCustomerName:
        error_type = error.ErrorType(1)
    elif table == model.MapCityName:
        error_type = error.ErrorType(2)
    elif table == model.MapStateName:
        error_type = error.ErrorType(3)
    elif table == model.CustomerBranch:
        error_type = error.ErrorType(4)
    
    if error_type:
        errors = api.get_errors()
        db = DatabaseServices()
        reintegrator = error_reintegration.Reintegrator(error_type, errors, db)
        reintegrator.process_and_commmit()
    return

def setup_api_event_handlers():
    event.subscribe("New Record", new_entity_default_name_mapping)
    event.subscribe("New Record", trigger_reprocessing_of_errors)
    event.subscribe("Record Updated", trigger_reprocessing_of_errors)
