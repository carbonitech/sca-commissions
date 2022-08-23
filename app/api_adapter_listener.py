from app import event
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
    """
    Conditions under which this should trigger:
        1. any name mapping created for an existing customer, city, or state
    """
    if table == model.MapRepToCustomer:
        print("default mapping creation has triggered this event!")
        print(args," ",kwargs)


def setup_api_event_handlers():
    event.subscribe("New Record", new_entity_default_name_mapping)
    event.subscribe("New Record", trigger_reprocessing_of_errors)
    # event.subscribe("New Record")
    # event.subscribe("New Record")
    # event.subscribe("Record Updated")
    # event.subscribe("Record Updated")
    # event.subscribe("Record Updated")
    # event.subscribe("Record Updated")
