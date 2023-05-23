"""Contains all post/insert methods for use by the higher level methods to
add records to a database"""

from services.utils import *
from jsonapi.jsonapi import jsonapi_error_handling, JSONAPIResponse
from sqlalchemy.orm import Session
from app import event

@jsonapi_error_handling
def __create_X(db: Session, json_data: dict, user: User, model: models.Base) -> JSONAPIResponse:
    model_name = hyphenated_name(model)
    hyphenate_json_obj_keys(json_data)
    result = models.serializer.post_collection(db,json_data,model_name,user.id(db=db)).data
    event.post_event(
        "New Record",
        model,
        db=db,
        user=user,
        id_=result["data"]["id"]
    )
    return result

@jsonapi_error_handling
def create_customer(db: Session, json_data: dict, user: User) -> JSONAPIResponse:
    new_name: str = json_data["data"]["attributes"]["name"]
    json_data["data"]["attributes"]["name"] = new_name.upper().strip()
    return __create_X(db, json_data, user, CUSTOMERS)


@jsonapi_error_handling
def create_branch(db: Session, json_data: dict, user: User) -> JSONAPIResponse:
    return __create_X(db, json_data, user, BRANCHES)


@jsonapi_error_handling
def create_mapping(db: Session, json_data: dict, user: User) -> JSONAPIResponse:
    return __create_X(db, json_data, user, ID_STRINGS)


@jsonapi_error_handling
def create_manufacturer(db: Session, json_data: dict, user: User) -> JSONAPIResponse:
    return __create_X(db, json_data, user, MANUFACTURERS)

@jsonapi_error_handling
def create_representative(db: Session, json_data: dict, user: User) -> JSONAPIResponse:
    return __create_X(db, json_data, user, REPS)