import os
from dotenv import load_dotenv

import sqlalchemy
from sqlalchemy.orm import Session, sessionmaker
from fastapi import Request, HTTPException

from app.auth import LocalTokenStore
from db import models
from entities.user import User

load_dotenv()

CUSTOMERS = models.Customer
BRANCHES = models.CustomerBranch
REPS = models.Representative
MANUFACTURERS = models.Manufacturer
REPORTS = models.ManufacturersReport
COMMISSION_DATA_TABLE = models.CommissionData
SUBMISSIONS_TABLE = models.Submission
DOWNLOADS = models.FileDownloads
FORM_FIELDS = models.ReportFormFields
USERS = models.User
USER_COMMISSIONS = models.UserCommissionRate
COMMISSION_SPLITS = models.CommissionSplit
ID_STRINGS = models.IDStringMatch
LOCATIONS = models.Location
TERRITORIES = models.Territory
REPORT_COL_NAMES = models.ReportColumnName

PROD_DB = os.getenv("DATABASE_URL").replace("postgres://", "postgresql://")
TESTING_DB = os.getenv("TESTING_DATABASE_URL", "").replace(
    "postgres://", "postgresql://"
)
if TESTING_DB:
    ENGINE = sqlalchemy.create_engine(TESTING_DB)
else:
    ENGINE = sqlalchemy.create_engine(PROD_DB)

SESSIONLOCAL = sessionmaker(autocommit=False, autoflush=False, bind=ENGINE)


def hyphenate_name(table_name: str) -> str:
    return table_name.replace("_", "-")


def hyphenated_name(table_obj) -> str:
    return table_obj.__tablename__.replace("_", "-")


all_models = [
    CUSTOMERS,
    BRANCHES,
    REPS,
    MANUFACTURERS,
    REPORTS,
    COMMISSION_DATA_TABLE,
    SUBMISSIONS_TABLE,
    DOWNLOADS,
    FORM_FIELDS,
    USERS,
    USER_COMMISSIONS,
    COMMISSION_SPLITS,
    ID_STRINGS,
    LOCATIONS,
    TERRITORIES,
]

# this table allows for a lookup of the model by it's JSONAPI resource name
models_dict = {hyphenated_name(model): model for model in all_models}


class UserMisMatch(Exception): ...


async def get_user(request: Request) -> User:
    access_token: str = request.headers.get("Authorization").replace("Bearer ", "")
    if token := LocalTokenStore.get(access_token):
        if not token.user.user_id:
            # __anext__ allows the async generator to be awaited
            token.update_user(user_id=token.user.id(await get_db().__anext__()))
        return token.user
    raise HTTPException(400, detail="User not found")


def hyphenate_json_obj_keys(json_data: dict) -> dict:
    for hi_level in json_data["data"].keys():
        if not isinstance(json_data["data"][hi_level], dict):
            continue
        json_data["data"][hi_level] = {
            hyphenate_name(k): v for k, v in json_data["data"][hi_level].items()
        }
    return json_data


async def get_db():
    db = SESSIONLOCAL()
    try:
        yield db
    finally:
        db.close()


def matched_user(user: User, model, reference_id: int, db: Session) -> bool:
    try:
        return (
            user.id(db)
            == db.query(model.user_id).filter(model.id == reference_id).scalar()
        )
    except AttributeError:  # unreliable as a catch for no user_id.
        # NOTE consider inspecting the error before returning True
        # if the model doesn't have user_id in it, return a truthy answer anyway
        return True
    except Exception:
        return False
