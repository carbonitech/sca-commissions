import os
import time
import requests
from dotenv import load_dotenv

import sqlalchemy
from sqlalchemy.orm import Session, sessionmaker
from fastapi import Request, HTTPException
from jose.jwt import get_unverified_claims

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
ERRORS_TABLE = models.Error
DOWNLOADS = models.FileDownloads
FORM_FIELDS = models.ReportFormFields
USERS = models.User
USER_COMMISSIONS = models.UserCommissionRate
COMMISSION_SPLITS = models.CommissionSplit
ID_STRINGS = models.IDStringMatch
LOCATIONS = models.Location


ENGINE = sqlalchemy.create_engine(os.getenv("DATABASE_URL").replace("postgres://","postgresql://"))
SESSIONLOCAL = sessionmaker(autocommit=False, autoflush=False, bind=ENGINE)

def hyphenate_name(table_name: str) -> str:
    return table_name.replace("_","-")

def hyphenated_name(table_obj) -> str:
    return table_obj.__tablename__.replace("_","-")

all_models = [
    CUSTOMERS,
    BRANCHES,
    REPS,
    MANUFACTURERS,
    REPORTS,
    COMMISSION_DATA_TABLE,
    SUBMISSIONS_TABLE,
    ERRORS_TABLE,
    DOWNLOADS,
    FORM_FIELDS,
    USERS,
    USER_COMMISSIONS,
    COMMISSION_SPLITS,
    ID_STRINGS,
    LOCATIONS
]

# this table allows for a lookup of the model by it's JSONAPI resource name
models_dict = {hyphenated_name(model): model for model in all_models} 

class UserMisMatch(Exception):
    pass

def get_user(request: Request) -> User:

    access_token: str = request.headers.get("Authorization")
    access_token_bare = access_token.replace("Bearer ","")
    if user_details := preverified(access_token_bare):
        return User(**user_details)

    url = os.getenv("AUTH0_DOMAIN") + "/userinfo"
    auth0_user_body: dict = requests.get(url, headers={"Authorization":access_token}).json()
    match auth0_user_body:
        case {"nickname": a, "name": b, "email": c, "email_verified": d, **other}:
            cache_token(access_token_bare, nickname=a, name=b, email=c, verified=d)
            return User(nickname=a, name=b, email=c, verified=d)
        case _:
            raise HTTPException(status_code=400, detail="user could not be verified")

def preverified(access_token: str) -> dict | None:
    session = SESSIONLOCAL()
    parameters = {"access_token": access_token, "current_time": int(time.time())}
    # without DISTINCT, may run into multiple of the same token cached
    sql = "SELECT DISTINCT nickname, name, email, verified FROM user_tokens WHERE access_token = :access_token and expires_at > :current_time"
    result = session.execute(sql, parameters).one_or_none()
    session.close()
    return result


def cache_token(access_token: str, nickname: str, name: str, email: str, verified: bool) -> None:
    session = SESSIONLOCAL()
    if session.execute("select id from user_tokens where access_token = :access_token", {"access_token": access_token}).fetchone():
        session.close()
        return
    sql = "INSERT INTO user_tokens (access_token, nickname, name, email, verified, expires_at)"\
        "VALUES (:access_token, :nickname, :name, :email, :verified, :expires_at)"
    parameters = {
        "access_token": access_token,
        "nickname": nickname,
        "name": name,
        "email": email,
        "verified": verified,
        "expires_at": int(get_unverified_claims(access_token)["exp"]) 
    }
    session.execute(sql, parameters)
    session.commit()
    session.close()


def hyphenate_json_obj_keys(json_data: dict) -> dict:
    for hi_level in json_data["data"].keys():
        if not isinstance(json_data["data"][hi_level], dict):
            continue
        json_data["data"][hi_level] = {hyphenate_name(k):v for k,v in json_data["data"][hi_level].items()}
    return json_data

async def get_db():
    db = SESSIONLOCAL()
    try:
        yield db
    finally:
        db.close()
    
def matched_user(user: User, model, reference_id: int, db: Session) -> bool:
    try:
        return user.id(db) == db.query(model.user_id).filter(model.id == reference_id).scalar()
    except AttributeError: # unreliable as a catch for no user_id. # NOTE consider inspecting the error before returning True
        return True  # if the model doesn't have user_id in it, return a truthy answer anyway
    except Exception:
        return False
