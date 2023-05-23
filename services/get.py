"""Contains all get/select methods for use by the higher level methods to
pull data from a database"""

import json
import calendar
from datetime import datetime
import pandas as pd
from services.utils import *
from jsonapi.jsonapi import jsonapi_error_handling, JSONAPIResponse
import sqlalchemy
from sqlalchemy.orm import Session

def __get_X(db: Session, query: dict, user: User, model: models.Base, _id: int=0) -> JSONAPIResponse:
    if not _id:
        user_id: int = user.id(db=db)
        return models.serializer.get_collection(db,query,model,user_id)
    else:
        if not matched_user(user, model, _id, db):
            raise UserMisMatch()
        return models.serializer.get_resource(db, query, hyphenated_name(model), _id, obj_only=True)
    

@jsonapi_error_handling
def customers(db: Session, query: dict, user: User, cust_id: int=0) -> JSONAPIResponse:
    return __get_X(db, query, user, CUSTOMERS, cust_id)

@jsonapi_error_handling
def reports(db: Session, query: dict, user: User, report_id: int=0) -> JSONAPIResponse:
    return __get_X(db, query, user, REPORTS, report_id)

@jsonapi_error_handling
def reps(db: Session, query: dict, user: User, rep_id: int=0) -> JSONAPIResponse:
    return __get_X(db, query, user, REPS, rep_id)

@jsonapi_error_handling
def submissions(db: Session, query: dict, user: User, submission_id: int=0) -> JSONAPIResponse:
    return __get_X(db, query, user, SUBMISSIONS_TABLE, submission_id)

@jsonapi_error_handling
def mappings(db: Session, query: dict, user: User, _id: int=0) -> JSONAPIResponse:
    return __get_X(db, query, user, ID_STRINGS, _id)

@jsonapi_error_handling
def manufacturers(db: Session, query: dict, user: User, manuf_id: int=0) -> JSONAPIResponse:
    return __get_X(db, query, user, MANUFACTURERS, manuf_id)

@jsonapi_error_handling
def commission_data(db: Session, query: dict, user: User, row_id: int=0) -> JSONAPIResponse:
    return __get_X(db, query, user, COMMISSION_DATA_TABLE, row_id)

@jsonapi_error_handling
def branch(db: Session, query: dict, user: User, branch_id: int=0) -> JSONAPIResponse:
    return __get_X(db, query, user, BRANCHES, branch_id)

@jsonapi_error_handling
def location(db: Session, query: dict, user: User, location_id: int=0) -> JSONAPIResponse:
    return __get_X(db, query, user, LOCATIONS, location_id)

def errors(db: Session, user: User, submission_id: int=0) -> pd.DataFrame:
    """get all report processing errors for all submissions by user, or a specific submission"""
    sql = sqlalchemy.select(ERRORS_TABLE).where(ERRORS_TABLE.user_id == user.id(db=db))
    if submission_id:
        sql = sql.where(ERRORS_TABLE.submission_id == submission_id)
    result = pd.read_sql(sql, con=db.get_bind())
    if result.empty:
        return result
    result.loc[:,'row_data'] = result.loc[:,'row_data'].apply(lambda json_str: json.loads(json_str))
    return result

def all_submissions(db: Session, user: User) -> pd.DataFrame:
    subs = SUBMISSIONS_TABLE
    reports = REPORTS
    manufs = MANUFACTURERS
    sql = sqlalchemy.select(subs.id,subs.submission_date,subs.reporting_month,subs.reporting_year,
            reports.id.label("report_id"),reports.report_name,reports.yearly_frequency, reports.pos_report,
            manufs.name).select_from(subs).join(reports).join(manufs).where(subs.user_id == user.id(db=db))
    return pd.read_sql(sql, con=db.get_bind())


def commission_rate(db: Session, manufacturer_id: int, user_id: int) -> float|None:
    sql = sqlalchemy.select(USER_COMMISSIONS.commission_rate)\
        .where(
            sqlalchemy.and_(
                USER_COMMISSIONS.manufacturer_id == manufacturer_id,
                USER_COMMISSIONS.user_id == user_id
        ))
    result = db.execute(sql).scalar()
    return result

def split(db: Session, report_id: int, user_id: int) -> float:
    sql = sqlalchemy.select(COMMISSION_SPLITS.split_proportion)\
        .where(
            sqlalchemy.and_(
                COMMISSION_SPLITS.report_id == report_id,
                COMMISSION_SPLITS.user_id == user_id
        ))
    result = db.execute(sql).scalar()
    return result

@jsonapi_error_handling
def related(db: Session, primary: str, id_: int, secondary: str, user: User) -> JSONAPIResponse:
    model = models_dict[primary]
    if not matched_user(user, model, id_, db):
        raise UserMisMatch()
    return models.serializer.get_related(db,{},primary,id_,secondary)

@jsonapi_error_handling
def relationship(db: Session, primary: str, id_: int, secondary: str, user: User) -> JSONAPIResponse:
    model = models_dict[primary]
    if not matched_user(user, model, id_, db):
        raise UserMisMatch()
    return models.serializer.get_relationship(db,{},primary,id_,secondary)

def all_by_user_id(db: Session, table: models.Base, user_id: int) -> pd.DataFrame:
    sql = sqlalchemy.select(table).where(table.user_id == user_id)
    return pd.read_sql(sql,con=db.get_bind())      

def report_name_by_id(db: Session, report_id: int) -> str:
    sql = sqlalchemy.select(REPORTS.report_name).where(REPORTS.id == report_id)
    result = db.execute(sql).one_or_none()
    if result:
        return result[0]

def all_manufacturers(db: Session) -> dict:
    sql = sqlalchemy.select(MANUFACTURERS.id,MANUFACTURERS.name).where(MANUFACTURERS.deleted == None)
    query_result = db.execute(sql).fetchall()
    return {id_: name_.lower().replace(" ","_").replace("-","_") for id_, name_ in query_result}

def branches(db: Session, user_id: int) -> pd.DataFrame:
    return all_by_user_id(db, BRANCHES, user_id)

def id_string_matches(db: Session, user_id: int) -> pd.DataFrame:
    return all_by_user_id(db, ID_STRINGS, user_id).loc[:,["match_string","report_id","customer_branch_id","id"]]

def string_match_supplement(db: Session, user_id: int) -> pd.DataFrame:
    branches_expanded_sql = sqlalchemy.select(CUSTOMERS.name, LOCATIONS.city, LOCATIONS.state, BRANCHES.id)\
        .select_from(BRANCHES).join(CUSTOMERS).join(LOCATIONS).where(BRANCHES.user_id == user_id)
    result = pd.read_sql(branches_expanded_sql, con=db.get_bind())
    # create the match_string from customer name, city, and state
    result.loc[:,"match_string"] = result[["name", "city", "state"]].apply(
        lambda row: '_'.join(row.values.astype(str)), axis=1
    )
    result = result.rename(columns={"id": "customer_branch_id"})
    return result.loc[:,["match_string", "customer_branch_id"]]

def report_id_by_submission(db: Session, user_id: int, sub_ids: list):
    all_subs = all_by_user_id(db, SUBMISSIONS_TABLE, user_id)
    target_subs = all_subs.loc[all_subs["id"].isin(sub_ids),["id", "report_id"]]
    target_subs.columns = ["submission_id", "report_id"]
    return target_subs

def download_file_lookup(db: Session, hash: str):
    sql = sqlalchemy.select(DOWNLOADS).where(DOWNLOADS.hash == hash)
    with db as session:
        return session.execute(sql).one_or_none()

def convert_cents_to_dollars(cent_amt: float) -> float:
    return cent_amt/100

def convert_month_from_number_to_name(month_num: int) -> str:
    return calendar.month_name[month_num]

def commission_data_with_all_names(db: Session, submission_id: int=0, **kwargs) -> pd.DataFrame:
    """runs sql query to produce the commission table format used by SCA
    and converts month number to name and cents to dollars before return
    
    Returns: pd.DataFrame"""
    commission_data_raw = COMMISSION_DATA_TABLE
    submission_data = SUBMISSIONS_TABLE
    reports = REPORTS
    manufacturers = MANUFACTURERS
    reps = REPS
    branches = BRANCHES
    customers = CUSTOMERS
    locations = LOCATIONS
    sql = (sqlalchemy
            .select(commission_data_raw.id, commission_data_raw.submission_id,
                submission_data.reporting_year, submission_data.reporting_month,
                manufacturers.name, reps.initials, customers.name,
                locations.city, locations.state, commission_data_raw.inv_amt,
                commission_data_raw.comm_amt
            )
            .select_from(commission_data_raw)
            .join(submission_data, commission_data_raw.submission_id == submission_data.id)
            .join(reports)
            .join(manufacturers, reports.manufacturer_id == manufacturers.id)
            .join(branches, commission_data_raw.customer_branch_id == branches.id)
            .join(reps)
            .join(customers)
            .join(locations)
            .where(commission_data_raw.user_id == kwargs.get("user_id"))
            .order_by(
                submission_data.reporting_year.desc(),
                submission_data.reporting_month.desc(),
                customers.name.asc(),
                locations.city.asc(),
                locations.state.asc()
            )
        )
    
    if submission_id:
        sql = sql.where(commission_data_raw.submission_id == submission_id)

    if (start_date := kwargs.get("startDate")):
        try:
            start_date = datetime.fromisoformat(start_date)
        except ValueError:
            start_date = datetime.fromisoformat(start_date.replace("Z",""))
        except Exception as e:
            print(e)
        if isinstance(start_date, datetime):
            sql = sql.where(sqlalchemy.or_(
                sqlalchemy.and_(
                    submission_data.reporting_year == start_date.year,
                    submission_data.reporting_month >= start_date.month),
                submission_data.reporting_year > start_date.year))
    if (end_date := kwargs.get("endDate")):
        try:
            end_date = datetime.fromisoformat(end_date)
        except ValueError:
            end_date = datetime.fromisoformat(end_date.replace("Z",""))
        except Exception as e:
            print(e)
        if isinstance(end_date, datetime):
            sql = sql.where(sqlalchemy.or_(
                sqlalchemy.and_(
                    submission_data.reporting_year == end_date.year,
                    submission_data.reporting_month <= end_date.month),
                submission_data.reporting_year < end_date.year))
    if(manufacturer := kwargs.get("manufacturer_id")):
        sql = sql.where(manufacturers.id == manufacturer)
    if(customer := kwargs.get("customer_id")):
        sql = sql.where(customers.id == customer)
    if(city := kwargs.get("city_id")):
        sql = sql.where(locations.city == city)
    if(state := kwargs.get("state_id")):
        sql = sql.where(locations.state == state)
    if(representative := kwargs.get("representative_id")):
        sql = sql.where(reps.id == representative)

    view_table = pd.read_sql(sql, con=db.get_bind())

    view_table.columns = ["ID","Submission","Year","Month","Manufacturer","Salesman",
            "Customer Name","City","State","Inv Amt","Comm Amt"]
    view_table.loc[:,"Inv Amt"] = view_table.loc[:,"Inv Amt"].apply(convert_cents_to_dollars)
    view_table.loc[:,"Comm Amt"] = view_table.loc[:,"Comm Amt"].apply(convert_cents_to_dollars)
    view_table.loc[:,"Month"] = view_table.loc[:,"Month"].apply(convert_month_from_number_to_name).astype(str)
    return view_table

def submission_exists(db: Session, submission_id: int) -> bool:
    sql = sqlalchemy.select(SUBMISSIONS_TABLE).where(SUBMISSIONS_TABLE.id == submission_id)
    result = db.execute(sql).fetchone()
    return True if result else False