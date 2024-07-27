"""Contains all get/select methods for use by the higher level methods to
pull data from a database"""

import calendar
from datetime import datetime
import pandas as pd
from services.utils import *
from jsonapi.jsonapi import jsonapi_error_handling, JSONAPIResponse
import sqlalchemy
from sqlalchemy.orm import Session


CHUNK_SIZE = 10000


def __get_X(
    db: Session, query: dict, user: User, model: models.Base, _id: int = 0
) -> JSONAPIResponse:
    if not _id:
        user_id: int = user.id(db=db)
        return models.serializer.get_collection(db, query, model, user_id)
    else:
        if not matched_user(user, model, _id, db):
            raise UserMisMatch()
        return models.serializer.get_resource(
            db, query, hyphenated_name(model), _id, obj_only=True
        )


@jsonapi_error_handling
def customers(
    db: Session, query: dict, user: User, cust_id: int = 0
) -> JSONAPIResponse:
    return __get_X(db, query, user, CUSTOMERS, cust_id)


@jsonapi_error_handling
def reports(
    db: Session, query: dict, user: User, report_id: int = 0
) -> JSONAPIResponse:
    return __get_X(db, query, user, REPORTS, report_id)


@jsonapi_error_handling
def reps(db: Session, query: dict, user: User, rep_id: int = 0) -> JSONAPIResponse:
    return __get_X(db, query, user, REPS, rep_id)


@jsonapi_error_handling
def submissions(
    db: Session, query: dict, user: User, submission_id: int = 0
) -> JSONAPIResponse:
    return __get_X(db, query, user, SUBMISSIONS_TABLE, submission_id)


@jsonapi_error_handling
def mappings(db: Session, query: dict, user: User, _id: int = 0) -> JSONAPIResponse:
    return __get_X(db, query, user, ID_STRINGS, _id)


@jsonapi_error_handling
def manufacturers(
    db: Session, query: dict, user: User, manuf_id: int = 0
) -> JSONAPIResponse:
    return __get_X(db, query, user, MANUFACTURERS, manuf_id)


@jsonapi_error_handling
def commission_data(
    db: Session, query: dict, user: User, row_id: int = 0
) -> JSONAPIResponse:
    return __get_X(db, query, user, COMMISSION_DATA_TABLE, row_id)


@jsonapi_error_handling
def branch(db: Session, query: dict, user: User, branch_id: int = 0) -> JSONAPIResponse:
    return __get_X(db, query, user, BRANCHES, branch_id)


@jsonapi_error_handling
def location(
    db: Session, query: dict, user: User, location_id: int = 0
) -> JSONAPIResponse:
    return __get_X(db, query, user, LOCATIONS, location_id)


def all_submissions(db: Session, user: User) -> pd.DataFrame:
    subs = SUBMISSIONS_TABLE
    reports = REPORTS
    manufs = MANUFACTURERS
    sql = (
        sqlalchemy.select(
            subs.id,
            subs.submission_date,
            subs.reporting_month,
            subs.reporting_year,
            reports.id.label("report_id"),
            reports.report_name,
            reports.yearly_frequency,
            reports.pos_report,
            manufs.name,
        )
        .select_from(subs)
        .join(reports)
        .join(manufs)
        .where(subs.user_id == user.id(db=db))
    )
    return pd.read_sql(sql, con=db.get_bind())


def commission_rate(db: Session, manufacturer_id: int, user_id: int) -> float | None:
    sql = sqlalchemy.select(USER_COMMISSIONS.commission_rate).where(
        sqlalchemy.and_(
            USER_COMMISSIONS.manufacturer_id == manufacturer_id,
            USER_COMMISSIONS.user_id == user_id,
        )
    )
    result = db.execute(sql).scalar()
    return result


def split(db: Session, report_id: int, user_id: int) -> float:
    sql = sqlalchemy.select(COMMISSION_SPLITS.split_proportion).where(
        sqlalchemy.and_(
            COMMISSION_SPLITS.report_id == report_id,
            COMMISSION_SPLITS.user_id == user_id,
        )
    )
    result = db.execute(sql).scalar()
    return result


@jsonapi_error_handling
def related(
    db: Session, primary: str, id_: int, secondary: str, user: User
) -> JSONAPIResponse:
    model = models_dict[primary]
    if not matched_user(user, model, id_, db):
        raise UserMisMatch()
    return models.serializer.get_related(db, {}, primary, id_, secondary)


@jsonapi_error_handling
def relationship(
    db: Session, primary: str, id_: int, secondary: str, user: User
) -> JSONAPIResponse:
    model = models_dict[primary]
    if not matched_user(user, model, id_, db):
        raise UserMisMatch()
    return models.serializer.get_relationship(db, {}, primary, id_, secondary)


def all_by_user_id(db: Session, table: models.Base, user_id: int) -> pd.DataFrame:
    sql = sqlalchemy.select(table).where(table.user_id == user_id)
    return pd.read_sql(sql, con=db.get_bind())


def report_name_by_id(db: Session, report_id: int) -> str:
    sql = sqlalchemy.select(REPORTS.report_name).where(REPORTS.id == report_id)
    result = db.execute(sql).one_or_none()
    if result:
        return result[0]


def all_manufacturers(db: Session) -> dict:
    sql = sqlalchemy.select(MANUFACTURERS.id, MANUFACTURERS.name).where(
        MANUFACTURERS.deleted == None
    )
    query_result = db.execute(sql).fetchall()
    return {
        id_: name_.lower().replace(" ", "_").replace("-", "_")
        for id_, name_ in query_result
    }


def branches(db: Session, user_id: int) -> pd.DataFrame:
    return all_by_user_id(db, BRANCHES, user_id)


def id_string_matches(db: Session, user_id: int) -> pd.DataFrame:
    return all_by_user_id(db, ID_STRINGS, user_id).loc[
        :, ["match_string", "report_id", "customer_branch_id", "id"]
    ]


def entities_w_alias(db: Session, user_id: int) -> pd.DataFrame:
    sql = """select * from branches_w_std_aliases where user_id = :user_id;"""
    result = db.execute(sql, params={"user_id": user_id}).fetchall()
    return pd.DataFrame(result, columns=["branch_id", "entity_alias", "user_id"]).drop(
        columns="user_id"
    )


def default_unknown_customer(db: Session, user_id: int) -> int:
    sql = """
        SELECT id
        FROM customer_branches
        WHERE EXISTS (
            SELECT 1 
            FROM customers
            WHERE customers.id = customer_id 
            AND customers.name = 'UNMAPPED') 
        AND customer_branches.user_id = :user_id"""
    return db.scalar(sql, params=dict(user_id=user_id))


def territory(db: Session, user_id: int, manf_id: int) -> list | None:
    sql = sqlalchemy.select(TERRITORIES.territory).where(
        sqlalchemy.and_(
            TERRITORIES.manufacturer_id == manf_id, TERRITORIES.user_id == user_id
        )
    )
    return db.execute(sql).scalar_one_or_none()


def manuf_name_by_id(db: Session, user_id: int, manf_id: int) -> str:
    sql = sqlalchemy.select(MANUFACTURERS.name).where(
        sqlalchemy.and_(MANUFACTURERS.id == manf_id, MANUFACTURERS.user_id == user_id)
    )
    return db.execute(sql).scalar()


def customer_location_proportions_by_state(
    db: Session, user_id: int, customer_id: int, territory: list[str]
) -> pd.DataFrame:
    sql = """SELECT customer, state, count(city) AS num_branches
            FROM branch_lookup
            WHERE user_id = :user_id and customer_id = :customer_id
            GROUP BY customer, state;"""  # using a view instead of a sqlalchemy model
    result = db.execute(
        sql, params={"user_id": user_id, "customer_id": customer_id}
    ).fetchall()
    table = pd.DataFrame(result, columns=["customer", "state", "num_branches"])
    in_territory = table.loc[table["state"].isin(territory)]
    total_branches = in_territory["num_branches"].sum()
    in_territory["total_share"] = in_territory["num_branches"] / total_branches
    return in_territory


def customer_id_and_name_from_report(
    db: Session, user_id: int, report_id: int
) -> tuple[int, str]:
    sql = (
        sqlalchemy.select(CUSTOMERS.id, CUSTOMERS.name)
        .join(REPORTS)
        .where(sqlalchemy.and_(REPORTS.id == report_id, REPORTS.user_id == user_id))
    )
    result = db.execute(sql).one_or_none()
    return result


def string_match_supplement(db: Session, user_id: int) -> pd.DataFrame:
    branches_expanded_sql = (
        sqlalchemy.select(CUSTOMERS.name, LOCATIONS.city, LOCATIONS.state, BRANCHES.id)
        .select_from(BRANCHES)
        .join(CUSTOMERS)
        .join(LOCATIONS)
        .where(BRANCHES.user_id == user_id)
    )
    result = pd.read_sql(branches_expanded_sql, con=db.get_bind())
    # create the match_string from customer name, city, and state
    result.loc[:, "match_string"] = result[["name", "city", "state"]].apply(
        lambda row: "_".join(row.values.astype(str)), axis=1
    )
    result = result.rename(columns={"id": "customer_branch_id"})
    return result.loc[:, ["match_string", "customer_branch_id"]]


def report_id_by_submission(db: Session, user_id: int, sub_ids: list):
    all_subs = all_by_user_id(db, SUBMISSIONS_TABLE, user_id)
    target_subs = all_subs.loc[all_subs["id"].isin(sub_ids), ["id", "report_id"]]
    target_subs.columns = ["submission_id", "report_id"]
    return target_subs


def download_file_lookup(db: Session, hash: str):
    sql = sqlalchemy.select(DOWNLOADS).where(DOWNLOADS.hash == hash)
    with db as session:
        return session.execute(sql).one_or_none()


def convert_cents_to_dollars(cent_amt: float) -> float:
    return cent_amt / 100


def convert_month_from_number_to_name(month_num: int) -> str:
    return calendar.month_name[month_num]


def commission_data_with_all_names(db: Session, submission_id: int = 0, **kwargs):
    """runs sql query to produce the commission table format used by SCA
    and converts month number to name and cents to dollars before return

    Returns: pd.DataFrame"""

    sql = (
        sqlalchemy.select(
            COMMISSION_DATA_TABLE.id,
            COMMISSION_DATA_TABLE.submission_id,
            REPORTS.report_label,
            SUBMISSIONS_TABLE.reporting_year,
            SUBMISSIONS_TABLE.reporting_month,
            MANUFACTURERS.name,
            REPS.initials,
            CUSTOMERS.name,
            LOCATIONS.city,
            LOCATIONS.state,
            COMMISSION_DATA_TABLE.inv_amt,
            COMMISSION_DATA_TABLE.comm_amt,
        )
        .select_from(COMMISSION_DATA_TABLE)
        .join(
            SUBMISSIONS_TABLE,
            COMMISSION_DATA_TABLE.submission_id == SUBMISSIONS_TABLE.id,
        )
        .join(REPORTS, REPORTS.id == SUBMISSIONS_TABLE.report_id)
        .join(MANUFACTURERS, REPORTS.manufacturer_id == MANUFACTURERS.id)
        .join(BRANCHES, COMMISSION_DATA_TABLE.customer_branch_id == BRANCHES.id)
        .join(REPS)
        .join(CUSTOMERS, CUSTOMERS.id == BRANCHES.customer_id)
        .join(LOCATIONS)
        .where(COMMISSION_DATA_TABLE.user_id == kwargs.get("user_id"))
        .order_by(
            SUBMISSIONS_TABLE.reporting_year.desc(),
            SUBMISSIONS_TABLE.reporting_month.desc(),
            CUSTOMERS.name.asc(),
            LOCATIONS.city.asc(),
            LOCATIONS.state.asc(),
        )
    )

    if submission_id:
        sql = sql.where(COMMISSION_DATA_TABLE.submission_id == submission_id)

    if start_date := kwargs.get("startDate"):
        try:
            start_date = datetime.fromisoformat(start_date)
        except ValueError:
            start_date = datetime.fromisoformat(start_date.replace("Z", ""))
        except Exception as e:
            print(e)
        if isinstance(start_date, datetime):
            sql = sql.where(
                sqlalchemy.or_(
                    sqlalchemy.and_(
                        SUBMISSIONS_TABLE.reporting_year == start_date.year,
                        SUBMISSIONS_TABLE.reporting_month >= start_date.month,
                    ),
                    SUBMISSIONS_TABLE.reporting_year > start_date.year,
                )
            )
    if end_date := kwargs.get("endDate"):
        try:
            end_date = datetime.fromisoformat(end_date)
        except ValueError:
            end_date = datetime.fromisoformat(end_date.replace("Z", ""))
        except Exception as e:
            print(e)
        if isinstance(end_date, datetime):
            sql = sql.where(
                sqlalchemy.or_(
                    sqlalchemy.and_(
                        SUBMISSIONS_TABLE.reporting_year == end_date.year,
                        SUBMISSIONS_TABLE.reporting_month <= end_date.month,
                    ),
                    SUBMISSIONS_TABLE.reporting_year < end_date.year,
                )
            )
    if manufacturer := kwargs.get("manufacturer_id"):
        sql = sql.where(MANUFACTURERS.id == manufacturer)
    if customer := kwargs.get("customer_id"):
        sql = sql.where(CUSTOMERS.id == customer)
    if city := kwargs.get("city_id"):
        sql = sql.where(LOCATIONS.city == city)
    if state := kwargs.get("state_id"):
        sql = sql.where(LOCATIONS.state == state)
    if representative := kwargs.get("representative_id"):
        sql = sql.where(REPS.id == representative)

    for chunk in pd.read_sql(
        sql,
        con=db.get_bind().execution_options(stream_results=True),
        chunksize=CHUNK_SIZE,
    ):

        chunk.columns = [
            "ID",
            "Submission",
            "Report",
            "Year",
            "Month",
            "Manufacturer",
            "Salesman",
            "Customer Name",
            "City",
            "State",
            "Inv Amt",
            "Comm Amt",
        ]
        chunk.loc[:, "Inv Amt"] = chunk.loc[:, "Inv Amt"].apply(
            convert_cents_to_dollars
        )
        chunk.loc[:, "Comm Amt"] = chunk.loc[:, "Comm Amt"].apply(
            convert_cents_to_dollars
        )
        chunk.loc[:, "Month"] = (
            chunk.loc[:, "Month"].apply(convert_month_from_number_to_name).astype(str)
        )
        yield chunk


def submission_exists(db: Session, submission_id: int) -> bool:
    sql = sqlalchemy.select(SUBMISSIONS_TABLE).where(
        SUBMISSIONS_TABLE.id == submission_id
    )
    result = db.execute(sql).fetchone()
    return True if result else False
