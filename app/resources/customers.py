from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session
from services import get, post, patch, delete
from jsonapi.jsonapi import Query, convert_to_jsonapi, JSONAPIRoute
from jsonapi.request_models import RequestModels
from services.utils import User, get_db, get_user

router = APIRouter(prefix="/customers", route_class=JSONAPIRoute)

@router.get("", tags=["customers"])
async def all_customers(
        query: Query=Depends(),
        db: Session=Depends(get_db),
        user: User=Depends(get_user),
        postgres_fuzzy_match: bool=False
    ):
    if postgres_fuzzy_match and user.name == 'admin':
        """This option changes the behavior of this route's return
        such that it is no longer JSON:API and filter query parameter is 
        used in a very specific way - as a term for running a trigram
        comparison in conjunction with using Levenshein distance"""
        customer_search_q = """
            SELECT id, name
            FROM customers
            WHERE name % CAST(:search_term AS text)
            AND LENGTH(name) >= LENGTH(:search_term)
            AND levenshtein(CAST(name AS text), CAST(:search_term AS text)) <= 
                CASE 
                    WHEN LENGTH(:search_term) < 5 THEN 15 
                    WHEN LENGTH(:search_term) BETWEEN 5 AND 10 THEN 10
                    ELSE 5
                END
            AND user_id = :user_id
            ORDER BY levenshtein(CAST(name AS text), CAST(:search_term AS text))
            LIMIT 5;
        """
        if filt := query.filter:
            term = filt
        else:
            term = ''
        params = dict(search_term=term, user_id=user.id(db=db))
        return db.execute(text(customer_search_q), params=params).fetchall()

    jsonapi_query = convert_to_jsonapi(query)
    return get.customers(db,jsonapi_query, user)

@router.get("/{customer_id}", tags=["customers"])
async def customer_by_id(
        customer_id: int,
        query: Query=Depends(),
        db: Session=Depends(get_db),
        user: User=Depends(get_user)
    ):
    jsonapi_query = convert_to_jsonapi(query)
    return get.customers(db, jsonapi_query, user, customer_id)
   
@router.patch("/{customer_id}", tags=["customers"])
async def modify_customer(
        customer_id: int,
        customer: RequestModels.customer_modification,
        db: Session=Depends(get_db),
        user: User=Depends(get_user)
    ):
    return patch.customer(db, customer_id, customer.dict(), user)

@router.post("", tags=["customers"])
async def new_customer(
        jsonapi_obj: RequestModels.new_customer,
        db: Session=Depends(get_db),
        user: User=Depends(get_user)
    ):
    return post.customer(db=db, json_data=jsonapi_obj.dict(), user=user)

@router.delete("/{customer_id}", tags=["customers"])
async def delete_customer(
        customer_id: int,
        db: Session=Depends(get_db),
        user: User=Depends(get_user)
    ):
    # soft delete
    return delete.customer(db=db,customer_id=customer_id)
