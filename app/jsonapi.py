from datetime import datetime
import functools
import re
import json
import warnings
from typing import Any, Callable
from urllib.parse import unquote
from dataclasses import dataclass

from pydantic import BaseModel
from sqlalchemy_jsonapi import JSONAPI
from starlette.requests import QueryParams
from starlette.datastructures import QueryParams
from fastapi import Request, Response, HTTPException
from fastapi.routing import APIRoute
from sqlalchemy import or_, func
from sqlalchemy.orm import Session, Query as sqlQuery
from sqlalchemy_jsonapi.errors import NotSortableError, PermissionDeniedError,BaseError
from sqlalchemy_jsonapi.serializer import Permissions, JSONAPIResponse, check_permission


DEFAULT_SORT: str = "id"
MAX_PAGE_SIZE: int = 300
MAX_RECORDS: int = 15000

class Query(BaseModel):
    include: str|None = None
    sort: str|None = None
    fields: str|None = None
    filter: str|None = None
    page: str|None = None


### nested objects ###

class JSONAPIBaseModification(BaseModel):
    type: str
class JSONAPIBaseRelationship(BaseModel):
    type: str
    id: int
class JSONAPIRelationshipObject(BaseModel):
    data: JSONAPIBaseRelationship

## branch ##
class Branch(BaseModel):
    deleted: datetime|None = None
    store_number: int|None = None
class BranchRelationship(BaseModel):
    representative: JSONAPIRelationshipObject
class BranchRelatonshipFull(BaseModel):
    customers: JSONAPIRelationshipObject|None
    locations: JSONAPIRelationshipObject|None
    representative: JSONAPIRelationshipObject|None
class BranchModification(JSONAPIBaseModification):
    id: int
    attributes: Branch|dict|None = {}
    relationships: BranchRelationship|dict|None = {}
class NewBranch(JSONAPIBaseModification):
    attributes: Branch
    relationships:BranchRelatonshipFull

## mapping ##
class Mapping(BaseModel):
    report_id: int
    match_string: str
    created_at: datetime
class MappingRelationship(BaseModel):
    branches: JSONAPIRelationshipObject
class NewMapping(JSONAPIBaseModification):
    attributes: Mapping
    relationships:MappingRelationship

## customer ##
class Customer(BaseModel):
    name: str
class CustomerModification(JSONAPIBaseModification):
    id: int
    attributes: Customer
class NewCustomer(JSONAPIBaseModification):
    attributes: Customer
class CustomerModificationRequest(BaseModel):
    data: CustomerModification
class CustomerNameMapping(BaseModel):
    recorded_name: str
class CustomerRelationship(BaseModel):
    customers: JSONAPIRelationshipObject
class NewCustomerNameMapping(JSONAPIBaseModification):
    attributes: CustomerNameMapping
    relationships: CustomerRelationship


### top level objects ###
class NewCustomerRequest(BaseModel):
    data: NewCustomer

class NewCustomerNameMappingRequest(BaseModel):
    data: NewCustomerNameMapping
    
class BranchModificationRequest(BaseModel):
    data: BranchModification

class NewBranchRequest(BaseModel):
    data: NewBranch

class NewMappingRequest(BaseModel):
    data: NewMapping


@dataclass
class RequestModels:
    new_customer_name_mapping = NewCustomerNameMappingRequest
    branch_modification = BranchModificationRequest
    new_customer = NewCustomerRequest
    new_branch = NewBranchRequest
    new_mapping = NewMappingRequest


def convert_to_jsonapi(query: dict) -> dict:
    jsonapi_query = {}
    bracketed_params = ['fields','page']
    for param_name, param_value in query.dict(exclude_none=True).items():
        param_name: str
        param_value: str
        if param_name in bracketed_params:
            param_value_dict: dict = json.loads(param_value)
            jsonapi_query.update({f"{param_name}[{param}]":value for param,value in param_value_dict.items()})
        else:
            jsonapi_query[param_name] = param_value
    return jsonapi_query

def format_error(error: BaseError, tb: str) -> dict:
    """format the error for raises an HTTPException so FastAPI can handle it properly"""
    data: dict = error.data
    status_code: int = error.status_code #subclass of BaseError actually raised will have this attr
    data["errors"][0]["id"] = str(data["errors"][0]["id"])
    data["errors"][0]["traceback"] = tb
    return {"status_code": status_code, "detail": data}

class JSONAPI_(JSONAPI):
    """
    Custom fixes applied to the JSONAPI object

    _apply_filter static method created to handle filtering arguments.
        This library does not handle filtering.

    _add_pagination adds pagination metadata totalPages and currentPage
        as well as pagination links

    get_collection is a copy of JSONAPI's same method, but with new
        logic spliced in to handle filtering arguments, add pagination metadata and links,
        and apply a default sorting pattern if a sort argument is not applied.

    _filter_deleted filters for null values in a hard-coded "deleted" column
    """

    @staticmethod
    def hyphenate_name(table_name: str) -> str:
        return table_name.replace("_","-")

    @staticmethod
    def _coerce_dict(query_params: QueryParams|dict) -> dict:
        if isinstance(query_params, QueryParams):
            return query_params._dict
        else:
            return query_params

    @staticmethod
    def _apply_filter(model, sqla_query_obj: sqlQuery, query_params: dict):
        """
        handler for filter parameters in the query string
        for any value or list of values, this filter is permissive,
        looking for a substring anywhere in the field value that matches the arguement(s)
        Roughly quivalent to SELECT field FROM table WHERE field LIKE '%value_1% OR LIKE '%value_2%'
        """
        if (filter_args_str := query_params.get('filter')):
            filter_args: dict[str,str] = json.loads(filter_args_str) # BUG an apostrophe in the value causes a parsing error, single quote is converted to double quote upstream
            filter_args = {k:[sub_v.upper().strip() for sub_v in v.split(',')] for k,v in filter_args.items() if v is not None}
            filter_query_args = []
            for field, values in filter_args.items():
                if model_attr := getattr(model, field, None):
                    filter_query_args.append(
                        or_(*[model_attr.like('%'+value+'%') for value in values])
                        )
                else:
                    warnings.warn(f"Warning: filter field {field} with value {values} was ignored.")
                
            return sqla_query_obj.filter(*filter_query_args)
        return sqla_query_obj


    @staticmethod
    def _filter_deleted(model, sqla_query_obj: sqlQuery):
        """
        The 'deleted' column signals a soft delete.
        While soft deleting preserves reference integrity,
            we don't want 'deleted' values showing up in query results
        """
        field="deleted"
        if model_attr := getattr(model, field, None):
            return sqla_query_obj.filter(model_attr == None)
        else:
            return sqla_query_obj



    def _add_pagination(self, query: dict, db: Session, resource_name: str, sa_query: sqlQuery) -> tuple[dict, dict]:
        size = MAX_PAGE_SIZE
        offset = 0

        class NoPagination:
            def __init__(self, query: dict):
                self.query = {k:v for k,v in query.items() if not k.startswith("page")}
                self.metadata = {"meta":{}}

            def return_disabled_pagination(self, row_count: int):
                if row_count > MAX_RECORDS:
                    raise HTTPException(status_code=400, detail=f"This request attempted to retrieve {row_count:,} records to be retrieved exceeded the allowed maxiumum: {MAX_RECORDS:,}")
                return self.query, self.metadata
            
            def return_one_page(self):
                self.metadata = {"meta":{"totalPages": 1, "currentPage": 1}}
                return self.query, self.metadata
            
            def return_zero_page(self):
                self.metadata = {"meta":{"totalPages": 0, "currentPage": 0}}
                return self.query, self.metadata


        row_count: int = db.execute(sa_query.statement).fetchone()[0]
        if row_count == 0:      # remove any pagination if no results in the query
            return NoPagination(query).return_zero_page()
        passed_args = {k[5:-1]: v for k, v in query.items() if k.startswith('page[')}
        link_template = "/{resource_name}?page[number]={page_num}&page[size]={page_size}" # defaulting to number-size
        if passed_args:
            if {'number', 'size'} == set(passed_args.keys()):
                number = int(passed_args['number'])
                if number == 0:
                    return NoPagination(query).return_disabled_pagination(row_count=row_count) 
                size = min(int(passed_args['size']), MAX_PAGE_SIZE)
                offset = (number-1) * size
            elif {'limit', 'offset'} == set(passed_args.keys()):
                offset = int(passed_args['offset'])
                limit = int(passed_args['limit'])
                size = min(limit, MAX_PAGE_SIZE)
                link_template = "/{resource_name}?page[offset]={offset}&page[limit]={limit}"
            elif {'number'} == set(passed_args.keys()): 
                number = int(passed_args['number'])
                if number == 0:
                    return NoPagination(query).return_disabled_pagination(row_count=row_count) 
                size = MAX_PAGE_SIZE
                offset = (number-1) * size
            elif {'size'} == set(passed_args.keys()):
                number = 1
                size = min(int(passed_args['size']), MAX_PAGE_SIZE)
                offset = (number-1) * size      # == 0

        total_pages = -(row_count // -size) # ceiling division
        if total_pages == 1:        # remove pagination if there is only one page to show
            return NoPagination(query=query).return_one_page()
        else:
            current_page = (offset // size) + 1
            first_page = 1
            last_page = total_pages
            if current_page > last_page:
                current_page = last_page
                offset = (current_page-1)*size
            next_page = current_page + 1 if current_page != last_page else None
            prev_page = current_page - 1 if current_page != 1 else None
            if "number" in link_template:
                pages = {
                    "first": first_page,
                    "last": last_page,
                    "next": next_page,
                    "prev": prev_page
                }
                links = {
                    link_name: link_template.format(
                        resource_name=resource_name,
                        page_num=page_val, 
                        page_size=size
                        ) 
                    for link_name,page_val in pages.items() 
                    if page_val is not None
                }
                query.update({
                    "page[number]": str(current_page-1),
                    "page[size]": str(size)
                })
            else:
                offsets = {
                    "first": 0,
                    "last": (total_pages - 1) * size,
                    "next": (next_page - 1) * size if next_page is not None else None,
                    "prev": (prev_page - 1) * size if prev_page is not None else None
                }
                links = {
                    link_name: link_template.format(
                        resource_name=resource_name,
                        offset=offset_val,
                        limit=size
                        )
                    for link_name,offset_val in offsets.items() 
                    if offset_val is not None
                }
                query.update({
                    "page[offset]": str(offset),
                    "page[limit]": str(size)
                })


        result_addition = {
            "meta":{"totalPages": total_pages, "currentPage": current_page},
            "links": links
        }
        return query, result_addition

    def get_collection(self, session: Session, query: QueryParams|dict, model_obj, user_id: int):
        """
        Fetch a collection of resources of a specified type.

        :param session: SQLAlchemy session
        :param query: Dict of query args
        :param api_type: The type of the model

        Override of JSONAPI get_collection - adding filter parameter handling
        after instantation of session.query on the 'model'
        """

        query = self._coerce_dict(query)
        model = self._fetch_model(self.hyphenate_name(model_obj.__tablename__))
        include = self._parse_include(query.get('include', '').split(','))
        fields = self._parse_fields(query)
        included = {}
        sorts = query.get('sort', '').split(',')
        order_by = []

        if sorts == ['']:
            sorts = [DEFAULT_SORT]

        collection: sqlQuery = session.query(model)
        collection = self._apply_filter(model,collection,query)
        collection = self._filter_deleted(model, collection)

        # for pagination, use count query instead of pulling in all of the data just for a row count
        collection_count: sqlQuery = session.query(func.count(model.id))
        collection_count = self._apply_filter(model,collection_count, query_params=query)
        collection_count = self._filter_deleted(model, collection_count)
        try:
            collection = collection.filter(model.user_id == user_id)
        except AttributeError:
            pass
        query, pagination_meta_and_links = self._add_pagination(query,session,model_obj.__jsonapi_type__, collection_count)

        for attr in sorts:
            if attr == '':
                break

            attr_name, is_asc = [attr[1:], False]\
                if attr[0] == '-'\
                else [attr, True]

            if attr_name not in model.__mapper__.all_orm_descriptors.keys()\
                    or not hasattr(model, attr_name)\
                    or attr_name in model.__mapper__.relationships.keys():
                return NotSortableError(model, attr_name)

            attr = getattr(model, attr_name)
            if not hasattr(attr, 'asc'):
                # pragma: no cover
                return NotSortableError(model, attr_name)

            check_permission(model, attr_name, Permissions.VIEW)

            order_by.append(attr.asc() if is_asc else attr.desc())

        if len(order_by) > 0:
            collection = collection.order_by(*order_by)

        pos = -1
        start, end = self._parse_page(query)

        response = JSONAPIResponse()
        response.data['data'] = []

        for instance in collection:
            try:
                check_permission(instance, None, Permissions.VIEW)
            except PermissionDeniedError:
                continue

            pos += 1
            if end is not None and (pos < start or pos > end):
                continue

            built = self._render_full_resource(instance, include, fields)
            included.update(built.pop('included'))
            response.data['data'].append(built)

        response.data['included'] = list(included.values())
        if pagination_meta_and_links:
            response.data.update(pagination_meta_and_links)
            
        return response.data

    def get_resource(self, session, query, api_type, obj_id, obj_only:bool=False):
        if obj_only:
            return super().get_resource(session, query, api_type, obj_id).data
        else:
            return super().get_resource(session, query, api_type, obj_id)

    def get_relationship(self, session, query, api_type, obj_id, rel_key):
        return super().get_relationship(session, query, api_type, obj_id, rel_key).data

    def get_related(self, session, query, api_type, obj_id, rel_key):
        return super().get_related(session, query, api_type, obj_id, rel_key).data

    def post_collection(self, session, data, api_type, user_id):
        data["data"]["attributes"]["user-id"] = user_id
        return super().post_collection(session, data, api_type)


class JSONAPIRequest(Request):

    def handle_jsonapi_params(self, query_params):
        
        if not query_params:
            return QueryParams(query_params)

        expression = r'(page|filter|fields)\[(\w*)]'
        query_params = unquote(query_params).split("&")
        query_params = dict([tuple(x.split("=")) for x in query_params])
        formatted_params: dict[dict,dict,dict,str|None,str|None] = {
            "page": {},
            "filter": {},
            "fields": {},
            "include": query_params.get("include"),
            "sort": query_params.get("sort"),
        }
        for param, value in query_params.items():
            if match_ := re.match(expression,param):
                base_name = match_.group(1)
                member_name = match_.group(2)
                try:
                    new_value = {member_name: int(value)}
                except ValueError:
                    new_value = {member_name: value}
                formatted_params[base_name].update(new_value)
        formatted_param_url_string = "&".join([f'{x}={y}' for x,y in formatted_params.items() if y]).replace("\'", "\"")
        return QueryParams(formatted_param_url_string)


    def convert_key_hyphens_to_underscores(self, target: dict[str,dict|str]) -> dict:
        new_dict = target.copy()
        for key, value in target.items():
            new_key = key.replace("-","_")
            new_dict[new_key] = new_dict.pop(str(key))
            if isinstance(value,dict):
                new_dict[new_key] = self.convert_key_hyphens_to_underscores(value)
            else:
                continue
        return new_dict


    @property
    def query_params(self) -> QueryParams:
        if not hasattr(self, "_query_params"):
            query_params = super().query_params
            if self.headers.get('content-type') == 'application/vnd.api+json':
                self._query_params = self.handle_jsonapi_params(str(query_params))
            else:
                self._query_params = query_params
        return self._query_params

    async def json(self) -> Any:
        if not hasattr(self, "_json"):
            body = await self.body()
            self._json = self.convert_key_hyphens_to_underscores(json.loads(body))
        return self._json

class JSONAPIRoute(APIRoute):
    def get_route_handler(self) -> Callable:
        original_route_handler = super().get_route_handler()

        async def custom_route_handler(request: Request) -> Response:
            request = JSONAPIRequest(request.scope, request.receive)
            return await original_route_handler(request)

        return custom_route_handler

def jsonapi_error_handling(route_function):
    @functools.wraps(route_function)
    def error_handling(*args, **kwargs):
        try:
            return route_function(*args, **kwargs)
        except BaseError as err:
            import traceback
            raise HTTPException(**format_error(err, traceback.format_exc()))
        except HTTPException as http_e:
            raise http_e
        except Exception as err:
            import traceback
            detail_obj = {"errors": [{"traceback": traceback.format_exc(),"detail":"An error occurred. Contact joe@carbonitech.com with the id number"}]}
            raise HTTPException(status_code=400,detail=detail_obj)
    return error_handling
