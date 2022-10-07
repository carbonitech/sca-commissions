import json
import warnings
from pydantic import BaseModel
from sqlalchemy import or_, select, func
from sqlalchemy.orm import Session, Query as sqlQuery
from sqlalchemy_jsonapi.errors import NotSortableError, PermissionDeniedError
from sqlalchemy_jsonapi.serializer import Permissions, JSONAPIResponse, check_permission
from sqlalchemy_jsonapi import JSONAPI
from starlette.requests import QueryParams

DEFAULT_SORT: str = "id"
MAX_PAGE_SIZE: int = 300
class Query(BaseModel):
    include: str|None = None
    sort: str|None = None
    fields: str|None = None
    filter: str|None = None
    page: str|None = None

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

class JSONAPI_(JSONAPI):
    """custom fixes applied to the JSONAPI object"""

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
        """"""
        if (filter_args_str := query_params.get('filter')):
            filter_args: dict[str,str] = json.loads(filter_args_str)
            filter_args = {k:[sub_v.upper().strip() for sub_v in v.split(',')] for k,v in filter_args.items() if v is not None}
            filter_query_args = []
            for field, values in filter_args.items():
                try:
                    model_attr = getattr(model, field)
                except Exception as err:
                    warnings.warn(f"Warning: field {field} with value {values} was not evaluated as a filter because {str(err)}. Filter argument was ignored.")
                    continue
                filter_query_args.append(
                    or_(*[model_attr.like('%'+value+'%') for value in values])
                    )
                
            return sqla_query_obj.filter(*filter_query_args)
        return sqla_query_obj

    def _add_pagination(self, query: dict, db: Session, resource) -> tuple[dict, dict]:
        resource_name: str = resource.__jsonapi_type__
        size = MAX_PAGE_SIZE
        offset = 0
        row_cnt_sql = select([func.count()]).select_from(resource)
        row_cnt_sql = self._apply_filter(resource,row_cnt_sql,query)

        row_count: int = db.execute(row_cnt_sql).scalar()
        if row_count == 0:
            return query, {"meta":{"totalPages": 0, "currentPage": 0}}
        passed_args = {k[5:-1]: v for k, v in query.items() if k.startswith('page[')}
        link_template = "/{resource_name}?page[number]={page_num}&page[size]={page_size}" # defaulting to number-size
        if passed_args:
            if {'number', 'size'} == set(passed_args.keys()):
                number = int(passed_args['number'])
                size = min(int(passed_args['size']), MAX_PAGE_SIZE)
                offset = (number-1) * size
            elif {'limit', 'offset'} == set(passed_args.keys()):
                offset = int(passed_args['offset'])
                limit = int(passed_args['limit'])
                size = min(limit, MAX_PAGE_SIZE)
                link_template = "/{resource_name}?page[offset]={offset}&page[limit]={limit}"

        total_pages = -(row_count // -size) # ceiling division
        if total_pages == 1:
            query = {k:v for k,v in query.items() if not k.startswith("page")} # remove any pagination if there aren't any pages
            return query, {"meta":{"totalPages": 1, "currentPage": 1}} # include info that there's only one "page" even though no pagination occurred
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


    def get_collection(self, session: Session, query: QueryParams|dict, model_obj):
        """
        Fetch a collection of resources of a specified type.

        :param session: SQLAlchemy session
        :param query: Dict of query args
        :param api_type: The type of the model

        Override of JSONAPI get_collection - adding filter parameter handling
        after instantation of session.query on the 'model'
        """

        query = self._coerce_dict(query)
        query, pagination_meta_and_links = self._add_pagination(query,session,model_obj)
        model = self._fetch_model(self.hyphenate_name(model_obj.__tablename__))
        include = self._parse_include(query.get('include', '').split(','))
        fields = self._parse_fields(query)
        included = {}
        sorts = query.get('sort', '').split(',')
        order_by = []

        if sorts == ['']:
            sorts = [DEFAULT_SORT]

        collection = session.query(model)
        collection = self._apply_filter(model,collection,query)

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
            
        return response
