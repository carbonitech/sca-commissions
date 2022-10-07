import json
import warnings
from pydantic import BaseModel
from sqlalchemy import or_
from sqlalchemy_jsonapi.errors import NotSortableError, PermissionDeniedError
from sqlalchemy_jsonapi.serializer import Permissions, JSONAPIResponse, check_permission
from sqlalchemy_jsonapi import JSONAPI

DEFAULT_SORT: str = "id"

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
    
    def get_collection(self, session, query, api_key):
        """
        Fetch a collection of resources of a specified type.

        :param session: SQLAlchemy session
        :param query: Dict of query args
        :param api_type: The type of the model

        Override of JSONAPI get_collection - adding filter parameter handling
        after instantation of session.query on the 'model'
        """


        model = self._fetch_model(api_key)
        include = self._parse_include(query.get('include', '').split(','))
        fields = self._parse_fields(query)
        included = {}
        sorts = query.get('sort', '').split(',')
        order_by = []

        if sorts == ['']:
            sorts = [DEFAULT_SORT]

        collection = session.query(model)

        ## addition by Joseph Carboni
        if (filter_args_str := query.get('filter')):
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
                
            collection = collection.filter(*filter_query_args)
        ####
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
        return response
