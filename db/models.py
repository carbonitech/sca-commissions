"""Database Table Models / Data Transfer Objects"""

import json
import warnings
from datetime import datetime
from sqlalchemy import Column, Float, Integer, String, Boolean, DateTime, TEXT, ForeignKey, or_
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy_jsonapi import JSONAPI

Base = declarative_base()

class City(Base):
    __tablename__ = 'cities'
    id = Column(Integer,primary_key=True)
    name = Column(String)
    deleted = Column(DateTime)
    branch_cities = relationship("CustomerBranch", back_populates="city_name")
    map_city_names = relationship("MapCityName", back_populates="city_name")

class MapCityName(Base):
    __tablename__ = 'map_city_names'
    id = Column(Integer,primary_key=True)
    recorded_name = Column(String)
    city_id = Column(Integer, ForeignKey("cities.id"))
    city_name = relationship("City", back_populates="map_city_names")


class State(Base):
    __tablename__ = 'states'
    id = Column(Integer,primary_key=True)
    name = Column(String)
    deleted = Column(DateTime)
    branch_states = relationship("CustomerBranch", back_populates="state_name")
    map_state_names = relationship("MapStateName", back_populates="state_name")

class MapStateName(Base):
    __tablename__ = 'map_state_names'
    id = Column(Integer,primary_key=True)
    recorded_name = Column(String)
    state_id = Column(Integer, ForeignKey("states.id"))
    state_name = relationship("State", back_populates="map_state_names")


class Customer(Base):
    __tablename__ = 'customers'
    id = Column(Integer,primary_key=True)
    name = Column(String)
    deleted = Column(DateTime)
    customer_branches = relationship("CustomerBranch", back_populates="customer")
    map_customer_name = relationship("MapCustomerName", back_populates="customer")

class MapCustomerName(Base):
    __tablename__ = 'map_customer_name'
    id = Column(Integer,primary_key=True)
    recorded_name = Column(String)
    customer_id = Column(Integer, ForeignKey("customers.id"))
    customer = relationship("Customer", back_populates="map_customer_name")

class CustomerBranch(Base):
    __tablename__ = 'customer_branches'
    id = Column(Integer,primary_key=True)
    customer_id = Column(Integer, ForeignKey("customers.id"))
    city_id = Column(Integer, ForeignKey("cities.id"))
    state_id = Column(Integer, ForeignKey("states.id"))
    deleted = Column(DateTime)
    store_number = Column(Integer)
    rep_id = Column(Integer, ForeignKey("representatives.id"))
    customer = relationship("Customer", back_populates="customer_branches")
    city_name = relationship("City", back_populates="branch_cities")
    state_name = relationship("State", back_populates="branch_states")
    representative = relationship("Representative", back_populates="branch")
    commission_data = relationship("CommissionData", back_populates="branch")


class Representative(Base):
    __tablename__ = 'representatives'
    id = Column(Integer,primary_key=True)
    first_name = Column(String)
    last_name = Column(String)
    initials = Column(String)
    date_joined = Column(DateTime)
    deleted = Column(DateTime)
    branch = relationship("CustomerBranch", back_populates="representative")


class Manufacturer(Base):
    __tablename__ = 'manufacturers'
    id = Column(Integer,primary_key=True)
    name = Column(String)
    deleted = Column(DateTime)
    manufacturers_reports = relationship("ManufacturersReport", back_populates="manufacturer")

class ManufacturersReport(Base):
    __tablename__ = 'manufacturers_reports'
    id = Column(Integer,primary_key=True)
    manufacturer_id = Column(Integer, ForeignKey("manufacturers.id"))
    report_name = Column(String)
    yearly_frequency = Column(Integer)
    POS_report = Column(Boolean)
    deleted = Column(DateTime)
    manufacturer = relationship("Manufacturer", back_populates="manufacturers_reports")
    submissions = relationship("Submission", back_populates="manufacturers_reports")


class Submission(Base):
    __tablename__ = 'submissions'
    id = Column(Integer,primary_key=True)
    submission_date = Column(DateTime)
    reporting_month = Column(Integer)
    reporting_year = Column(Integer)
    report_id = Column(Integer, ForeignKey("manufacturers_reports.id"))
    manufacturers_reports = relationship("ManufacturersReport", back_populates="submissions")
    commission_data = relationship("CommissionData", back_populates="submission")
    errors = relationship("Error", back_populates="submission")
    processing_steps = relationship("ProcessingStep", back_populates="submission")


class ProcessingStep(Base):
    __tablename__ = 'processing_steps'
    id = Column(Integer,primary_key=True)
    submission_id = Column(Integer, ForeignKey("submissions.id"))
    step_num = Column(Integer)
    description = Column(String)
    submission = relationship("Submission", back_populates="processing_steps")
    

class Error(Base):
    __tablename__ = 'errors'
    id = Column(Integer,primary_key=True)
    submission_id = Column(Integer, ForeignKey("submissions.id"))
    row_index = Column(Integer)
    reason = Column(Integer)
    row_data = Column(TEXT)
    submission = relationship("Submission", back_populates="errors")


class CommissionData(Base):
    __tablename__ = 'commission_data'
    id = Column(Integer,primary_key=True)
    recorded_at = Column(DateTime, default = datetime.now())
    submission_id = Column(Integer, ForeignKey("submissions.id"))
    customer_branch_id = Column(Integer, ForeignKey("customer_branches.id"))
    inv_amt = Column(Float)
    comm_amt = Column(Float)
    branch = relationship("CustomerBranch", back_populates="commission_data")
    submission = relationship("Submission", back_populates="commission_data")

class FileDownloads(Base):
    __tablename__ = "file_downloads"
    id = Column(Integer,primary_key=True)
    hash = Column(String)
    type = Column(String)
    query_args = Column(TEXT)
    created_at = Column(DateTime)
    expires_at = Column(DateTime)
    downloaded = Column(Boolean, default=False)


from sqlalchemy_jsonapi.errors import NotSortableError, PermissionDeniedError
from sqlalchemy_jsonapi.serializer import Permissions, JSONAPIResponse, check_permission

class JSONAPI_(JSONAPI):

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

setattr(Base,"_decl_class_registry",Base.registry._class_registry) # because JSONAPI's constructor is broken for SQLAchelmy 1.4.x
serializer = JSONAPI_(Base)
