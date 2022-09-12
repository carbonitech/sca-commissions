"""Database Table Models / Data Transfer Objects"""

from datetime import datetime
from sqlalchemy import Column, Float, Integer, String, Boolean, DateTime, TEXT, ForeignKey
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy_jsonapi import JSONAPI

Base = declarative_base()

## Model-only Tables
class City(Base):
    __tablename__ = 'cities'
    id = Column(Integer,primary_key=True)
    name = Column(String)
    deleted = Column(DateTime)
    branch_cities = relationship("CustomerBranch", back_populates="city_name")
    map_names = relationship("MapCityName", back_populates="city_name")

class MapCityName(Base):
    __tablename__ = 'map_city_names'
    id = Column(Integer,primary_key=True)
    recorded_name = Column(String)
    city_id = Column(Integer, ForeignKey("cities.id"))
    city_name = relationship("City", back_populates="map_names")


class State(Base):
    __tablename__ = 'states'
    id = Column(Integer,primary_key=True)
    name = Column(String)
    deleted = Column(DateTime)
    branch_states = relationship("CustomerBranch", back_populates="state_name")
    map_names = relationship("MapStateName", back_populates="state_name")

class MapStateName(Base):
    __tablename__ = 'map_state_names'
    id = Column(Integer,primary_key=True)
    recorded_name = Column(String)
    state_id = Column(Integer, ForeignKey("states.id"))
    state_name = relationship("State", back_populates="map_names")


class Customer(Base):
    __tablename__ = 'customers'
    id = Column(Integer,primary_key=True)
    name = Column(String)
    deleted = Column(DateTime)
    customer_branches = relationship("CustomerBranch", back_populates="customer")
    map_names = relationship("MapCustomerName", back_populates="customer")

class MapCustomerName(Base):
    __tablename__ = 'map_customer_name'
    id = Column(Integer,primary_key=True)
    recorded_name = Column(String)
    customer_id = Column(Integer, ForeignKey("customers.id"))
    customer = relationship("Customer", back_populates="map_names")

class CustomerBranch(Base):
    __tablename__ = 'customer_branches'
    id = Column(Integer,primary_key=True)
    customer_id = Column(Integer, ForeignKey("customers.id"))
    city_id = Column(Integer, ForeignKey("cities.id"))
    state_id = Column(Integer, ForeignKey("states.id"))
    deleted = Column(DateTime)
    rep = relationship("MapRepToCustomer", back_populates="branch")
    customer = relationship("Customer", back_populates="customer_branches")
    city_name = relationship("City", back_populates="branch_cities")
    state_name = relationship("State", back_populates="branch_states")

class MapRepToCustomer(Base):
    __tablename__ = 'map_reps_customers'
    id = Column(Integer,primary_key=True)
    rep_id = Column(Integer, ForeignKey("representatives.id"))
    customer_branch_id = Column(Integer, ForeignKey("customer_branches.id"))
    orphaned = Column(DateTime)
    commission_data = relationship("FinalCommissionDataDTO", back_populates="rep_customer")
    rep_name = relationship("Representative", back_populates="branches")
    branch = relationship("CustomerBranch", back_populates="rep")


class Representative(Base):
    __tablename__ = 'representatives'
    id = Column(Integer,primary_key=True)
    first_name = Column(String)
    last_name = Column(String)
    initials = Column(String)
    date_joined = Column(DateTime)
    deleted = Column(DateTime)
    branches = relationship("MapRepToCustomer", back_populates="rep_name")


class ManufacturersReport(Base):
    __tablename__ = 'manufacturers_reports'
    id = Column(Integer,primary_key=True)
    manufacturer_id = Column(Integer, ForeignKey("manufacturers.id"))
    report_name = Column(String)
    yearly_frequency = Column(Integer)
    POS_report = Column(Boolean)
    deleted = Column(DateTime)

## Entity DTOs
class ManufacturerDTO(Base):
    __tablename__ = 'manufacturers'
    id = Column(Integer,primary_key=True)
    name = Column(String)
    deleted = Column(DateTime)
    manuf_reports = relationship("ManufacturersReport")


class SubmissionDTO(Base):
    __tablename__ = 'submissions'
    id = Column(Integer,primary_key=True)
    submission_date = Column(DateTime)
    reporting_month = Column(Integer)
    reporting_year = Column(Integer)
    report_id = Column(Integer, ForeignKey("manufacturers_reports.id"))
    commission_data = relationship("FinalCommissionDataDTO")
    errors = relationship("ErrorDTO")
    steps = relationship("ProcessingStepDTO")


class ProcessingStepDTO(Base):
    __tablename__ = 'processing_steps'
    id = Column(Integer,primary_key=True)
    submission_id = Column(Integer, ForeignKey("submissions.id"))
    step_num = Column(Integer)
    description = Column(String)
    

class ErrorDTO(Base):
    __tablename__ = 'errors'
    id = Column(Integer,primary_key=True)
    submission_id = Column(Integer, ForeignKey("submissions.id"))
    row_index = Column(Integer)
    reason = Column(Integer)
    row_data = Column(TEXT)


class FinalCommissionDataDTO(Base):
    __tablename__ = 'final_commission_data'
    row_id = Column(Integer,primary_key=True)
    recorded_at = Column(DateTime, default = datetime.now())
    submission_id = Column(Integer, ForeignKey("submissions.id"))
    map_rep_customer_id = Column(Integer, ForeignKey("map_reps_customers.id"))
    inv_amt = Column(Float)
    comm_amt = Column(Float)
    rep_customer = relationship("MapRepToCustomer", back_populates="commission_data")

setattr(Base,"_decl_class_registry",Base.registry._class_registry) # because JSONAPI's constructor is broken
serializer = JSONAPI(Base)