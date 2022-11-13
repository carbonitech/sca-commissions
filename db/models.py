"""Database Table Models / Data Transfer Objects"""

import uuid
from datetime import datetime
from sqlalchemy import Column, Float, Integer, String, Boolean, DateTime, TEXT, ForeignKey, Enum
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.dialects.postgresql import UUID
from app.jsonapi import JSONAPI_

Base = declarative_base()

class City(Base):
    __tablename__ = 'cities'
    id = Column(Integer,primary_key=True)
    name = Column(String, unique=True)
    deleted = Column(DateTime)
    user_id = Column(Integer, ForeignKey("users.id"))
    branch_cities = relationship("CustomerBranch", back_populates="city_name")
    map_city_names = relationship("MapCityName", back_populates="city_name")

class MapCityName(Base):
    __tablename__ = 'map_city_names'
    id = Column(Integer,primary_key=True)
    recorded_name = Column(String, unique=True)
    city_id = Column(Integer, ForeignKey("cities.id"))
    city_name = relationship("City", back_populates="map_city_names")


class State(Base):
    __tablename__ = 'states'
    id = Column(Integer,primary_key=True)
    name = Column(String, unique=True)
    deleted = Column(DateTime)
    user_id = Column(Integer, ForeignKey("users.id"))
    branch_states = relationship("CustomerBranch", back_populates="state_name")
    map_state_names = relationship("MapStateName", back_populates="state_name")

class MapStateName(Base):
    __tablename__ = 'map_state_names'
    id = Column(Integer,primary_key=True)
    recorded_name = Column(String, unique=True)
    state_id = Column(Integer, ForeignKey("states.id"))
    state_name = relationship("State", back_populates="map_state_names")


class Customer(Base):
    __tablename__ = 'customers'
    id = Column(Integer,primary_key=True)
    name = Column(String)
    deleted = Column(DateTime)
    user_id = Column(Integer, ForeignKey("users.id"))
    customer_branches = relationship("CustomerBranch", back_populates="customers")
    map_customer_names = relationship("MapCustomerName", back_populates="customers")

class MapCustomerName(Base):
    __tablename__ = 'map_customer_names'
    id = Column(Integer,primary_key=True)
    recorded_name = Column(String, unique=True)
    customer_id = Column(Integer, ForeignKey("customers.id"))
    customers = relationship("Customer", back_populates="map_customer_names")

class CustomerBranch(Base):
    __tablename__ = 'customer_branches'
    id = Column(Integer,primary_key=True)
    customer_id = Column(Integer, ForeignKey("customers.id"))
    city_id = Column(Integer, ForeignKey("cities.id"))
    state_id = Column(Integer, ForeignKey("states.id"))
    deleted = Column(DateTime)
    store_number = Column(String)
    rep_id = Column(Integer, ForeignKey("representatives.id"))
    in_territory = Column(Boolean)
    customers = relationship("Customer", back_populates="customer_branches")
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
    user_id = Column(Integer, ForeignKey("users.id"))
    branch = relationship("CustomerBranch", back_populates="representative")


class Manufacturer(Base):
    __tablename__ = 'manufacturers'
    id = Column(Integer,primary_key=True)
    name = Column(String)
    deleted = Column(DateTime)
    user_id = Column(Integer, ForeignKey("users.id"))
    manufacturers_reports = relationship("ManufacturersReport", back_populates="manufacturer")
    commission_rates = relationship("UserCommissionRate")

class ManufacturersReport(Base):
    __tablename__ = 'manufacturers_reports'
    id = Column(Integer,primary_key=True)
    manufacturer_id = Column(Integer, ForeignKey("manufacturers.id"))
    report_name = Column(String)
    yearly_frequency = Column(Integer)
    pos_report = Column(Boolean)
    deleted = Column(DateTime)
    manufacturer = relationship("Manufacturer", back_populates="manufacturers_reports")
    submissions = relationship("Submission", back_populates="manufacturers_reports")
    report_form_fields = relationship("ReportFormFields", back_populates='manufacturers_report')
    commission_split = relationship("CommissionSplit")


class Submission(Base):
    __tablename__ = 'submissions'
    id = Column(Integer,primary_key=True)
    submission_date = Column(DateTime)
    reporting_month = Column(Integer)
    reporting_year = Column(Integer)
    report_id = Column(Integer, ForeignKey("manufacturers_reports.id"))
    total_commission_amount = Column(Float)
    user_id = Column(Integer, ForeignKey("users.id"))
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

class ReportFormFields(Base):
    __tablename__ = "report_form_fields"
    id = Column(Integer,primary_key=True)
    report_id = Column(Integer, ForeignKey("manufacturers_reports.id"))
    label = Column(String)
    name = Column(String)
    default_value = Column(Integer)
    min_value = Column(Integer)
    max_value = Column(Integer)
    options = Column(TEXT)
    required = Column(Boolean)
    input_type = Column(Enum('TEXT','NUMBER', name='input'))
    manufacturers_report = relationship("ManufacturersReport", back_populates="report_form_fields")

class Failures(Base):
    __tablename__ = "failures"
    id = Column(UUID(as_uuid=True),primary_key=True, default=uuid.uuid4)
    occurred_at = Column(DateTime, default=datetime.utcnow)
    request = Column(TEXT)
    response = Column(TEXT)
    traceback = Column(TEXT)

class User(Base):
    __tablename__ = "users"
    id = Column(Integer,primary_key=True)
    company_domain = Column(String)
    commission_rates = relationship("UserCommissionRate")
    commission_splits = relationship("CommissionSplit")
    customers = relationship("Customer")
    cities = relationship("City")
    states = relationship("State")
    representatives = relationship("Representative")
    manufacturers = relationship("Manufacturer")
    submissions = relationship("Submission")


class UserCommissionRate(Base):
    __tablename__ = "user_commission_rates"
    id = Column(Integer,primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    manufacturer_id = Column(Integer, ForeignKey("manufacturers.id"))
    commission_rate = Column(Float)

class CommissionSplit(Base):
    __tablename__ = "commission_splits"
    id = Column(Integer,primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    report_id = Column(Integer, ForeignKey("manufacturers_reports.id"))
    split_proportion = Column(Float)
    



setattr(Base,"_decl_class_registry",Base.registry._class_registry) # because JSONAPI's constructor is broken for SQLAchelmy 1.4.x
serializer = JSONAPI_(Base)