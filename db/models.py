"""Database Table Models / Data Transfer Objects"""

import uuid
from datetime import datetime
from sqlalchemy import Column, Float, Integer, String, Boolean, DateTime, TEXT, ForeignKey, Enum, UniqueConstraint, Numeric
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.dialects.postgresql import UUID
from app.jsonapi import JSONAPI_

Base = declarative_base()

class Customer(Base):
    __tablename__ = 'customers'
    id = Column(Integer,primary_key=True)
    name = Column(String)
    deleted = Column(DateTime)
    user_id = Column(Integer, ForeignKey("users.id"))
    customer_branches = relationship("CustomerBranch", back_populates="customers")

class CustomerBranch(Base):
    __tablename__ = 'customer_branches'
    id = Column(Integer,primary_key=True)
    customer_id = Column(Integer, ForeignKey("customers.id"))
    rep_id = Column(Integer, ForeignKey("representatives.id"))
    user_id = Column(Integer, ForeignKey("users.id"))
    location_id = Column(Integer, ForeignKey("locations.id"))
    customers = relationship("Customer", back_populates="customer_branches")
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
    user_id = Column(Integer, ForeignKey("users.id"))
    report_label = Column(String)
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
    status = Column(Enum('QUEUED', 'PROCESSING', 'COMPLETE', 'NEEDS_ATTENTION', 'FAILED', name='status'))
    manufacturers_reports = relationship("ManufacturersReport", back_populates="submissions")
    commission_data = relationship("CommissionData", back_populates="submission")
    errors = relationship("Error", back_populates="submission")
    processing_steps = relationship("ProcessingStep", back_populates="submission")

class Error(Base):
    __tablename__ = 'errors'
    id = Column(Integer,primary_key=True)
    submission_id = Column(Integer, ForeignKey("submissions.id"))
    reason = Column(Integer)
    row_data = Column(TEXT)
    user_id = Column(Integer, ForeignKey("users.id"))
    submission = relationship("Submission", back_populates="errors")

class CommissionData(Base):
    __tablename__ = 'commission_data'
    id = Column(Integer,primary_key=True)
    recorded_at = Column(DateTime, default = datetime.now())
    submission_id = Column(Integer, ForeignKey("submissions.id"))
    customer_branch_id = Column(Integer, ForeignKey("customer_branches.id"))
    inv_amt = Column(Float)
    comm_amt = Column(Float)
    user_id = Column(Integer, ForeignKey("users.id"))
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
    user_id = Column(Integer, ForeignKey("users.id"))

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
    input_type = Column(Enum('TEXT','NUMBER','OPTIONS','FILE', name='input'))
    sort_order = Column(Integer)
    user_id = Column(Integer, ForeignKey("users.id"))
    manufacturers_report = relationship("ManufacturersReport", back_populates="report_form_fields")

class Failures(Base):
    __tablename__ = "failures"
    id = Column(UUID(as_uuid=True),primary_key=True, default=uuid.uuid4)
    occurred_at = Column(DateTime, default=datetime.utcnow)
    request = Column(TEXT)
    response = Column(TEXT)
    traceback = Column(TEXT)
    user_id = Column(Integer, ForeignKey("users.id"))

class User(Base):
    __tablename__ = "users"
    id = Column(Integer,primary_key=True)
    company_domain = Column(String)
    commission_rates = relationship("UserCommissionRate")
    commission_splits = relationship("CommissionSplit")
    customers = relationship("Customer")
    branches = relationship("CustomerBranch")
    representatives = relationship("Representative")
    manufacturers = relationship("Manufacturer")
    manufacturers_reports = relationship("ManufacturersReport")
    submissions = relationship("Submission")
    processing_steps = relationship("ProcessingStep")
    errors = relationship("Error")
    commission_data = relationship("CommissionData")
    file_downloads = relationship("FileDownloads")
    report_form_fields = relationship("ReportFormFields")
    failures = relationship("Failures")

class UserToken(Base):
    __tablename__ = "user_tokens"
    id = Column(Integer,primary_key=True)
    access_token = Column(String)
    nickname = Column(String)
    name = Column(String)
    email = Column(String)
    verified = Column(Boolean)
    expires_at = Column(Integer)


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
    
class Location(Base):
    __tablename__ = 'locations'
    id = Column(Integer,primary_key=True)
    city = Column(String(200))
    state = Column(String(20))
    lat = Column(Numeric)
    long = Column(Numeric)

class IDStringMatch(Base):
    __tablename__ = "id_string_matches"
    id = Column(Integer,primary_key=True)
    match_string = Column(String)
    report_id = Column(Integer, ForeignKey("manufacturers_reports.id"))
    customer_branch_id = Column(Integer, ForeignKey("customer_branches.id"))
    created_at = Column(DateTime)
    auto_matched = Column(Boolean)
    user_id = Column(Integer, ForeignKey("users.id"))
    

setattr(Base,"_decl_class_registry",Base.registry._class_registry) # because JSONAPI's constructor is broken for SQLAchelmy 1.4.x
serializer = JSONAPI_(Base)