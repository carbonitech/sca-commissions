"""Database Table Models"""

from sqlalchemy import Column, Float, Integer, String, Boolean, DateTime, TEXT, ForeignKey
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

## Core Tables
class City(Base):
    __tablename__ = 'cities'
    id = Column(Integer,primary_key=True)
    name = Column(String)    
    branch_cities = relationship("CustomerBranch")
    map_names = relationship("MapCityName")


class State(Base):
    __tablename__ = 'states'
    id = Column(Integer,primary_key=True)
    name = Column(String)
    branch_states = relationship("CustomerBranch")


class Customer(Base):
    __tablename__ = 'customers'
    id = Column(Integer,primary_key=True)
    name = Column(String)
    branches = relationship("CustomerBranch")
    map_names = relationship("MapCustomerName")


class CustomerBranch(Base):
    __tablename__ = 'customer_branches'
    id = Column(Integer,primary_key=True)
    customer_id = Column(Integer, ForeignKey("customers.id"))
    city = Column(Integer, ForeignKey("cities.id"))
    state = Column(Integer, ForeignKey("states.id"))
    rep = relationship("MapRepsToCustomer")


class Manufacturer(Base):
    __tablename__ = 'manufacturers'
    id = Column(Integer,primary_key=True)
    name = Column(String)


class ManufacturersReport(Base):
    __tablename__ = 'manufacturers_reports'
    id = Column(Integer,primary_key=True)
    manufacturer_id = Column(Integer, ForeignKey("manufacturers.id"))
    report_name = Column(String)
    yearly_frequency = Column(Integer)
    POS_report = Column(Boolean)


class Representative(Base):
    __tablename__ = 'representatives'
    id = Column(Integer,primary_key=True)
    first_name = Column(String)
    last_name = Column(String)
    initials = Column(String)
    date_joined = Column(DateTime)
    branches = relationship("MapRepsToCustomer")


## Mappings
class MapCustomerName(Base):
    __tablename__ = 'map_customer_name'
    id = Column(Integer,primary_key=True)
    recorded_name = Column(String)
    customer_id = Column(Integer, ForeignKey("customers.id"))


class MapCityName(Base):
    __tablename__ = 'map_city_names'
    id = Column(Integer,primary_key=True)
    recorded_name = Column(String)
    city_id = Column(Integer, ForeignKey("cities.id"))


class MapRepsToCustomer(Base):
    __tablename__ = 'map_reps_customers'
    id = Column(Integer,primary_key=True)
    rep_id = Column(Integer, ForeignKey("representatives.id"))
    customer_branch_id = Column(Integer, ForeignKey("customer_branches.id"))
    commission_data = relationship("FinalCommissionData")

## Submissions
class ReportSubmissionsLog(Base):
    __tablename__ = 'report_submissions_log'
    id = Column(Integer,primary_key=True)
    submission_date = Column(DateTime)
    reporting_month = Column(Integer)
    reporting_year = Column(Integer)
    report_id = Column(Integer, ForeignKey("manufacturers_reports.id"))
    commission_data = relationship("FinalCommissionData")
    errors = relationship("CurrentError")
    steps = relationship("ReportProcessingStepsLog")


class ReportProcessingStepsLog(Base):
    __tablename__ = 'report_processing_steps_log'
    id = Column(Integer,primary_key=True)
    submission_id = Column(Integer, ForeignKey("report_submissions_log.id"))
    step_num = Column(Integer)
    description = Column(String)


class CurrentError(Base):
    __tablename__ = 'current_errors'
    id = Column(Integer,primary_key=True)
    submission_id = Column(Integer, ForeignKey("report_submissions_log.id"))
    row_index = Column(Integer)
    field = Column(String)
    value_type = Column(String)
    value_content = Column(String)
    reason = Column(String)
    row_data = Column(TEXT)


## Final Data
class FinalCommissionData(Base):
    __tablename__ = 'final_commission_data'
    row_id = Column(Integer,primary_key=True)
    submission_id = Column(Integer, ForeignKey("report_submissions_log.id"))
    map_rep_customer_id = Column(Integer, ForeignKey("map_reps_customers.id"))
    inv_amt = Column(Float)
    comm_amt = Column(Float)

