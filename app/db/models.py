"""Database Table Models"""

from sqlalchemy import Column, Float, Integer, String, Boolean, DateTime, TEXT, ForeignKey
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class Customer(Base):
    __tablename__ = 'customers'
    id = Column(Integer,primary_key=True)
    name = Column(String)
    branches = relationship("CustomerBranch")


class CustomerBranch(Base):
    __tablename__ = 'customer_branches'
    id = Column(Integer,primary_key=True)
    customer_id = Column(Integer, ForeignKey("customers.id"))
    city = Column(String)
    state = Column(String)
    zip = Column(Integer)
    rep = relationship("MapRepsToCustomer")


class MapCustomerName(Base):
    __tablename__ = 'map_customer_name'
    id = Column(Integer,primary_key=True)
    recorded_name = Column(String)
    standard_name = Column(String)


class MapCityName(Base):
    __tablename__ = 'map_city_names'
    id = Column(Integer,primary_key=True)
    recorded_name = Column(String)
    standard_name = Column(String)


class MapRepsToCustomer(Base):
    __tablename__ = 'map_reps_customers'
    id = Column(Integer,primary_key=True)
    rep_id = Column(Integer, ForeignKey("representatives.id"))
    customer_branch_id = Column(Integer, ForeignKey("customer_branches.id"))


class Manufacturer(Base):
    __tablename__ = 'manufacturers'
    id = Column(Integer,primary_key=True)
    name = Column(String)


class ManufacturersReport(Base):
    __tablename__ = 'manufacturers_reports'
    id = Column(Integer,primary_key=True)
    manufacturer_id = Column(Integer)
    report_name = Column(String)
    yearly_frequency = Column(Integer)
    POS_report = Column(Boolean)


class ReportSubmissionsLog(Base):
    __tablename__ = 'report_submissions_log'
    id = Column(Integer,primary_key=True)
    submission_date = Column(DateTime)
    reporting_month = Column(Integer)
    reporting_year = Column(Integer)
    report_id = Column(Integer)


class FinalCommissionData(Base):    # TODO: make this table customizable instead of hard-coded
    __tablename__ = 'final_commission_data'
    id = Column(Integer,primary_key=True)
    submission_id = Column(Integer)
    year = Column(Integer)
    month = Column(String)
    manufacturer = Column(String)
    salesman = Column(String)
    customer_name = Column(String)
    city = Column(String)
    state = Column(String)
    inv_amt = Column(Float)
    comm_amt = Column(Float)


class ReportProcessingStepsLog(Base):
    __tablename__ = 'report_processing_steps_log'
    id = Column(Integer,primary_key=True)
    submission_id = Column(Integer)
    step_num = Column(Integer)
    description = Column(String)


class CurrentError(Base):
    __tablename__ = 'current_errors'
    id = Column(Integer,primary_key=True)
    submission_id = Column(Integer)
    row_index = Column(Integer)
    field = Column(String)
    value_type = Column(String)
    value_content = Column(String)
    reason = Column(String)
    row_data = Column(TEXT)


class Representative(Base):
    __tablename__ = 'representatives'
    id = Column(Integer,primary_key=True)
    first_name = Column(String)
    last_name = Column(String)
    initials = Column(String)
    date_joined = Column(DateTime)
    branches = relationship("MapRepsToCustomer")
