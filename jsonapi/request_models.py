from pydantic import BaseModel, ConfigDict, Field
from datetime import datetime
from dataclasses import dataclass
from typing import Optional

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
    deleted: datetime | None


class BranchRelationship(BaseModel):
    representative: JSONAPIRelationshipObject
    locations: JSONAPIRelationshipObject
    customers: JSONAPIRelationshipObject


class BranchRelatonshipFull(BaseModel):
    customers: JSONAPIRelationshipObject | None
    locations: JSONAPIRelationshipObject | None
    representative: JSONAPIRelationshipObject | None


class BranchModification(JSONAPIBaseModification):
    id: int
    attributes: Branch | dict | None = {}
    relationships: BranchRelationship | dict | None = {}


class NewBranch(JSONAPIBaseModification):
    attributes: dict | None
    relationships: BranchRelatonshipFull


## mapping ##
class Mapping(BaseModel):
    match_string: str
    created_at: datetime
    auto_matched: bool


class MappingRelationship(BaseModel):
    branches: JSONAPIRelationshipObject
    manufacturers_reports: Optional[JSONAPIRelationshipObject] = Field(
        default=None, alias="manufacturers-reports"
    )


class NewMapping(JSONAPIBaseModification):
    attributes: Mapping
    relationships: MappingRelationship


class MappingModification(JSONAPIBaseModification):
    id: int
    relationships: MappingRelationship


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


### submissions ###
class Submission(BaseModel):
    total_commission_amount: float | int
    submission_date: datetime
    reporting_month: int
    status: str
    reporting_year: int


class SubmissionModification(JSONAPIBaseModification):
    id: int
    attributes: Submission


### manufacturers ###
class Manufacturer(BaseModel):
    name: str
    deleted: datetime | None


class NewManufacturer(JSONAPIBaseModification):
    attributes: Manufacturer
    relationships: dict | None = None


### representatives ###
class Representative(BaseModel):
    initials: str
    first_name: str
    last_name: str
    date_joined: datetime


class NewRepresentative(JSONAPIBaseModification):
    attributes: Representative


class RepresentativeModification(JSONAPIBaseModification):
    id: int
    attributes: Representative


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


class SubmissionModificationRequest(BaseModel):
    data: SubmissionModification


class NewManufacturerRequest(BaseModel):
    data: NewManufacturer


class NewRepresentativeRequest(BaseModel):
    data: NewRepresentative


class RepresentativeModificationRequest(BaseModel):
    data: RepresentativeModification


class MappingModificdationRequest(BaseModel):
    data: MappingModification


@dataclass
class RequestModels:
    new_customer = NewCustomerRequest
    new_branch = NewBranchRequest
    new_mapping = NewMappingRequest
    customer_modification = CustomerModificationRequest
    submission_modification = SubmissionModificationRequest
    branch_modification = BranchModificationRequest
    new_manufacturer = NewManufacturerRequest
    new_representative = NewRepresentativeRequest
    rep_modification = RepresentativeModificationRequest
    mapping_modification = MappingModificdationRequest
