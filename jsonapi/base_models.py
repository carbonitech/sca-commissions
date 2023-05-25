from pydantic import BaseModel
from typing import Optional

class JSONAPIBaseModification(BaseModel):
    type: str

class JSONAPIVersion(BaseModel):
    version: str

class JSONAPIBaseRelationshipData(BaseModel):
    type: str
    id: int

class JSONAPIBaseRelationshipLinks(BaseModel):
    self: str
    related: str

class JSONAPIRelationshipObject(BaseModel):
    links: JSONAPIBaseRelationshipLinks
    data: Optional[JSONAPIBaseRelationshipData]

class PaginationMetadata(BaseModel):
    sqlalchemy_jsonapi_version: Optional[str]
    totalPages: Optional[int]
    currentPage: Optional[int]

class JSONAPIBaseObject(BaseModel):
    jsonapi: JSONAPIVersion
    meta: PaginationMetadata
    # data: ... extended by endpoint-specific classes