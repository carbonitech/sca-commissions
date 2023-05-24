from pydantic import BaseModel
from typing import Optional

class JSONAPIBaseModification(BaseModel):
    type: str

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
    totalPages: int
    currentPage: int

class JSONAPIBaseObject(BaseModel):
    jsonapi: str
    meta: PaginationMetadata
    # data: ... extended by endpoint-specific classes