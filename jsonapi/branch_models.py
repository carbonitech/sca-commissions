from pydantic import BaseModel
from jsonapi.base_models import *
from datetime import datetime
from typing import Optional

class BranchAttributes(BaseModel):
    deleted: datetime|None
    user_id: int
class BranchRelationship(BaseModel):
    representative: JSONAPIRelationshipObject
    locations: JSONAPIRelationshipObject

class BranchRelationshipFull(BaseModel):
    customers: JSONAPIRelationshipObject
    locations: JSONAPIRelationshipObject
    representative: JSONAPIRelationshipObject
    commission_data: JSONAPIRelationshipObject
    id_strings: JSONAPIRelationshipObject
    
class Branch(BaseModel):
    id: int
    type: str
    attributes: BranchAttributes
    relationships: BranchRelationshipFull

class BranchResponse(JSONAPIBaseObject):
    data: list[Branch] | Branch
    included: Optional[list]