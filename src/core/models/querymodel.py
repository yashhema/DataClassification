# Pydantic-style models for defining the schema

from enum import Enum
from typing import List, Literal, Union, Any, Annotated, Optional
from pydantic import BaseModel, Field

class FilterOperator(str, Enum):
    EQUAL = "equal"
    LESS_THAN = "less_than"
    GREATER_THAN = "greater_than"
    CONTAINS = "contains"
    STARTS_WITH = "starts_with"
    TEXT_SEARCH = "text_search"

class FilterCondition(BaseModel):
    field: str
    operator: FilterOperator
    value: Any

# This recursive model allows for complex AND/OR/NOT logic
class ComplexFilter(BaseModel):
    AND: List[Union["ComplexFilter", FilterCondition]] = Field(default_factory=list)
    OR:  List[Union["ComplexFilter", FilterCondition]] = Field(default_factory=list)
    NOT: List[Union["ComplexFilter", FilterCondition]] = Field(default_factory=list)





class ReferenceSelectionCriteria(BaseModel):
    type: Literal["reference"]
    # The ID of another PolicyTemplate whose selection criteria we want to reuse
    policy_template_id: str



# Pydantic-style models for defining the schema




class Pagination(BaseModel):
    offset: int = Field(0, description="The starting point of the result set.")
    limit: int = Field(1000, description="The number of items to return per page.")

class Sort(BaseModel):
    field: str
    order: Literal["asc", "desc"]

class QueryDefinition(BaseModel):
    source: Literal["metadata", "findings"]
    filters: Union[List[FilterCondition], ComplexFilter]
    # NEW: Added optional pagination and sorting
    pagination: Optional[Pagination] = None
    sort: Optional[List[Sort]] = None # List allows for multi-field sorting
    
class QuerySelectionCriteria(BaseModel):
    type: Literal["query"]
    definition: QueryDefinition
    
# The top-level object is a discriminated union
SelectionCriteria = Annotated[
    Union[QuerySelectionCriteria, ReferenceSelectionCriteria],
    Field(discriminator="type")
]