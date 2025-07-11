from typing import List, Dict, Any
from pydantic import BaseModel, Field

class ConnectionSchema(BaseModel):
    name: str
    type: str
    host: str
    db_name: str
    port: str
    user: str
    password: str

    class Config:
        from_attributes = True

class FieldSchema(BaseModel):
    name: str
    data_type: str
    length: int = -1
    precision: int = -1

    class Config:
        from_attributes = True

class StepSchema(BaseModel):
    name: str
    type: str
    config: Dict[str, Any] = Field(default_factory=dict)
    fields: List[FieldSchema] = Field(default_factory=list)
    gui_location: Dict[str, int] = Field(default_factory=lambda: {"x": 0, "y": 0})
    sql: str = ""
    target_schema: str = ""
    target_table: str = ""
    connection_name: str = ""

    class Config:
        from_attributes = True

class HopSchema(BaseModel):
    from_step: str
    to_step: str
    enabled: bool = True

    class Config:
        from_attributes = True

class TransformationModelSchema(BaseModel):
    name: str = ""
    description: str = ""
    directory: str = "/"
    connections: List[ConnectionSchema] = Field(default_factory=list)
    steps: List[StepSchema] = Field(default_factory=list)
    hops: List[HopSchema] = Field(default_factory=list)
    parameters: Dict[str, str] = Field(default_factory=dict)
    attributes: Dict[str, Any] = Field(default_factory=dict)

    class Config:
        from_attributes = True
