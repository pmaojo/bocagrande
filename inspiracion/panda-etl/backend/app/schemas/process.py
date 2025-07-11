# Updated Pydantic V2 configuration in ProcessFromKTRData model.
from pydantic import BaseModel, Field
from typing import Any, Dict, Optional, Union # Added List and Union
from datetime import datetime # Added datetime
from app.models.process import ProcessStatus # Added ProcessStatus


class ProcessBase(BaseModel):
    name: str
    type: str

    project_id: int
    details: Optional[Dict[str, Any]] = None
    output: Optional[Dict[str, Any]] = None
    message: Optional[str] = None


class ProcessCreate(ProcessBase):
    pass


class ProcessUpdate(BaseModel):
    name: Optional[str] = None
    type: Optional[str] = None
    status: Optional[ProcessStatus] = None
    details: Optional[Dict[str, Any]] = None
    output: Optional[Dict[str, Any]] = None
    message: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class ProcessSchema(ProcessBase):
    id: int
    status: ProcessStatus
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
        use_enum_values = True


class ProcessData(BaseModel):
    name: str
    type: str
    data: Dict[str, Any]  # For type-specific parameters like script_code
    project_id: Union[str, int]
    target_asset_id: Optional[int] = None # For processes that target a single asset




class ProcessFromKTRData(BaseModel):
    project_id: int
    name: Optional[str] = None
    ktr_model: Dict[str, Any] = Field(..., alias="ktrModel")

    class Config:
        from_attributes = True


class ProcessSuggestion(BaseModel):
    name: str
    type: str    
    project_id: int
    output_type: str
