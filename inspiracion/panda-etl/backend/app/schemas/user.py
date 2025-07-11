from pydantic import BaseModel, EmailStr, Field
from typing import Optional


# Base User Schemas
class UserBase(BaseModel):
    email: EmailStr
    is_active: bool = True
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    # Add other common user fields if needed, e.g., is_superuser: bool = False


class UserCreate(UserBase):
    password: str


class UserUpdate(UserBase):
    email: Optional[EmailStr] = None # Allow email update
    is_active: Optional[bool] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    password: Optional[str] = None # Allow password update


# Schema for returning a user (e.g., from DB)
class User(UserBase):
    id: int

    class Config:
        from_attributes = True


# Schema for user in database (includes hashed_password)
class UserInDBBase(User):
    hashed_password: str


# Token Schemas
class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    email: Optional[EmailStr] = None
    # Depending on JWT content, could be user_id: Optional[int] = None


# Specific Request/Response Schemas (existing ones)
class APIKeyRequest(BaseModel):
    email: EmailStr

    class Config:
        from_attributes = True


class UpdateAPIKeyRequest(BaseModel):
    api_key: str

    class Config:
        from_attributes = True


class UserUpdateRequest(BaseModel):
    email: EmailStr = Field(..., title="Email", description="User's email address")
    first_name: Optional[str] = Field(None, title="First Name", description="User's first name")
    last_name: Optional[str] = Field(None, title="Last Name", description="User's last name")

    class Config:
        from_attributes = True