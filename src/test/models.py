from pydantic import BaseModel, Field
from typing import Optional, Any
from datetime import datetime


class BookModel(BaseModel):
    name: str = Field(..., description="The name of book")
    author: str = Field(..., description="The author of book")
    pages: int = Field(..., description="The count of pages of book")
    published_date: datetime = Field(..., description="The published date of book")
    weight: int = Field(..., description='the weight of book, and the unit is g')
    description: Optional[str] = Field(description="The extra information of book")

    class Config:
        schema_extra = {
            "example": {
                "name": "Happy Place",
                "author": "Emily Henry",
                "pages": 400,
                "published_date": "2023-04-25",
                "weight": "400",
                "description": "it is a book",
            }
        }


class UpdateBookModel(BaseModel):
    author: Optional[str]
    pages: Optional[int]
    published_date: Optional[datetime]
    weight: Optional[int]
    description: Optional[str]

    class Config:
        schema_extra = {
            "example": {
                "name": "Happy Place",
                "author": "Emily Henry",
                "pages": 400,
                "published_date": "2023-04-25T00:00:00Z",
                "weight": "400",
                "description": "nothing"
            }
        }


class ResponseModel(BaseModel):

    status: str = Field(..., description="can be 'ok' or 'bad'")
    message: str = Field(..., description="hint message")
    data: Any = Field(..., description="the data returned by endpoints")

    class Config:
        scheme_extra = {
            "example": {
                "status": "ok",
                "message": "it's ok",
                "data": [{"name": "ammy", "test": True},
                         {"name": 'Salad', 'test': False}]
            }
        }


class S3ResponseModel(ResponseModel):

    class Config:
        scheme_extra = {
            "example": {
                "status": "ok",
                "data": [{'name': 'file1', 'size': 123},
                         {'name': 'file2', 'size': 99903}]
            }
        }


def ErrorResponseModel(error, code, message):
    return {"error": error, "code": code, "message": message}