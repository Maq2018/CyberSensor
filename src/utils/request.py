from typing import Literal, Optional
from pydantic import StrictStr, BaseModel, Field
from fastapi import Query


IP_VER = Literal['4', '6']


class IPBaseQuery(BaseModel):
    # v
    v: Literal['4', '6'] = Field(Query(default='4'))


class IPQueryWithTime(IPBaseQuery):
    # v, date
    date: Optional[int] = Field(Query(default=0, gte=1))


class PageQuery(BaseModel):
    # page_size, page
    page_size: int = Field(Query(default=30))
    page: int = Field(Query(default=1))


class TimeRangeQuery(BaseModel):
    # start, end
    start: int = Field(Query())
    end: int = Field(Query())


class DateQuery(BaseModel):
    # date
    date: int = Field(Query(default=0))


class RefreshQuery(BaseModel):
    # refresh
    refresh: int = Field(Query(default=0))
