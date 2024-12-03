import math
import logging
import traceback
from fastapi import Query
from typing import Optional
from pydantic import Field, BaseModel
from pymongo import UpdateOne
from extensions import mongo
from utils.request import (
    IPQueryWithTime,
    IPBaseQuery,
    PageQuery
)
from utils.misc import (
    str_to_int_v4,
    str_to_exploded_ipv6,
    subnet_range,
    Regions
)


logger = logging.getLogger('ip.models')


class IPPrefixInfoQuery(IPQueryWithTime):
    # todo add validator for prefix
    # v, date, prefix
    prefix: Optional[str] = Field(Query(default=None))


class IPTrendsQuery(IPBaseQuery):
    # v, countries
    countries: Optional[str] = Field(Query(default=None))


class IPSpaceQuery(IPQueryWithTime):
    # v, date, countries
    countries: Optional[str] = Field(Query(default=None))
    tight: int = Field(Query(default=0))


class IPPrefixInfoCountryQuery(IPQueryWithTime, PageQuery):
    # v, date, country
    country: Optional[str] = Field(Query(default=None))


class VisIPv4Alloc(BaseModel):
    # vis_ipv4_alloc

    registry: str
    cc: str
    status: str
    date: int
    prefix_start: int
    prefix_end: int
    prefix: str
    count: int

    @classmethod
    def to_item(cls, line):
        # todo strip spaces besides the value
        keys = ['registry', 'cc', 'type', 'start', 'value', 'date', 'status']
        return dict(zip(keys, line.strip('\n').split('|')))

    @classmethod
    def to_cc(cls, cc):
        if Regions.is_focused_region(cc):
            return Regions.CN

        return cc

    @classmethod
    def is_need(cls, item):
        if item['status'] in {'available', 'reserved'}:
            return False

        if item['cc'] == 'EU':
            return False

        return True

    @classmethod
    def to_obj(cls, item):
        try:
            if not cls.is_need(item):
                return None

            obj = {
                'registry': item['registry'],
                'cc': cls.to_cc(item['cc']),
                'status': cls.to_status(item['status']),
                'date': cls.to_date(item['date']),
                'prefix_start': str_to_int_v4(item['start']),
                'prefix_end': str_to_int_v4(item['start']) + int(item['value']) - 1,
                'prefix': f"{item['start']}/{32 - int(math.log(int(item['value']), 2))}",
                'count': int(item['value'])
            }

            return obj

        except Exception as e:
            logger.error(f'failed to generate obj for ipv4 with {item},'
                         f' err: {e}, stack: {traceback.format_exc()}')
            return None

    @classmethod
    def to_status(cls, status):
        return status

    @classmethod
    def to_date(cls, date):
        try:
            return int(date)
        except:
            return 0

    @classmethod
    def to_mongo(cls, item):
        obj = cls.to_obj(item)
        if not obj:
            return None

        _q = {'prefix': obj.pop('prefix')}
        _u = {'$set': obj}

        return UpdateOne(_q, _u, upsert=True)


class VisIPv6Alloc(VisIPv4Alloc):
    # vis_ipv6_alloc

    registry: str
    cc: str
    status: str
    date: int
    prefix_start: str
    prefix_end: str
    prefix: str
    count: int

    @classmethod
    def to_obj(cls, item):
        try:
            if not cls.is_need(item):
                return None

            prefix_start, prefix_end = subnet_range(f"{item['start']}/{item['value']}", v=6)

            cidr = int(item['value'])
            _low_count = None

            if cidr > 64:
                _low_count = 128 - cidr
                logger.warning(f"got unexpected item for 128-bit ipv6 address with cidr={cidr}")
                cidr = 64

            obj = {
                'registry': item['registry'],
                'cc': cls.to_cc(item['cc']),
                'status': cls.to_status(item['status']),
                'date': cls.to_date(item['date']),
                'prefix_start': prefix_start,
                'prefix_end': prefix_end,
                'prefix': f"{item['start']}/{item['value']}",
                'count': 2 ** (64 - cidr)
            }

            if _low_count is not None:
                obj['_low_count'] = 2 ** 64

            return obj

        except Exception as e:
            logger.error(f'failed to generate obj for ipv6 with {item},'
                         f' err: {e}, stack: {traceback.format_exc()}')
            return None

    @classmethod
    def to_mongo(cls, item):
        obj = cls.to_obj(item)

        if not obj:
            return None

        _q = {'prefix': obj.pop('prefix')}
        _u = {'$set': obj}

        return UpdateOne(_q, _u, upsert=True)


'''
registriy: string: {afrinic,apnic,arin,iana,lacnic,ripencc};

cc: string: CN

status: int: 1 -> alloc | 2 -> assign

date: int: 19891919

prefix_start: int: int(IPv4Address(start))
prefix_end: int: nstart + value - 1

prefix: string: 1.0.1.0/24

count: value
'''




class VisASAlloc(VisIPv4Alloc):

    """
    registriy: string: {afrinic,apnic,arin,iana,lacnic,ripencc};

    cc: string: CN

    status: int: 1 -> alloc | 2 -> assign

    date: int: 19891919

    asn_start: int: int(start)
    asn_end: int: int(start) + value - 1
    count: value
    """

    registry: str
    cc: str
    status: str
    date: int
    asn_start: int
    asn_end: int
    count: int

    @classmethod
    def to_obj(cls, item):
        try:
            if not cls.is_need(item):
                return None

            obj = {
                'registry': item['registry'],
                'cc': cls.to_cc(item['cc']),
                'status': cls.to_status(item['status']),
                'date': cls.to_date(item['date']),
                'asn_start': int(item['start']),
                'asn_end': int(item['start']) + int(item['value']) - 1,
                'count': int(item['value'])
            }

            return obj

        except Exception as e:
            logger.error(f'failed to generate obj for asn with {item},'
                         f' err: {e}, stack: {traceback.format_exc()}')
            raise e

    @classmethod
    def to_mongo(cls, item):
        obj = cls.to_obj(item)

        if not obj:
            return None

        _q = {'asn_start': obj.pop('asn_start'), 'asn_end': obj.pop('asn_end')}
        _u = {'$set': obj}

        return UpdateOne(_q, _u, upsert=True)
