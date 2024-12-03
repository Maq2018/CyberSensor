import logging
import traceback
from pydantic import BaseModel, Field
from datetime import datetime
from pymongo import InsertOne, UpdateOne
from fastapi import Query
from typing import Optional, Literal
import json
import re
from extensions import mongo
from utils.request import PageQuery, TimeRangeQuery
from utils.misc import (
    subnet_range,
    str_to_int_v4,
    strip_list,
    timestring_to_timestamp
)


logger = logging.getLogger('asn.models')


class VisEduASHistory(BaseModel):
    date: int
    location: str
    asn: int
    bandwidth: float
    op: str
    name: str
    abroad: int

    @classmethod
    def to_item(cls, line):
        keys = ['date', 'location', 'asn', 'bandwidth', 'op', 'name']
        return dict(zip(keys, line.strip('\n').split(',')))

    @classmethod
    def to_op(cls, op):
        if op == '新增':
            return 'add'
        elif op == '撤销':
            return 'rm'
        else:
            return op

    @classmethod
    def to_date(cls, date):
        dt = datetime.strptime(date, '%d/%m/%Y')
        return int(dt.strftime('%Y%m%d'))

    @classmethod
    def to_asn(cls, asn):
        return int(asn)

    @classmethod
    def to_bandwidth(cls, bandwidth):
        return float(bandwidth)

    @classmethod
    def to_obj(cls, item, abroad):

        obj = {
            'date': cls.to_date(item['date']),
            'location': item['location'],
            'asn': cls.to_asn(item['asn']),
            'bandwidth': cls.to_bandwidth(item['bandwidth']),
            'op': cls.to_op(item['op']),
            'name': item['name'],
            'abroad': int(abroad)
        }
        return obj

    @classmethod
    def to_mongo(cls, item, abroad):
        obj = cls.to_obj(item, abroad)
        return InsertOne(obj)


class VisEduASCityLocation(BaseModel):

    city: str
    lng: float
    lat: float

    @classmethod
    def to_item(cls, line):
        keys = ['city', 'lng', 'lat']
        return dict(zip(keys, line.strip('\n').split(',')))

    @classmethod
    def to_loc(cls, loc):
        return float(loc)

    @classmethod
    def to_obj(cls, item):
        obj = {
            'city': item['city'],
            'lng': cls.to_loc(item['lng']),
            'lat': cls.to_loc(item['lat'])
        }
        return obj

    @classmethod
    def to_mongo(cls, item):
        obj = cls.to_obj(item)
        _q = {'city': obj.pop('city')}
        _u = {'$set': obj}

        return UpdateOne(_q, _u, upsert=True)

    @classmethod
    def to_op(cls, line):
        item = cls.to_item(line)
        return cls.to_mongo(item)


class VisAS(BaseModel):
    """
    {
    "asn": "3356",
    "asnName": "LEVEL3",
    "rank": 1,
    "organization": {
        "orgId": "589f9199b0",
        "orgName": "Level 3 Parent, LLC"
    },
    "cliqueMember": true,
    "seen": true,
    "longitude": -91.6647856118214,
    "latitude": 36.0166304061453,
    "cone": {
        "numberAsns": 49212,
        "numberPrefixes": 774077,
        "numberAddresses": 2133503614
    },
    "country": {
        "iso": "US",
        "name": "United States"
    },
    "asnDegree": {
        "provider": 0,
        "peer": 71,
        "customer": 6415,
        "total": 6486,
        "transit": 6486,
        "sibling": 10
    },
    "announcing": {
        "numberPrefixes": 1782,
        "numberAddresses": 29575117
    }
}
    """
    asn: int
    asn_name: str
    rank: int
    org_name: str
    lng: float
    lat: float
    country: Optional[str]  # cn

    @classmethod
    def to_item(cls, line):
        return json.loads(line.strip('\n'))

    @classmethod
    def to_asn(cls, asn):
        return int(asn)

    @classmethod
    def to_cc(cls, country):
        # todo change name of ases in cn
        if country and country.get("iso"):
            return country['iso']
        return None

    @classmethod
    def to_org_name(cls, org):
        if org and org.get("orgName"):
            return org['orgName']
        return 'None'

    @classmethod
    def to_obj(cls, item):
        obj = {
            'asn': cls.to_asn(item['asn']),
            'asn_name': item['asnName'],
            'rank': item['rank'],
            'org_name': cls.to_org_name(item['organization']),
            'lng': item['longitude'],
            'lat': item['latitude'],
            'country': cls.to_cc(item['country'])
        }
        return obj

    @classmethod
    def to_mongo(cls, item):
        obj = cls.to_obj(item)
        _q = {'asn': obj.pop('asn')}
        _u = {'$set': obj}

        return UpdateOne(_q, _u, upsert=True)

    @classmethod
    def to_op(cls, line):
        item = cls.to_item(line)
        return cls.to_mongo(item)


class VisHijackEvent(BaseModel):
    timestamp: int
    index: str
    prefix: str
    attacker: int
    victim: int
    normal_paths: list[int]
    abnormal_paths: list[int]
    hops: list[list[int]]
    affected_count: int

    """
    {
    "timestamp": 1688028558,
    "index": "11304251",
    "prefix": "66.35.15.0/24",
    "type": 0,
    "start_time": "2023-06-29 16:49:18",
    "victim_prefix": "66.35.15.0/24",
    "attacker": "1052",
    "victim": "40033",
    "normal_paths": [
        "263237 6939 20055 40033",
        "134823 206264 1299 20055 40033",
        "49544 2914 20055 40033"
    ],
    "abnormal_paths": [
        "134823 38047 7473 6461 20055 1052",
        "49544 3356 6461 20055 1052",
        "263237 16397 15830 6762 6461 20055 1052",
        "134823 17408 15412 6461 20055 1052"
    ],
    "1_hop": [
        "20055"
    ],
    "2_hop": []
    }
    
    """

    @classmethod
    def to_asn(cls, asn):
        return int(asn)

    @classmethod
    def strip_path(cls, path: str) -> list[int]:
        if not path:
            return []

        ases = re.split(' +', path)
        _a = None
        _path = []

        for as_ in ases:
            if _a is None:
                _path.append(cls.to_asn(as_))
                continue

            if as_ == _a:
                continue

            _path.append(cls.to_asn(as_))
            _a = as_

        return _path

    @classmethod
    def to_obj(cls, item, cc_map):
        """
        :return:
        timestamp: int
        index: str
        prefix: str
        attacker: int
        victim: int
        normal_paths: list[int]
        abnormal_paths: list[int]
        hops: list[list[int]]
        affected_count: int
        """
        obj = {
            'timestamp': item['timestamp'],
            'index': item['index'],
            'prefix': item['prefix'],
            'attacker': cls.to_asn(item['attacker']),
            'victim': cls.to_asn(item['victim']),
            'normal_paths': [cls.strip_path(_p) for _p in item['normal_paths']],
            'abnormal_paths': [cls.strip_path(_p) for _p in item['abnormal_paths']],
            'attacker_country': cc_map.get(cls.to_asn(item['attacker'])),
            'victim_country': cc_map.get(cls.to_asn(item['victim']))
        }

        affected = set()
        affected.add(obj['victim'])

        hops = []
        for i in range(1, 21):
            _key = f'{i}_hop'
            if _key not in item:
                continue

            _hop = []
            _last_as = None
            for _as in item[_key]:
                affected.add(cls.to_asn(_as))

                if _as == _last_as:
                    continue

                _hop.append(cls.to_asn(_as))
                _last_as = _as

            if _hop:
                hops.append(_hop)

        affected -= {obj['attacker']}

        obj['hops'] = hops
        obj['affected_count'] = len(affected)
        return obj

    @classmethod
    def to_mongo(cls, item, cc_map):
        obj = cls.to_obj(item, cc_map)
        _q = {'index': obj.pop('index')}
        _u = {'$set': obj}

        return UpdateOne(_q, _u, upsert=True)


class EduHistoryQuery(BaseModel):
    # date, abroad
    date: int = Field(Query())
    abroad: int = Field(Query(default=0))


class ASQuery(BaseModel):
    asns: str = Field(Query(default=''))


class EduTrendQuery(BaseModel):
    abroad: int = Field(Query(default=0))


class ASHijackDetailQuery(BaseModel):
    index: str = Field(Query(default=''))


class ASHijackQuery(PageQuery):
    start: int = Field(Query())
    end: int = Field(Query())
    search: str = Field(Query(default=''))


class ASHijackSummaryQuery(TimeRangeQuery):
    data_type: Literal['attacker', 'victim', 'attacker_region', 'victim_region'] = Field(Query())


class ASPathSearchQuery(BaseModel):
    prefix: str = Field(Query())


class ASTrendsQuery(BaseModel):
    countries: Optional[str] = Field(Query(default=None))


class VisEduASPath(BaseModel):
    prefix: str
    prefix_start: int
    prefix_end: int
    count: int
    path: list[int]
    origin: Literal['i', '?', 'e']

    @classmethod
    def to_item(cls, line):
        p = re.split(' +', line.strip('\n '))
        if not p:
            return None

        if p[-1] not in {'i', 'e', '?'}:
            logger.warning(f'line without valid origin found line={line}')
            return None

        if len(p) < 4:
            logger.warning(f'line is not valid without enough hops, line={line}')
            return None

        _p = []
        for v in p[2:-1]:
            v = v.strip('{}')
            if ',' in v:
                _vs = v.split(',')
                _p += _vs
            else:
                _p.append(v)

        _p = strip_list(_p)
        route = [int(_e) for _e in _p]

        if not route:
            logger.warning(f'path not found, line={line}')
            return None

        prefix = p[1]

        if '/' in prefix:
            prefix_start, prefix_end = subnet_range(prefix)
            if None in [prefix_start, prefix_end]:
                logger.warning(f'invalid prefix, line={line}')
                return None

            count = 2 ** (32 - int(prefix.split('/', 1)[-1]))

        else:
            prefix_start, prefix_end = str_to_int_v4(prefix), str_to_int_v4(prefix)
            count = 1
        """
         prefix: str
    prefix_start: int
    prefix_end: int
    count: int
    path: list[int]
    origin: Literal['i', '?', 'e']
        """
        item = {
            'row': int(p[0]),
            'prefix': prefix,
            'prefix_start': prefix_start,
            'prefix_end': prefix_end,
            'count': count,
            'path': route,
            'origin': p[-1]
        }

        return item

    @classmethod
    def to_obj(cls, item):
        return item

    @classmethod
    def to_mongo(cls, item):
        if not item:
            return None

        obj = cls.to_obj(item)
        _q = {'prefix': obj.pop('prefix')}
        _u = {'$set': obj}

        return UpdateOne(_q, _u, upsert=True)

    @classmethod
    def to_op(cls, line):
        try:
            item = cls.to_item(line)
            return cls.to_mongo(item)

        except Exception as e:
            logger.error(f'failed to_op from line={line}, err={e}, stack={traceback.format_exc()}')
            return None


class VisASHijackSimpleEvent(VisHijackEvent):
    timestamp: int
    index: str
    prefix: str
    attacker: int
    victim: int
    affected_count: int
    attacker_country: str
    victim_country: str

    @classmethod
    def to_item(cls, line):
        return json.loads(line.strip(' \n'))

    @classmethod
    def to_obj(cls, item, cc_map):
        obj = {
            'timestamp': timestring_to_timestamp(item['start_time']),
            'index': item['index'],
            'prefix': item['prefix'],
            'attacker': cls.to_asn(item['attacker']),
            'victim': cls.to_asn(item['victim']),
            'attacker_country': cc_map.get(cls.to_asn(item['attacker'])),
            'victim_country': cc_map.get(cls.to_asn(item['victim']))
        }
        return obj

    @classmethod
    def to_mongo(cls, item, cc_map):
        obj = cls.to_obj(item, cc_map)
        _q = {'index': obj.pop('index')}
        _u = {'$set': obj}

        return UpdateOne(_q, _u, upsert=True)

    @classmethod
    def to_op(cls, line, cc_map):
        item = cls.to_item(line)
        return cls.to_mongo(item, cc_map)
