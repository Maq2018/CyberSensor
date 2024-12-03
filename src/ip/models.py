import json
import math
import logging
import traceback
from statistics import mean
from fastapi import Query
from typing import Optional, Union
from ipaddress import IPv6Address
from pydantic import Field, BaseModel
from pymongo import UpdateOne
from functools import partial
from netaddr import (
    IPNetwork,
    cidr_merge
)
from utils.request import (
    IPQueryWithTime,
    IPBaseQuery,
    PageQuery,
    RefreshQuery
)
from utils.misc import (
    str_to_int_v4,
    subnet_range,
    Regions,
    str_to_exploded_ipv6,
    get_b_subnets_v4,
    get_b_subnet_v4,
    get_b_subnet_v4_str,
    ip_to_str
)
from database.models import (
    TableSelector,
    CacheSelector
)
from database.services import _async_bulk_load, _bulk_load
from .services import _add_to_cc_map, _add_to_prefix_map, _get_diff_date
from decorators import time_cost
from config import Config


logger = logging.getLogger('ip.models')
TTL = 365 * 24 * 60 * 60  # 1 year


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
    refresh: int = Field(Query(default=0))
    force_cache: int = Field(Query(default=0))


class IPPrefixInfoCountryQuery(IPQueryWithTime, PageQuery):
    # v, date, country
    country: Optional[str] = Field(Query(default=None))


class IPNetflowQuery(BaseModel):
    ip: str = Field(Query(default=''))


class ProbePictureQuery(IPBaseQuery):
    ip: str = Field(Query(default=''))


class ProbeMapQuery(IPBaseQuery, RefreshQuery):
    country: str = Field(Query(default=''))


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
            if '240e:2000::' in item['start']:
                pass

            if not cls.is_need(item):
                return None

            cidr = 32 - int(math.log(int(item['value']), 2))
            obj = {
                'registry': item['registry'],
                'cc': cls.to_cc(item['cc']),
                'status': cls.to_status(item['status']),
                'date': cls.to_date(item['date']),
                'prefix_start': str_to_int_v4(item['start']),
                'prefix_end': str_to_int_v4(item['start']) + int(item['value']) - 1,
                'prefix': f"{item['start']}/{cidr}",
                'count': int(item['value']),
                'cidr': cidr,
                'prefix_b': get_b_subnet_v4_str(item['start']) if cidr >= 16 else f"{item['start']}/{cidr}"
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


def _get_tight_prefix(_prefix, cidr, _mask):
    _i = IPv6Address(int(IPv6Address(_prefix)) & int(IPv6Address(_mask)))
    return f'{_i}/{cidr}'


_tight_prefix_fn = partial(_get_tight_prefix, _mask='FFFF:FF00::')
_straight_prefix_fn = partial(_get_tight_prefix, _mask='FFFF:F000::')


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
                'count': 2 ** (64 - cidr),
                'prefix_tight': _tight_prefix_fn(item['start'], 24) if cidr >= 24 else f"{item['start']}/{item['value']}",
                'prefix_straight': _straight_prefix_fn(item['start'], 20) if cidr >= 20 else f"{item['start']}/{item['value']}",
                'cidr': cidr
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


VIS_IP_NETFLOW_KEYS = ['timestamp',
                       'src_ip_packet_rate',
                       'src_ip_bandwidth',
                       'dst_ip_packet_rate',
                       'dst_ip_bandwidth',
                       'src_ip_dst_ip_packet_rate',
                       'src_ip_dst_ip_bandwidth',
                       'src_ip_src_port_packet_rate',
                       'src_ip_src_port_bandwidth',
                       'dst_ip_dst_port_packet_rate',
                       'dst_ip_dst_port_bandwidth',
                       'status']


class VisIPNetflow(BaseModel):
    timestamp: int
    ip: str
    src_ip_packet_rate: float
    src_ip_bandwidth: float
    dst_ip_packet_rate: float
    dst_ip_bandwidth: float
    src_ip_dst_ip_packet_rate: float
    src_ip_dst_ip_bandwidth: float
    src_ip_src_port_packet_rate: float
    src_ip_src_port_bandwidth: float
    dst_ip_dst_port_packet_rate: float
    dst_ip_dst_port_bandwidth: float
    status: int

    @classmethod
    def to_status(cls, status):
        if status == 'Normal':
            return 0
        return 1

    @classmethod
    def to_item(cls, line):
        p = line.strip(' \n').split(',')
        return dict(zip(VIS_IP_NETFLOW_KEYS, p))

    @classmethod
    def to_ops(cls, ip, items):
        t_map = {}
        ops = []

        for item in items:
            t = item['timestamp']
            if t not in t_map:
                t_map[t] = []

            t_map[t].append(item)

        objs = []
        for t, items in t_map.items():
            status = 0
            obj = {}

            for item in items:
                if item['status'] == 'Anomaly':
                    status = 1
                    break

            for key in VIS_IP_NETFLOW_KEYS[1:-1]:
                _v = mean([float(item[key]) for item in items])
                obj[key] = _v

            obj['timestamp'] = int(float(t))
            obj['status'] = status
            objs.append(obj)

        n = len(objs)
        for obj in objs:
            obj['n'] = n
            ops.append(UpdateOne({'ip': ip, 'timestamp': obj.pop('timestamp')}, {'$set': obj}, upsert=True))

        return ops


class VisIPMap(BaseModel):
    date: int
    v: str
    data: list

    @classmethod
    async def get_latest_map(cls, v: str, date: int):
        _q = {'v': v, 'date': {'$lte': date}}
        _table = TableSelector.get_ip_map_table()
        _cur = None
        async for cur in _table.find(_q, {'_id': 0}).sort('date', -1).limit(1):
            _cur = cur

        return _cur

    @classmethod
    async def insert_map(cls, v: str, date: int, data: list):
        _table = TableSelector.get_ip_map_table()
        await _table.update_one({'v': v, 'date': date},
                                {'$set': {'data': data}}, upsert=True)

    @classmethod
    async def addup_to_date(cls, v: str, start_date: int, end_date: int, data: list):
        _table = TableSelector.get_prefix_alloc_table(v)

        pipe = [
            {'$match': {'date': {'$gt': start_date, '$lte': end_date}}},
            {'$group': {'_id': '$cc',
                        'ips': {'$sum': '$count'},
                        'prefixes': {'$sum': 1}}}
        ]

        logger.debug(f"add_to_date pip={pipe}")

        cc_map = {}
        async for cur in _table.aggregate(pipe):
            if cur['_id'] not in cc_map:
                item = {
                    'country': cur['_id'],
                    'ips': cur['ips'],
                    'prefixes': cur['prefixes']
                }
                cc_map[cur['_id']] = item

        for cur in data:
            if cur['country'] not in cc_map:
                cc_map[cur['country']] = cur

            else:
                cc_map[cur['country']]['ips'] += cur['ips']
                cc_map[cur['country']]['prefixes'] += cur['prefixes']

        cc = sorted(list(cc_map.values()), key=lambda x: x['ips'], reverse=True)
        if cc:
            await cls.insert_map(v, end_date, cc)

        return cc


class VisIPv4Picture(BaseModel):
    ip: str
    port_services: list
    cc: str
    carrier: str
    lat: float
    lng: float

    @classmethod
    def to_item(cls, line):
        keys = ['ip', 'port_services', 'cc', 'carrier', 'lat', 'lng']
        _p = line.strip(' \n').split(',')
        item = {
            'ip': _p[0],
            'port_services': ','.join(_p[1:-4]),
            'cc': _p[-4],
            'carrier': _p[-3],
            'lat': _p[-2],
            'lng': _p[-1],
        }
        return item

    @classmethod
    def to_obj(cls, item):
        obj = {
            'port_services': eval(eval(item['port_services'])) if item['port_services'] else [],
            'ip': str_to_int_v4(item['ip']),
            'cc': item['cc'],
            'carrier': item['carrier'],
            'lat': float(item['lat']) if item['lat'] != 'Unknown' else float(0),
            'lng': float(item['lng']) if item['lng'] != 'Unknown' else float(0)
        }

        return obj

    @classmethod
    def to_mongo(cls, obj):
        _q = {'ip': obj.pop('ip')}
        _u = {'$set': obj}
        return UpdateOne(_q, _u, upsert=True)

    @classmethod
    def to_op(cls, line):
        item = cls.to_item(line)
        obj = cls.to_obj(item)
        return cls.to_mongo(obj)


class VisIPv6Picture(BaseModel):
    ip: str
    asn: str
    prefix: str
    cc: str
    lng: float
    lat: float

    @classmethod
    def to_item(cls, line):
        keys = ['ip', 'asn', 'prefix', 'cc', 'lng', 'lat']
        _p = line.strip(' \n').split(',')
        return dict(zip(keys, _p))

    @classmethod
    def to_obj(cls, item):
        obj = {
            'ip': str_to_exploded_ipv6(item['ip']),
            'asn': item['asn'],
            'prefix': item['prefix'],
            'cc': item['cc'],
            'lat': float(item['lat']) if item['lat'] != 'Unknown' else float(0),
            'lng': float(item['lng']) if item['lng'] != 'Unknown' else float(0)
        }
        return obj

    @classmethod
    def to_mongo(cls, item):
        q = {'ip': item.pop('ip')}
        u = {'$set': item}

        return UpdateOne(q, u, upsert=True)

    @classmethod
    def to_op(cls, line):
        item = cls.to_item(line)
        obj = cls.to_obj(item)
        return cls.to_mongo(obj)


ipv4_time_range = [19810101, 20240101]
ipv6_time_range = [19980101, 20240101]

IPv4_RANGE = _get_diff_date(ipv4_time_range[0], ipv4_time_range[1]) + [Config.IPv4_ALLOC_END]
IPv6_RANGE = _get_diff_date(ipv6_time_range[0], ipv6_time_range[1]) + [Config.IPv6_ALLOC_END]
ORDERED_IPv4_RANGE = list(reversed(IPv4_RANGE))
ORDERED_IPv6_RANGE = list(reversed(IPv6_RANGE))


class VisIPSpace(BaseModel):
    cc: str
    date: int
    data: dict

    @classmethod
    def get_latest_date(cls, v: str, date: int) -> int:
        _range = ORDERED_IPv4_RANGE if v == '4' else ORDERED_IPv6_RANGE

        _date = date
        for _d in _range:
            if date >= _d:
                _date = _d
                break

        logger.debug(f'got latest day={_date} for={date}')
        return _date

    @classmethod
    def cache_key(cls, v: str, date: int, is_tight=0):
        if v == '6':
            return f'{v}-{date}-{is_tight}'
        return f'{v}-{date}'

    @classmethod
    async def get_space(cls, v: str, date: int, ccs: list, is_tight: int = 0) -> (dict, set):
        if not ccs:
            return {}, set()

        _spaces = {}
        ccs = set(ccs)

        cache = CacheSelector.get_cache()
        """
        {
            data: {
                cn: {prefix: count},
                us: {prefix: count}
            }
        }
        """
        _cache_key = cls.cache_key(v, date, is_tight)
        found = await cache.get(_cache_key)

        if found is not None:
            logger.debug(f'got cache with key={_cache_key}')

        else:
            _q = {'date': date}

            if v == '6':
                _q['tight'] = is_tight

            _table = TableSelector.get_ip_space(v)
            logger.debug(f'get_space_q={_q}, v={v}')

            one = await _table.find_one(_q)
            if not one:
                return {}, ccs

            found = one

        for k, v in found['data'].items():
            if k not in ccs:
                continue

            _spaces[k] = v

        _left_cc = ccs - set(_spaces.keys())
        return _spaces, _left_cc

    @classmethod
    async def get_latest_space(cls, v: str, date: int, ccs: list):
        if not ccs:
            return {}

        _q = {'date': {'$lte': date}}
        _table = TableSelector.get_ip_space(v)
        # {cc: {prefix: count}}

        _spaces = {}
        _date = 0
        _left = {}

        ccs = set(ccs)
        logger.debug(f'get_latest_q={_q}')

        async for cur in _table.find(_q, {'_id': 0}).sort('date', -1).limit(1):
            # logger.debug(f'cur={cur}')
            for cc, prefixes in cur['data'].items():

                if cc not in ccs:
                    _left[cc] = prefixes
                    continue

                _spaces[cc] = prefixes

            _date = cur['date']

        _left_cc = ccs - set(_spaces.keys())

        return _spaces, _left, _date, _left_cc

    @classmethod
    async def get_latest_space2(cls, v: str, date: int,
                                ccs: list, is_tight: int = 0) \
            -> (bool, dict, int, set, set):

        ccs = set(ccs)

        _found = False
        _found_date = 0
        _found_cc = set()
        _spaces = {}

        if not ccs:
            return _found, _spaces, _found_date, ccs, _found_cc

        cache = CacheSelector.get_cache()
        """
        {
            data: {
                cn: {prefix: count},
                us: {prefix: count}
            },
            cc: [cn, us, de]
        }
        """
        _latest_date = cls.get_latest_date(v, date)
        _cache_key = cls.cache_key(v, _latest_date, is_tight)
        found = await cache.get(_cache_key)

        if found is not None:
            logger.debug(f'got cache with key={_cache_key}')
            _found = True
            _found_date = _latest_date

        else:
            _q = {'date': {'$lte': _latest_date}}
            if v == '6':
                _q['tight'] = is_tight

            _table = TableSelector.get_ip_space(v)
            logger.debug(f'get_latest_space_q={_q}, v={v}')

            async for cur in _table.find(_q, {'_id': 0}).sort('date', -1).limit(1):
                found = cur

            if not found:
                return _found, _spaces, _found_date, ccs, _found_cc

            _found = True
            _found_date = found['date']

        for k, v in found['data'].items():
            if k not in ccs:
                continue
            _spaces[k] = v

        _found_cc = found['cc']
        _left_cc = ccs - set(_found_cc)

        return _found, _spaces, _found_date, _left_cc, _found_cc

    @classmethod
    async def insert_space(cls, v: str, date: int, cc_map: dict,
                           left: dict, tight: int = 0, cc: list = None):
        logger.debug(f'insert space date={date}, map={len(cc_map)}, '
                     f'left={len(left)}, tight={tight}, v={v}')

        if not cc_map:
            return

        if left:
            cc_map.update(left)

        # set in db
        _q = {'date': date}
        if v == '6':
            _q['tight'] = tight

        _u = {'data': cc_map, 'cc': cc}

        # set in mem
        cache = CacheSelector.get_cache()
        _cache_key = cls.cache_key(v, date, tight)

        logger.debug(f"set cache with key={_cache_key},"
                     f" value={len(_u['data'])}")
        # todo remove TTL
        ok = await cache.set(key=_cache_key,
                             value=_u,
                             expire=None)
        if not ok:
            logger.warning(f'failed to set cache with key={_cache_key}')

        _table = TableSelector.get_ip_space(v)
        await _table.update_one(_q,
                                {'$set': _u},
                                upsert=True)

    @classmethod
    async def addup_to_date(cls,
                            v,
                            start_date: int,
                            end_date: int,
                            data: dict,
                            ccs: list,
                            left: dict,
                            cidr: int = 16):
        if not data:
            return {}

        cc_map = {}  # {cc: {prefix: count}}
        step = 2 ** cidr

        ccs = set(ccs) - set(data.keys())

        def _add_item(cur_):

            if cur_['count'] <= step:
                b_ = get_b_subnet_v4(cur_['prefix_start'])
                _add_to_cc_map(cc_map, f'{ip_to_str(b_, v=v)}/{cidr}',
                               cur_['cc'], cur_['count'])

            else:
                _add_to_cc_map(cc_map, cur_['prefix_start'], cur_['cc'], cur_['count'])

        _table = TableSelector.get_prefix_alloc_table(v)

        if start_date < end_date:

            q = {'date': {'$gt': start_date, '$lte': end_date},
                 'cc': {'$in': list(data.keys())}}

            logger.debug(f'addup_to_date_q={q}')

            async for cur in _table.find(q, {'_id': 0}):
                _add_item(cur)

        cc_q = {'cc': {'$in': list(ccs)}, 'date': {'$lte': end_date}}
        logger.debug(f'cc_q={cc_q}')

        async for cur in _table.find(cc_q, {'_id': 0}):
            _add_item(cur)

        for cc, _prefixes in data.items():
            for _prefix, _count in _prefixes.items():
                _add_to_cc_map(cc_map, _prefix, cc, _count)

        await cls.insert_space(v, end_date, cc_map, left)
        return cc_map

    @classmethod
    @time_cost()
    def convert_cc_map(cls, cc_map: dict) -> list:
        if not cc_map:
            return []

        prefix_map = {}

        for cc, prefixes in cc_map.items():

            for _prefix, _count in prefixes.items():
                if _prefix not in prefix_map:
                    prefix_map[_prefix] = {cc: _count}

                else:
                    if cc not in prefix_map[_prefix]:
                        prefix_map[_prefix][cc] = _count

                    else:
                        prefix_map[_prefix][cc] += _count

        if not prefix_map:
            return []

        data = []
        for prefix, cc in prefix_map.items():

            if not cc:
                continue

            c = sorted(list(cc.items()), key=lambda x: x[1], reverse=True)[0]
            #logger.debug(f'prefix={prefix}, c={c}, '
            #             f'from={sorted(list(cc.items()), key=lambda x: x[1], reverse=True)}')
            item = {
                'prefix': prefix,
                'country': c[0]
            }
            data.append(item)

        return data

    @classmethod
    @time_cost()
    def convert_prefix_map(cls, prefix_map: dict) -> list:
        if not prefix_map:
            return []

        data = []
        for prefix, cc in prefix_map.items():

            if not cc:
                continue

            c = sorted(list(cc.items()), key=lambda x: x[1], reverse=True)[0]
            # logger.debug(f'c={c[0]}, prefix={prefix}, from={sorted(list(cc.items()), key=lambda x: x[1], reverse=True)}')
            item = {
                'prefix': prefix,
                'country': c[0]
            }
            data.append(item)

        logger.debug(f'prefix={len(data)}')
        return data


class VisIPTrend(BaseModel):
    cc: str
    data: list
    v: str

    @classmethod
    async def get_trend(cls, v, cc: list):
        if not cc:
            return {}

        _table = TableSelector.get_ip_trend()

        cc_map = {}
        async for cur in _table.find({'cc': {'$in': cc}, 'v': v}, {'_id': 0, 'v': 0}):
            cc_map[cur['cc']] = cur['data']

        _left = set(cc) - set(cc_map.keys())
        return cc_map, _left

    @classmethod
    def insert_trend(cls, v, cc_map):
        logger.debug(f'v={v}, cc_map={len(cc_map)}')

        if not cc_map:
            return

        logger.debug(f'insert_trend len={len(cc_map)}')
        _table = TableSelector.get_ip_trend(name='default_sync')
        ops = []

        for cc, trend in cc_map.items():
            ops.append(UpdateOne({'cc': cc, 'v': v},
                                 {'$set': {'data': trend}}, upsert=True))

        _bulk_load(_table, ops)

    @classmethod
    def convert_trend(cls, v, cc_map):
        if not cc_map:
            return cc_map

        _cc_map = {}
        for cc, trend in cc_map.items():
            for _t in trend:
                if str(v) == '6':
                    _t['count'] = _t['count'] * (2 ** 64)
            _cc_map[cc] = trend

        return _cc_map
