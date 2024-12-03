from fastapi import APIRouter, Depends, Response
import orjson
import time
import math
import logging
from ipaddress import IPv6Address
from functools import partial
from netaddr import (
    IPNetwork,
    cidr_merge
)
from collections import namedtuple
from .models import (
    IPPrefixInfoQuery,
    IPTrendsQuery,
    IPQueryWithTime,
    IPSpaceQuery,
    IPPrefixInfoCountryQuery,
    IPNetflowQuery,
    VisIPMap,
    ProbePictureQuery,
    VisIPSpace,
    VisIPTrend,
    ProbeMapQuery,
    IPv4_RANGE,
    IPv6_RANGE
)
from database.models import (
    TableSelector,
    VisCache
)
from utils.misc import (
    to_list,
    subnet_range,
    get_b_subnet_v4,
    get_b_subnets_v4,
    ip_to_str,
    extract_limit_offset_from_args,
    str_to_int_v4,
    str_to_exploded_ipv6,
    Regions,
)
from .services import (
    _get_diff_date,
    convert_map,
    convert_picture,
    _add_to_cc_map,
    _add_to_prefix_map,
    convert_prefix
)
from utils.request import IPBaseQuery
from decorators import time_cost
from config import Config


router = APIRouter(prefix='/ip')
logger = logging.getLogger('ip.views')

# TODO:
# HK, MO, TW -> CN


@router.get('/prefix')
async def prefix_info(args: IPPrefixInfoQuery = Depends()):
    """
    :param args:prefix, v, date
    :return:
    {
    "data": [{
        "country": "CN",
        "count": 123,
        "prefixes": ["10.16.0.0/16", "10.17.8.0/24"]
    }],
    "status": "ok",
    "message": ""
    }
    """

    _table = TableSelector.get_prefix_alloc_table(args.v)

    cc_map = {}  # {cc: {count: 0, subnets: []}
    _left, _right = subnet_range(args.prefix, args.v)

    q = {'prefix_start': {'$gte': _left},
         'prefix_end': {'$lte': _right},
         'date': {'$lte': args.date}}

    logger.debug(f'q={q}')
    _cidr = int(args.prefix.split('/')[-1])

    if args.v == '4':
        _count = 2 ** (32 - _cidr)

    else:
        _count = 2 ** (64 - _cidr)

    async for cur in _table.find(q):
        if cur['cc'] not in cc_map:
            cc_map[cur['cc']] = {'count': cur['count'],
                                 'prefixes': [cur['prefix']],
                                 'country': cur['cc']}

        else:
            cc_map[cur['cc']]['count'] += cur['count']
            cc_map[cur['cc']]['prefixes'].append(cur['prefix'])

    if not cc_map:

        q = {'prefix_start': {'$lte': _left},
             'prefix_end': {'$gte': _right},
             'date': {'$lte': args.date}}

        logger.debug(f'contain_q={q}')

        async for cur in _table.find(q).sort('prefix_start', -1).limit(1):
            if cur['cc'] not in cc_map:
                cc_map[cur['cc']] = {'count': _count,
                                     'prefixes': [args.prefix],
                                     'country': cur['cc']}

            else:
                cc_map[cur['cc']]['count'] += cur['count']
                cc_map[cur['cc']]['prefixes'].append(cur['prefix'])

    cc_list = sorted(list(cc_map.items()), key=lambda x: x[1]['count'], reverse=True)

    data = []
    for _, item in cc_list:
        item['prefixes'] = sorted(item['prefixes'])
        data.append(convert_prefix(item, args.v))

    return {'data': data, 'status': 'ok', 'message': ''}


@router.get('/prefix/country')
async def prefix_info_country(args: IPPrefixInfoCountryQuery = Depends()):
    """
    :param args: v, date, country, page_size, page
    :return:
    {
    "data": [{
        "prefix": 100,
        "count": 89
    }],
    "status": "",
    "message": ""
    }
    """

    _table = TableSelector.get_prefix_alloc_table(args.v)

    q = {'cc': args.country, 'date': {'$lte': args.date}}
    _limit, _offset = extract_limit_offset_from_args(args)

    logger.debug(f'q={q}, limit={_limit}, offset={_offset}')

    data = []
    async for cur in _table.find(q).sort('count', -1).\
            skip(_offset).limit(_limit):

        item = {
            'prefix': cur['prefix'],
            'count': cur['count']
        }
        data.append(item)

    _total = await _table.count_documents(q)

    data = sorted(data, key=lambda x: x['count'], reverse=True)
    return {'data': data, 'status': 'ok', 'message': '', 'total': _total}


@router.get('/trends')
async def ip_trends(args: IPTrendsQuery = Depends()):
    """
    todo: only have cn as parameter, cannot have its son as parameter
    todo: think about aggregations of data
    :param args: v, countries
    :return:
    {
    "data": {
        "CN": [{
            "date": 19480101,
            "count": 223
        }],
        "US": [{
            "date": 19480101,
            "count": 2223
        }]
    },
    "status": "ok",
    "message": ""
    }
    """

    _table = TableSelector.get_prefix_alloc_table(args.v)

    cc = to_list(args.countries, raise_error=False, fn=str.upper)
    if not args.countries or not cc:
        return {'data': {}, 'status': 'ok', 'message': 'no countries provided'}

    _cached, _left = await VisIPTrend.get_trend(args.v, cc)
    logger.debug(f'cached={len(_cached)}, left={len(_left)}')

    if _cached and not _left:
        return {'data': VisIPTrend.convert_trend(args.v, _cached),
                'status': 'ok', 'message': ''}

    q = {'cc': {'$in': list(_left)}}
    cc_map = {}

    async for cur in _table.find(q):

        if cur['cc'] not in cc_map:
            cc_map[cur['cc']] = []

        cc_map[cur['cc']].append(dict(date=cur['date'], count=cur['count']))

    cc_hills = {}
    logger.debug(f'got cc_map={len(cc_map)}')

    for cc, tlist in cc_map.items():
        ordered = sorted(tlist, key=lambda x: x['date'], reverse=False)

        hills = []
        addup = 0
        last_ts = None

        for t in ordered:
            addup += t['count']

            if last_ts == t['date']:
                hills[-1]['count'] = addup
            else:
                hills.append({'date': t['date'], 'count': addup})

            last_ts = t['date']

        cc_hills[cc] = hills

    VisIPTrend.insert_trend(args.v, cc_hills)
    _cached.update(cc_hills)

    return {'data': VisIPTrend.convert_trend(args.v, _cached),
            'status': 'ok', 'message': ''}


@router.get('/map')
async def ip_map(args: IPQueryWithTime = Depends()):
    # todo can use trend table
    """
    :param args:  v, date
    :return:
        {
        "data": [{
            "country": "CN",
            "prefixes": 30,
            "ips": 100
        }],
        "status": "ok",
        "message": ""
    }
    """
    _map = await VisIPMap.get_latest_map(args.v, args.date)
    if _map:

        if _map['v'] == args.v and _map['date'] == args.date:
            return {'data': convert_map(args.v, _map['data']),
                    'status': 'ok', 'message': ''}

        else:
            _addup_map = await VisIPMap.addup_to_date(args.v, _map['date'], args.date, _map['data'])
            return {'data': convert_map(args.v, _addup_map),
                    'status': 'ok', 'message': ''}

    _table = TableSelector.get_prefix_alloc_table(args.v)

    _q = {}
    pipe = [
        {'$match': {'date': {'$lte': args.date}}},
        {'$group': {'_id': '$cc',
                    'ips': {'$sum': '$count'},
                    'prefixes': {'$sum': 1}}},
        {'$sort': {'ips': -1}}
    ]

    data = []
    logger.debug(f'pipe={pipe}')

    async for cur in _table.aggregate(pipe):
        if cur['_id'] in {'HK', 'MO', 'TW', ''}:
            logger.debug(f"got watched region={cur}")
            continue

        item = dict(
            country=cur['_id'],
            ips=cur['ips'],
            prefixes=cur['prefixes']
        )

        data.append(item)

    data = sorted(data, key=lambda x: x['ips'], reverse=True)
    await VisIPMap.insert_map(args.v, args.date, data)

    data = convert_map(args.v, data)
    return {'data': data, 'message': '', 'status': 'ok'}


SpaceArgs = namedtuple('SpaceArgs',
                       ['v', 'date', 'countries',
                        'refresh', 'tight', 'force_cache'])


@time_cost()
async def _ip_space_init(cache_reuse=1):

    countries = 'US,CN,JP,DE,GB,KR,BR,FR,CA,IT'
    logger.debug('init ip space ...')

    for _date in IPv4_RANGE:
        args = SpaceArgs(v='4', date=_date, countries=countries,
                         refresh=1, tight=0, force_cache=cache_reuse)
        await _ip_space(args)

    for _tight in [0, 1]:
        for _date in IPv6_RANGE:

            args = SpaceArgs(v='6', date=_date, countries=countries,
                             refresh=1, tight=_tight, force_cache=cache_reuse)
            await _ip_space(args)

    logger.debug('finished init ...')


async def _ip_space(args):
    """
    :param args: v, date, countries，tight(1 - tight - /24; 0 - not tight - /20), force_cache
    :return:
    {
    "data": [{
        "country": "CN",
        "prefix": "1.1.0.0/16"
    }],
    "status": "ok",
    "message": ""
    }
    """

    cc = to_list(args.countries, fn=str.upper, raise_error=False)
    if args.v == '6':
        if args.date > Config.IPv6_ALLOC_END:
            logger.info(f'v6 modify date={args.date} to={Config.IPv6_ALLOC_END}')
            args.date = Config.IPv6_ALLOC_END

    else:
        if args.date > Config.IPv4_ALLOC_END:
            logger.info(f'v4 modify date={args.date} to={Config.IPv4_ALLOC_END}')
            args.date = Config.IPv4_ALLOC_END

    if not cc:
        return {'data': [], 'status': 'ok', 'message': ''}

    _cached = {}  # {cc: {prefix: count}}}
    _found = False
    _found_date = 0
    _found_cc = set()
    _left_cc = set()

    _latest_date = VisIPSpace.get_latest_date(args.v, args.date)

    if not args.refresh or args.force_cache:

        _found, _cached, _found_date, _left_cc, _found_cc = await VisIPSpace.get_latest_space2(
            args.v, args.date, cc, args.tight)

        logger.debug(f'got one={len(_cached)}, left={_left_cc}, '
                     f'found_date={_found_date}, date={args.date}, '
                     f'found_cc={_found_cc}, v={args.v}')

        if not args.refresh and _found and _found_date == args.date and not _left_cc:
            return {'data': VisIPSpace.convert_cc_map(_cached),
                    'status': 'ok', 'message': '',
                    'found_date': _found_date}

    if args.v == '4':
        _q_prefix = '$prefix_b'

    else:
        if args.tight:
            _q_prefix = '$prefix_tight'
        else:
            _q_prefix = '$prefix_straight'

    if not _found:
        _q_cc = cc

    else:
        if _left_cc:
            _q_cc = _left_cc
        else:
            _q_cc = set()

    _addup_cc = set()
    if _found and _found_date < args.date:
        _addup_cc = _found_cc

    _table = TableSelector.get_prefix_alloc_table(args.v)

    if _q_cc:
        pipe = [
            {'$match': {'cc': {'$in': list(_q_cc)},
                        'date': {'$lte': args.date}}},
            {'$group': {'_id': {'cc': '$cc', 'prefix': _q_prefix},
                        'count': {'$sum': '$count'}}}
        ]

        logger.debug(f'q={pipe}')

        tick = time.time()
        idx = 0

        async for cur in _table.aggregate(pipe):
            idx += 1
            _add_to_cc_map(_cached, cur['_id']['prefix'], cur['_id']['cc'], cur['count'])

        logger.debug(f'cost={time.time() - tick}, rows = {idx}')

    if _addup_cc:
        addup_pipe = [
            {'$match': {'cc': {'$in': list(_addup_cc)},
                        'date': {'$lte': args.date,
                                 '$gt': _found_date}}},
            {'$group': {'_id': {'cc': '$cc', 'prefix': _q_prefix},
                        'count': {'$sum': '$count'}}}
        ]
        logger.debug(f'addup_pipe={addup_pipe}')

        tick = time.time()
        addup_idx = 0

        async for cur in _table.aggregate(addup_pipe):
            addup_idx += 1
            _add_to_cc_map(_cached, cur['_id']['prefix'], cur['_id']['cc'], cur['count'])

        logger.debug(f'cost={time.time() - tick}, addup_row = {addup_idx}')

    if args.refresh:
        await VisIPSpace.insert_space(args.v, args.date, _cached,
                                      left={}, tight=args.tight, cc=cc)

    return {'data': VisIPSpace.convert_cc_map(_cached),
            'status': 'ok', 'message': ''}


@router.get("/space")
async def ip_space(args: IPSpaceQuery = Depends()):
    """
    :param args: v, date, countries，tight(1 - tight - /24; 0 - not tight - /20),
                 refresh, force_cache
    :return:
    {
    "data": [{
        "country": "CN",
        "prefix": "1.1.0.0/16"
    }],
    "status": "ok",
    "message": ""
    }
    """
    data = await _ip_space(args)
    #return data
    return Response(orjson.dumps(data), media_type='application/json')


@router.get('/netflow')
async def ip_netflow(args: IPNetflowQuery = Depends()):
    if not args.ip:
        return {'data': {}, 'status': 'ok', 'message': ''}

    _table = TableSelector.get_ip_netflow_table()
    data = []

    q = {'ip': args.ip}
    logger.debug(f'q={q}')

    async for cur in _table.find(q, {'_id': 0, 'n': 0, 'ip': 0}):
        data.append(cur)

    data = sorted(data, key=lambda x: x['timestamp'])
    return {'data': data, 'status': 'ok', 'message': ''}


@router.get('/probe/picture')
async def probe_picture(args: ProbePictureQuery = Depends()):
    # v, ip
    if not args.ip:
        return {'data': {}, 'status': 'ok', 'message': 'no ip provided'}

    _table = TableSelector.get_ip_picture(args.v)

    q = {'ip': str_to_int_v4(args.ip) if args.v == '4' else str_to_exploded_ipv6(args.ip)}

    logger.debug(f'q={q}')

    _cur = await _table.find_one(q, {'_id': 0})
    if not _cur:
        return {'data': {}, 'status': 'ok', 'message': 'not matched'}

    return {'data': convert_picture(args.v, _cur), 'status': 'ok', 'message': ''}


@router.get('/probe/countries')
async def probe_countries(args: IPBaseQuery = Depends()):
    _table = TableSelector.get_ip_picture(args.v)

    cc = await _table.distinct('cc')
    carriers = await _table.distinct('carrier', {'cc': Regions.CN})
    carriers = list(set(carriers) - {Regions.UNKNOWN})

    data = []
    _me = {}

    for _c in cc:
        if Regions.is_unknown(_c):
            continue

        item = {
            'country': _c,
            'children': carriers if _c == Regions.CN else []
        }

        if _c == Regions.CN:
            _me = item
        else:
            data.append(item)

    if _me:
        data = [_me] + data

    return {'data': data, 'status': 'ok', 'message': ''}


@router.get('/probe/map')
async def probe_map(args: ProbeMapQuery = Depends()):

    if not args.country:
        pipe = [
            {'$group': {'_id': '$cc', 'count': {'$sum': 1}}}
        ]
        _type = 'country'
        _key = 'ip/probe/map'

    else:
        pipe = [
            {'$match': {'cc': args.country.upper()}},
            {'$group': {'_id': '$carrier', 'count': {'$sum': 1}}}
        ]
        _type = 'carrier'
        _key = f'ip/probe/map/{args.country.upper()}'

    if args.refresh == 0:
        _cached = await VisCache.get_cache(_key)

        if _cached:
            return _cached

    _table = TableSelector.get_ip_picture(args.v)
    logger.debug(f'probe_map_pipe={pipe}')

    items = []
    async for cur in _table.aggregate(pipe):
        if Regions.is_unknown(cur['_id']):
            continue

        item = {
            'name': cur['_id'],
            'count': cur['count']
        }
        items.append(item)

    items = sorted(items, key=lambda x: x['count'], reverse=True)
    data = {'data': items, 'status': 'ok', 'message': '', 'type': _type}

    await VisCache.add_cache(_key, data)
    return data
