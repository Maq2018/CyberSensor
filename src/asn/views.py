from fastapi import APIRouter, Depends
import logging
from copy import deepcopy
from collections import OrderedDict
from .models import (
    EduHistoryQuery,
    ASQuery,
    EduTrendQuery,
    ASHijackDetailQuery,
    ASHijackQuery,
    ASHijackSummaryQuery,
    ASPathSearchQuery,
    ASTrendsQuery
)
from .services import (
    strip_location_name,
    str_to_ases,
    get_ases_country,
    get_cities_location,
    cernet_paths,
    _path_to_str,
    convert_hijack_event,
    convert_as_item,
    convert_bandwidth_list
)
from database.models import (
    TableSelector,
    VisCache
)
from utils.request import (
    TimeRangeQuery,
    DateQuery,
    RefreshQuery
)
from utils.misc import (
    extract_limit_offset_from_args,
    subnet_range,
    to_list,
    Regions,
    to_int,
    to_str_list
)
from decorators import time_cost

router = APIRouter(prefix='/as')
logger = logging.getLogger('asn.views')


@router.get('/cernet/history')
async def cernet_history(args: EduHistoryQuery = Depends()):
    """
    :param args: date, abroad
    :return:
    {
    "data": {
        "ases": 1000,
        "locations": 68,
        "bandwidth": 56,
        "routes": [{
            "location": "上海",
            "bandwidth": 68,
            "asn": "AS23434",
            "name": "上海互联",
            "lng": 121.55419074442398,
            "lat": 31.197895374762922
        }]
    },
    "status": "ok",
    "message": ""
    }
    """

    _edu_as_table = TableSelector.get_edu_as_history_table()
    _edu_as_city_table = TableSelector.get_edu_as_city_table()

    ases = set()
    locations = set()
    bandwidth = 0

    q = {'abroad': 0 if args.abroad == 0 else 1, 'date': {'$lte': args.date * 10000 + 9999}}

    logger.debug(f'q={q}')

    route_map = {}  # {(location, asn, name): bandwidth}
    location_map = {}  # {city: (lng, lat)}
    abroad_route_map = {}  # {(location, asn, name, bandwidth): count}
    searched_locations = set()

    async for cur in _edu_as_table.find(q):
        bandwidth += cur['bandwidth']
        searched_locations.add(cur['location'])

        if args.abroad == 0:
            _name = strip_location_name(cur['name'])
            _key = cur['location'], cur['asn'], _name

            if _key not in route_map:
                route_map[_key] = 0

            route_map[_key] += cur['bandwidth']

        else:
            _name = cur['name']
            _key = cur['location'], cur['asn'], _name, cur['bandwidth']
            _rkey = cur['location'], cur['asn'], _name, -cur['bandwidth']

            if _key not in abroad_route_map:

                if _rkey in abroad_route_map:
                    abroad_route_map[_rkey] -= 1
                    # logger.debug(f'rkey={_rkey} dow, abroad_route_map={abroad_route_map}, len={len(abroad_route_map)}')

                    if abroad_route_map[_rkey] == 0:
                        del abroad_route_map[_rkey]
                        # logger.debug(f'delete={_rkey}, abroad_route_map={abroad_route_map}, len={len(abroad_route_map)}')
                    continue
                else:
                    abroad_route_map[_key] = 0

            abroad_route_map[_key] += 1
            # logger.debug(f'abroad_route_map={abroad_route_map}, len={len(abroad_route_map)}')

    if searched_locations:
        location_map = await get_cities_location(searched_locations)

    routes = []

    def _get_location(_loc):
        if _loc not in location_map:
            # logger.warning(f"location of {_loc} missing")
            _long, _lati = None, None
        else:
            _long, _lati = location_map[_loc]

        return _long, _lati

    if args.abroad == 0:

        for (_location, _asn, _name), _bandwidth in route_map.items():
            if _bandwidth <= 0:
                continue

            ases.add(_asn)
            locations.add(_location)

            _lng, _lat = _get_location(_location)
            item = {
                'bandwidth': _bandwidth,
                'location': _location,
                'asn': f"AS{_asn}",
                'name': _name,
                'lng': _lng,
                'lat': _lat,
            }
            routes.append(item)

    else:

        for (_location, _asn, _name, _bandwidth), _count in abroad_route_map.items():
            if _count < 1 or _bandwidth == 0:
                continue

            ases.add(_asn)
            locations.add(_location)

            _lng, _lat = _get_location(_location)

            for i in range(_count):
                item = {
                    'bandwidth': _bandwidth,
                    'location': _location,
                    'asn': f"AS{_asn}",
                    'name': _name,
                    'lng': _lng,
                    'lat': _lat,
                }

                routes.append(item)

    data = {
        'ases': len(ases),
        'locations': len(locations),
        'bandwidth': round(bandwidth, 3),
        'routes': convert_bandwidth_list(routes)
    }

    return {'data': data, 'status': 'ok', 'message': ''}


@router.get('')
async def as_(args: ASQuery = Depends()):
    _table = TableSelector.get_as_table()
    ases = str_to_ases(args.asns)

    if not ases:
        return {'data': [], 'status': 'ok', 'message': ''}

    q = {'asn': {'$in': ases}}
    data = []

    async for cur in _table.find(q, {'_id': 0}):
        data.append(convert_as_item(cur))

    return {'data': data, 'status': 'ok', 'message': ''}


@router.get('/cernet/trend')
async def cernet_trend(args: EduTrendQuery = Depends()):
    _table = TableSelector.get_edu_as_history_table()

    data = []
    last_date = None
    bandwidth = 0
    ases = set()
    locations = set()

    def _get_year(_date):
        return int(_date / 10000)

    q = {'abroad': 0 if args.abroad == 0 else 1}
    logger.debug(f'q={q}')

    async for cur in _table.find(q).sort('date', 1):

        _year = _get_year(cur['date'])

        bandwidth += cur['bandwidth']
        ases.add(cur['asn'])
        locations.add(cur['location'])

        item = {
            'date': _year,
            'ases': len(ases),
            'bandwidth': bandwidth,
            'locations': len(locations)
        }

        if last_date is None:
            data.append(item)

        else:

            if last_date == _year:
                data[-1]['locations'] = len(locations)
                data[-1]['ases'] = len(ases)
                data[-1]['bandwidth'] = bandwidth

            else:

                if _year - last_date == 1:
                    data.append(item)

                else:
                    _last_item = data[-1]

                    for i in range(1, _year - last_date):
                        _item = deepcopy(_last_item)
                        _item['date'] = last_date + i
                        data.append(_item)

                    data.append(item)

        last_date = _year

    return {'data': convert_bandwidth_list(data), 'status': 'ok', 'message': ''}


@router.get('/hijack/detail')
async def hijack_detail(args: ASHijackDetailQuery = Depends()):
    _table = TableSelector.get_as_hijack_table()
    _as_table = TableSelector.get_as_table()

    if not args.index:
        return {'data': {}, 'status': 'ok', 'message': ''}

    q = {'index': args.index}
    logger.debug(f'q={q}')
    obj = None

    async for cur in _table.find(q):
        obj = cur

    # logger.debug(f'obj={obj}')
    data = {
        'timestamp': obj['timestamp'],
        'index': obj['index'],
        'prefix': obj['prefix'],
        'attacker': str(obj['attacker']),
        'victim': str(obj['victim']),
        'affected_count': obj['affected_count'],
        'attacker_country': obj['attacker_country'],
        'victim_country': obj['victim_country'],
        'abnormal_paths': [],
        'normal_paths': [],
        'regions': [],
        'hops': []
    }

    _path_map = {}

    def _merge_paths(_rpaths):
        if len(_rpaths) < 2:
            return _rpaths

        _spath_map = set()
        _rpaths = sorted(_rpaths, key=lambda x: len(x), reverse=True)

        _merged_paths = []
        for _rpath in _rpaths:
            _record = []

            if _path_to_str(_path_map, _rpath) in _spath_map:
                continue

            for _v in _rpath:
                _record += [_v]
                _spath_map.add(_path_to_str(_path_map, _record))

            _merged_paths.append(_rpath)

        return _merged_paths

    normal_paths = []
    abnormal_paths = []
    normal_paths_map = {}
    for _path in obj['normal_paths']:

        if _path and _path[-1] == obj['victim']:
            normal_paths_map[_path[0]] = _path

    for _path in obj['abnormal_paths']:

        if not _path:
            continue

        if _path[-1] == obj['attacker'] and _path[0] in normal_paths_map:
            abnormal_paths.append(_path)
            normal_paths.append(normal_paths_map[_path[0]])

    ases = set()
    for _paths in [normal_paths, abnormal_paths, obj['hops']]:
        for _path in _paths:
            ases |= set(_path)

    cc_map = {}  # {as: country}
    if ases:
        cc_map = await get_ases_country(ases)

    def _get_has_cc_paths(_cc_paths):

        _has_cc_paths = []
        for _cc_path in _cc_paths:

            _has_cc_path = []
            _has_cc = True

            for _cc_as in _cc_path:
                if not cc_map.get(_cc_as) or cc_map[_cc_as] == 'None':
                    logger.debug(f'country of {_cc_as} not found')
                    _has_cc = False
                    break

                _has_cc_path.append({'as': _cc_as, 'region': cc_map[_cc_as]})

            if _has_cc:
                _has_cc_paths.append(_has_cc_path)

        return _has_cc_paths

    if not cc_map:
        logger.warning(f'no countries found for cc={ases}')
        _cc_normal_paths = []
        _cc_abnormal_paths = []

    else:
        _cc_normal_paths = _get_has_cc_paths(_merge_paths(normal_paths))
        _cc_abnormal_paths = _get_has_cc_paths(_merge_paths(abnormal_paths))

        _cc_normal_path_map = {}
        for _cc_n_path in _cc_normal_paths:
            _cc_normal_path_map[_cc_n_path[0]['as']] = {'normal': _cc_n_path}

        for _cc_a_path in _cc_abnormal_paths:
            if _cc_a_path[0]['as'] in _cc_normal_path_map:
                _cc_normal_path_map[_cc_a_path[0]['as']]['abnormal'] = _cc_a_path

        _cc_normal_paths = []
        _cc_abnormal_paths = []

        idx = 0
        for _, paths in _cc_normal_path_map.items():
            if idx >= 3:
                break

            _cc_normal_paths.append(paths['normal'])
            _cc_abnormal_paths.append(paths['abnormal'])
            idx += 1

    ordered_cc = OrderedDict()  # {cc: {as}

    _hops = []

    for hop in obj['hops']:
        _hop_cc = []
        _ordered_hop_cc = OrderedDict()
        for _as in hop:

            if not cc_map.get(_as) or cc_map[_as] == 'None':
                # logger.debug(f'country of {_as} not found')
                continue

            if cc_map[_as] not in ordered_cc:
                ordered_cc[cc_map[_as]] = set()

            if cc_map[_as] not in _ordered_hop_cc:
                _ordered_hop_cc[cc_map[_as]] = set()

            ordered_cc[cc_map[_as]].add(_as)
            _ordered_hop_cc[cc_map[_as]].add(_as)

        for cc, ases in _ordered_hop_cc.items():
            if Regions.is_hidden_region(cc):
                continue

            item = {
                'region': cc,
                'ases': len(ases)
            }

            _hop_cc.append(item)

        if _hop_cc:
            _hops.append(_hop_cc)

    _countries = []

    for cc, ases in ordered_cc.items():
        if Regions.is_hidden_region(cc):
            continue

        item = {
            'region': cc,
            'ases': len(ases)
        }
        _countries.append(item)

    def _convert_paths(_cpaths):
        for _cpath in _cpaths:
            for _c in _cpath:
                _c['as'] = str(_c['as'])

        return _cpaths

    data['regions'] = _countries
    data['hops'] = _hops
    data['abnormal_paths'] = _convert_paths(_cc_abnormal_paths)
    data['normal_paths'] = _convert_paths(_cc_normal_paths)

    return {'data': convert_as_item(data), 'message': '', 'status': 'ok'}


@router.get('/hijack')
async def hijack(args: ASHijackQuery = Depends()):
    # start, end, search
    _limit, _offset = extract_limit_offset_from_args(args)

    _table = TableSelector.get_as_simple_hijack_table()
    _detail_table = TableSelector.get_as_hijack_table()

    data = []
    ases = set()
    q = {'timestamp': {'$gte': args.start, '$lte': args.end}}

    if args.search:
        i, flag = to_int(args.search)
        if not flag:
            return {'data': [], 'total': 0, 'status': 'ok', 'message': ''}

        q['$or'] = [
            {'attacker': i},
            {'victim': i}
        ]

    logger.debug(f'q={q}')
    has_detail = {}
    indexes = set()

    async for cur in _table.find(q, {
        '_id': 0,
        'normal_paths': 0,
        'abnormal_paths': 0,
        'hops': 0,
    }).sort('timestamp', -1).skip(_offset).limit(_limit):

        ases.add(cur['victim'])
        ases.add(cur['attacker'])
        indexes.add(cur['index'])
        data.append(convert_hijack_event(cur))

    _total = await _table.count_documents(q)

    if indexes:
        _i_q = {'index': {'$in': list(indexes)}}
        logger.debug(f'i_q={_i_q}')

        async for cur in _detail_table.find(_i_q, {'index': 1}):
            has_detail[cur['index']] = 1

    for d in data:
        if d['index'] in has_detail:
            d['has_detail'] = True
        else:
            d['has_detail'] = False

    return {'data': data, 'total': _total, 'status': 'ok', 'message': ''}


@router.get('/hijack/summary')
async def hijack_summary(args: ASHijackSummaryQuery = Depends()):
    if args.data_type not in {'attacker', 'victim', 'attacker_region', 'victim_region'}:
        return {'data': [], 'status': 'ok', 'message': ''}

    if args.data_type == 'attacker_region':
        args.data_type = 'attacker_country'

    if args.data_type == 'victim_region':
        args.data_type = 'victim_country'

    q = {'timestamp': {'$gte': args.start, '$lte': args.end}}
    if 'country' in args.data_type:
        q[args.data_type] = {'$ne': None}

    pipe = [
        {'$match': q},
        {'$group': {'_id': f'${args.data_type}', 'count': {'$sum': 1}}},
        {'$sort': {'count': -1, '_id': 1}},
        {'$limit': 5}
    ]

    logger.debug(f'q={pipe}')
    data = []

    _table = TableSelector.get_as_simple_hijack_table()

    async for cur in _table.aggregate(pipe):
        item = {
            'name': str(cur['_id']),
            'count': cur['count']
        }
        data.append(item)

    return {'data': data, 'status': 'ok', 'message': ''}


@router.get('/hijack/regions')
async def hijack_countries(args: TimeRangeQuery = Depends()):
    q = {'timestamp': {'$gte': args.start, '$lte': args.end}}
    pipe = [
        {'$match': {'timestamp': {'$gte': args.start, '$lte': args.end},
                    'victim_country': {'$ne': None}}},
        {'$group': {'_id': '$victim_country', 'count': {'$sum': 1}}}
    ]

    logger.debug(f'pipe={pipe}, q={q}')
    data = []

    _table = TableSelector.get_as_simple_hijack_table()

    async for cur in _table.aggregate(pipe):
        item = {
            'region': cur['_id'],
            'count': cur['count']
        }
        data.append(item)

    _total = await _table.count_documents(q)
    return {'data': data, 'total': _total, 'status': 'ok', 'message': ''}


@router.get('/cernet/path/search')
async def path_search(args: ASPathSearchQuery = Depends()):
    prefix_start, prefix_end = subnet_range(args.prefix)

    _table = TableSelector.get_edu_as_path_table()

    if None in [prefix_start, prefix_end]:
        return {'data': [], 'message': 'invalid prefix', 'status': 'ok'}

    _large_q = {'prefix_start': {'$lte': prefix_start},
                'prefix_end': {'$gte': prefix_end}}

    _small_q = {'prefix_start': {'$gte': prefix_start},
                'prefix_end': {'$lte': prefix_end}}

    @time_cost()
    async def _large_search():
        _matched = None
        _searched = []
        logger.debug(f'large_q={_large_q}')

        async for _cur in _table.find(_large_q).sort('prefix_start', -1).limit(1):
            _searched = to_str_list(_cur['path'])
            _matched = _cur['prefix']

        return _searched, _matched

    @time_cost()
    async def _small_search():
        _matched = None
        _searched = []
        logger.debug(f'small_q={_small_q}')

        async for _cur in _table.find(_small_q).sort('prefix_start', 1).limit(1):
            _searched = to_str_list(_cur['path'])
            _matched = _cur['prefix']

        return _searched, _matched

    searched, matched = await _large_search()

    if searched:
        return {'data': searched, 'status': 'ok', 'message': '', 'matched': matched}

    searched, matched = await _small_search()

    return {'data': searched, 'status': 'ok', 'message': '', 'matched': matched}


@router.get('/cernet/path')
async def path(args: RefreshQuery = Depends()):
    _key = 'as/cernet/path'

    if not args.refresh:
        cached = await VisCache.get_cache(_key)
        if cached:
            return cached

    paths, point_map = await cernet_paths()
    data = {'data': {'paths': paths, 'dependencies': point_map}, 'status': 'ok', 'message': ''}

    await VisCache.add_cache(_key, data)
    return data


@router.get('/cernet/summary')
async def cernet_summary(args: RefreshQuery = Depends()):
    _key = 'as/cernet/summary'
    if not args.refresh:
        cached = await VisCache.get_cache(_key)
        if cached:
            return cached

    _table = TableSelector.get_edu_as_path_table()
    _hops_count_map = [set(), set(), set(), set()]
    _dependencies_map = {}

    length = 4
    async for cur in _table.find({}, {'path': 1}):

        for idx, v in enumerate(cur['path'][:length]):
            _hops_count_map[idx].add(v)

            if v not in _dependencies_map:
                _dependencies_map[v] = 0

            _dependencies_map[v] += 1

    as_summary = []
    for _hops in _hops_count_map:
        as_summary.append(len(_hops))

    _tops = sorted(_dependencies_map.items(), key=lambda x: x[1], reverse=True)[:5]
    top_dependencies = []
    for v, count in _tops:
        item = {
            'as': str(v),
            'dependency': count
        }
        top_dependencies.append(item)

    data = {'data': {'top_dependencies': top_dependencies,
                     'as_summary': as_summary},
            'status': 'ok', 'message': ''}

    await VisCache.add_cache(_key, data)
    return data


@router.get('/trends')
async def trends(args: ASTrendsQuery = Depends()):
    cc = to_list(args.countries, raise_error=False, fn=str.upper)
    if not cc:
        return {'data': {}, 'message': '', 'status': 'ok'}

    q = {'cc': {'$in': cc}}
    logger.debug(f'q={q}')

    _table = TableSelector.get_asn_alloc_table()
    cc_map = {}

    async for cur in _table.find(q, {'count': 1, 'cc': 1, 'date': 1}):
        if cur['cc'] not in cc_map:
            cc_map[cur['cc']] = []

        item = {
            'date': int(cur['date'] / 10000),
            'count': cur['count'],
        }

        cc_map[cur['cc']].append(item)

    trend_map = {}

    for cc, trend in cc_map.items():
        if not trend:
            continue

        _trend = []
        trend = sorted(trend, key=lambda x: x['date'])
        prev = None

        addup = 0
        for item in trend:
            addup += item['count']

            if item['date'] == 0:
                #logger.warning(f"got zero date of cc, cur={item}")
                continue

            if item['date'] == prev and _trend:
                _trend[-1]['count'] = addup

            else:
                _trend.append({'date': item['date'], 'count': addup})

            prev = item['date']

        logger.debug(f'first={trend[0]}, last={trend[-1]},'
                     f'trend={len(trend)}, new-trend={len(_trend)}, '
                     f'first-new-trend={_trend[0]}, last-new-trend={_trend[-1]}')

        if _trend:
            trend_map[cc] = _trend

    pipe = [
        {'$group': {'_id': {'$toInt': {'$divide': ['$date', 10000]}},
                    'count': {'$sum': '$count'}}},
        {'$sort': {'_id': 1}}
    ]
    _totals = []
    logger.debug(f'pipe={pipe}')

    y_addup = 0
    async for cur in _table.aggregate(pipe):
        y_addup += cur['count']

        if cur['_id'] == 0:
            logger.warning(f"got zero date cur={cur}")
            continue

        item = {
            'date': cur['_id'],
            'count': y_addup
        }
        _totals.append(item)

    trend_map['total'] = _totals
    return {'data': trend_map, 'status': 'ok', 'message': ''}


@router.get('/summary')
async def summary(args: DateQuery = Depends()):
    if args.date < 1:
        return {'data': {}, 'status': 'ok', 'message': ''}

    _date = int(args.date * 10000 + 9999)
    _q = {'date': {'$lte': _date}}

    _pipe = [
        {'$match': _q},
        {'$group': {'_id': None, 'count': {'$sum': '$count'}}}
    ]

    _prefix_pipe = [
        {'$match': _q},
        {'$group': {'_id': None, 'count': {'$sum': 1}}}
    ]

    logger.debug(f'pipe={_pipe}, prefix_pip={_prefix_pipe}')
    _as_table = TableSelector.get_asn_alloc_table()

    ases = 0
    async for cur in _as_table.aggregate(_pipe):
        ases = cur['count']

    return {'data': {'ases': ases},
            'status': 'ok', 'message': ''}


@router.get('/map')
async def as_map(args: DateQuery = Depends()):
    if args.date < 1:
        return {'data': [], 'status': 'ok', 'message': ''}

    _table = TableSelector.get_asn_alloc_table()

    pipe = [
        {'$match': {'date': {'$lte': int(args.date * 10000 + 9999)}}},
        {'$group': {'_id': '$cc', 'count': {'$sum': '$count'}}}
    ]

    logger.debug(f'q={pipe}')

    cc = []
    async for cur in _table.aggregate(pipe):
        if not cur.get('_id'):
            continue

        if cur['_id'] == Regions.ZZ:
            continue

        item = {
            'adcode': cur['_id'],
            'count': cur['count']
        }

        cc.append(item)

    cc = sorted(cc, key=lambda x: x['count'], reverse=True)
    return {'data': cc, 'status': 'ok', 'message': ''}
