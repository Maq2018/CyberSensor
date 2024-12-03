import logging
from utils.misc import (
    to_list,
    to_str_list
)
from database.models import TableSelector

logger = logging.getLogger('asn.services')


CN_NUMBER = '一二三四五六七八九十百千万亿兆京'
"""
千字节	KB	103
兆字节	MB	106
吉字节	GB	109
太字节	TB	1012
拍字节	PB	1015
艾字节	EB	1018
泽字节	ZB	1021
尧字节	YB	1024
容字节	RB	1027
昆字节	QB	1030
"""
SIZE_UNIT = 'KMGTPEZYRQ'
NUMBER = '0123456789.'


def strip_location_name(name):
    """
    :param name: 上海电信互联2.5G二
    :return: 上海电信互联
    """
    name = name.rstrip(CN_NUMBER)
    name = name.rstrip(SIZE_UNIT)
    name = name.rstrip(NUMBER)
    return name


def str_to_ases(s):
    ases = to_list(s, fn=lambda x: int(x.strip('asAS')), raise_error=False)
    return ases


async def get_ases_country(ases):
    # todo tw/hk/mo should be taken into cn
    if not ases:
        return {}

    _table = TableSelector.get_as_table()
    cc_map = {}
    # logger.debug(f'q={ases}')

    async for cur in _table.find({'asn': {'$in': list(ases)}}):
        if cur['country'] == 'None' or not cur['country']:
            continue

        cc_map[cur['asn']] = cur['country']

    # logger.debug(f'cc_map={cc_map}')
    return cc_map


async def get_cities_location(cities):
    if not cities:
        return {}

    _edu_as_city_table = TableSelector.get_edu_as_city_table()
    city_map = {}

    async for cur in _edu_as_city_table.find({'city': {'$in': list(cities)}}):
        city_map[cur['city']] = (cur['lng'], cur['lat'])

    return city_map


def load_all_ases_country():
    as_map = {}
    _table = TableSelector.get_as_table(name='default_sync')

    for cur in _table.find({}, {'asn': 1, 'country': 1}):
        if cur['country'] == 'None' or not cur['country']:
            continue

        as_map[cur['asn']] = cur['country']

    return as_map


def get_top_hops(idx, routes, need):

    last_count = {}
    last_route_map = {}
    _routes = []
    for route in routes:

        if idx >= len(route):
            _routes.append(route)
            continue

        if route[idx] not in last_count:
            last_count[route[idx]] = 1
            last_route_map[route[idx]] = [route]

        else:
            last_count[route[idx]] += 1
            last_route_map[route[idx]].append(route)

    filter_count = sorted(list(last_count.items()), key=lambda x: x[1], reverse=True)[:need]
    filter_map = {k for k, _ in filter_count}

    return {k: v for k, v in last_route_map.items() if k in filter_map}, _routes


def get_need_count(route_map, top=10):
    max_count = 0

    for k, v in route_map.items():
        if len(v) >= max_count:
            max_count = len(v)

    count_map = {}
    depend_map = {}

    for k, v in route_map.items():
        n = int(top * len(v) / max_count)
        if n < 1:
            n = 1

        count_map[k] = n
        depend_map[k] = n * 1000

    return count_map, depend_map


def get_paths(routes):

    _routes = routes

    tops, _ = get_top_hops(0, routes, need=10)
    _tops = {}

    idx = 1
    short_routes = []
    depend_map = {}

    while idx < 4:

        count_map, _depend_map = get_need_count(tops, top=7)
        depend_map.update(_depend_map)
        for v, _routes in tops.items():
            _t, _r = get_top_hops(idx, _routes, count_map[v])
            short_routes += _r
            _tops.update(_t)

        idx += 1
        tops = _tops
        _tops = {}

    for v, _routes in tops.items():
        depend_map[v] = len(_routes) * 1000

    _filter_routes = []
    for _routes in tops.values():
        _filter_routes += _routes

    return _filter_routes + short_routes, depend_map


async def cernet_paths():
    _table = TableSelector.get_edu_as_path_table()

    length = 4
    routes = []
    records = set()
    _path_map = {}

    async for cur in _table.find({}, {'path': 1}):
        _path = [str(_c) for _c in cur['path'][:length]]

        _record = []

        if _path_to_str(_path_map, _path) in records:
            continue

        for v in _path:
            _record.append(v)
            records.add(_path_to_str(_path_map, _record))

        routes.append(_path)

    idx = 0
    level_map = {}  # {asn: {v: 0, {0: asn, 1: asn-1, 2: asn-2}}

    while idx < length:
        c_map = {}

        for route in routes:
            if len(route) < idx + 1:
                continue

            if route[idx] not in level_map:
                level_map[route[idx]] = (idx, 0)

            else:
                _i, _c = level_map[route[idx]]

                if _i == idx:
                    if _c != 0:
                        route[idx] = f'{route[idx]}-{_c}'  # f'{route[idx]}-{_c}'
                else:
                    level_map[route[idx]] = (idx, _c + 1)
                    route[idx] = f'{route[idx]}-{_c+1}'  # f'{route[idx]}-{_c+1}'

            if route[idx] not in c_map:
                c_map[route[idx]] = 1
            else:
                c_map[route[idx]] += 1

        top_c = sorted(list(c_map.items()), key=lambda x: x[1], reverse=True)[:1]
        idx += 1
        logger.debug(f'^^^^idx={idx}, a={top_c[0]}')

    top_routes, point_map = get_paths(routes)
    return top_routes, point_map


def _path_to_str(_path_map, _rp):
    if not _rp:
        return ''

    _rp = tuple(_rp)
    if _path_map and _rp in _path_map:
        return _path_map[_rp]

    _path_s = ','.join([str(_r) for _r in _rp])
    _path_map[_rp] = _path_s
    return _path_s


def convert_hijack_event(e):
    if 'victim' in e:
        e['victim'] = str(e['victim'])

    if 'attacker' in e:
        e['attacker'] = str(e['attacker'])

    if 'normal_paths' in e:
        e['normal_paths'] = to_str_list(e['normal_paths'])

    if 'abnormal_paths' in e:
        e['abnormal_paths'] = to_str_list(e['abnormal_paths'])

    if 'hops' in e:
        _hops = []
        for hop in e['hops']:
            _hops.append(list(map(str, hop)))

        e['hops'] = _hops

    return convert_as_item(e)


def convert_as_item(item: dict) -> dict:
    _replace = {'country': 'region', 'countries': 'regions'}
    _new = {}

    for k, v in item.items():
        _key = k

        for _rk, _rn in _replace.items():
            if _rk in k:
                _key = k.replace(_rk, _rn)

        _new[_key] = v

    for key in {'as', 'asn'}:
        if key in _new:
            _new[key] = str(_new[key])

    return _new


def convert_bandwidth_item(item: dict) -> dict:
    if 'bandwidth' in item:
        item['bandwidth'] = round(item['bandwidth'], 3)

    return item


def convert_bandwidth_list(items: list) -> list:
    for item in items:
        convert_bandwidth_item(item)

    return items
