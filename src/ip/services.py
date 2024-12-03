from datetime import datetime
from dateutil.rrule import rrule, YEARLY
from utils.misc import ip_to_str


def _get_diff_date(_start: int, _end: int):
    """
    NOTE: the dates returned will not contain _start and _end
    """
    _s = datetime.strptime(str(_start), '%Y%m%d')
    _e = datetime.strptime(str(_end), '%Y%m%d')
    _d = []

    for _c in rrule(YEARLY, dtstart=_s, until=_e):
        if _c in (_s, _e):
            continue

        _d.append(int(_c.strftime('%Y%m%d')))

    return _d


def convert_v6_map_item(item: dict):
    item['ips'] = item['ips'] * (2 ** 64)
    return item


def convert_v6_map(items: list) -> list:
    for item in items:
        convert_v6_map_item(item)
    return items


def convert_map(v: str, items: list) -> list:
    if v == '4':
        return items
    return convert_v6_map(items)


def convert_picture(v, item: dict) -> dict:
    for k in item:
        if item[k] == 'Unknown':
            item[k] = None

        if k == 'ip' and str(v) == '4':
            item[k] = ip_to_str(item[k], v)

    if 'port_services' not in item:
        item['port_services'] = []

    for k in {'asn', 'carrier'}:
        if k not in item:
            item[k] = None

    if 'cc' in item:
        item['country'] = item.pop('cc')

    item['v'] = v

    return item


def _add_to_cc_map(cc_map, _prefix, _cc, _count):

    if _cc not in cc_map:
        cc_map[_cc] = {_prefix: _count}

    else:

        if _prefix not in cc_map[_cc]:
            cc_map[_cc][_prefix] = _count

        else:
            cc_map[_cc][_prefix] += _count


def _add_to_prefix_map(cc_map, _prefix, _cc, _count):

    if _prefix not in cc_map:
        cc_map[_prefix] = {_cc: _count}

    else:

        if _cc not in cc_map[_prefix]:
            cc_map[_prefix][_cc] = _count

        else:
            cc_map[_prefix][_cc] += _count


def convert_prefix(item: dict, v) -> dict:
    if str(v) == '4':
        return item

    else:
        item['count'] = int(item['count'] * (2 ** 64))

    return item
