import os
import time
import logging
import subprocess
import multiprocessing
import traceback
from ipaddress import IPv4Network, IPv6Network, IPv4Address, IPv6Address, ip_address
from datetime import datetime


logger = logging.getLogger('utils.misc')


def cmd(s: list, **kwargs):
    logger.info(f"executing command: {s}")
    subprocess.check_call(s, **kwargs)


def cmd2(s: str):
    logger.info(f"executing command: {s}")
    try:
        os.system(s)
    except Exception as e:
        logger.error(f"got err {e} when executing {s}")

    logger.info("finished executing: %s", s)


def get_file_row_count(_file_path: str):
    count = 0
    with open(_file_path, 'r') as fd:
        for _ in fd:
            count += 1

    return count


def split_file(_file_path: str, count: int):
    _row_count = get_file_row_count(_file_path)
    _l = int(_row_count / count)

    _dir = os.path.dirname(_file_path)
    _base = os.path.basename(_file_path)

    string = 'cd %s && split -l%s %s %s.' % (_dir, _l, _base, _base)
    cmd2(string)

    _files = []
    for _file in os.listdir(_dir):
        _p = os.path.join(_dir, _file)
        if _p == _file_path:
            continue

        if not _file.startswith(_base):
            continue

        _files.append(_p)

    return _files


def multiprocess_fn(fn: callable, args: list):
    logger.info(f'call {fn.__name__} with {len(args)} args')

    pool = multiprocessing.Pool(len(args))

    for arg in args:
        pool.apply_async(fn, args=arg)

    pool.close()
    pool.join()


def extract_subnet(string):
    try:
        subnet, mask = string.split('/', 1)
        if int(mask) > 32:
            return None, None
        return subnet, int(mask)
    except:
        return None, None


def subnet_range(subnet, v=4):
    try:
        if str(v) == '4':
            _subnet = IPv4Network(subnet)
            return int(_subnet.network_address), int(_subnet.broadcast_address)

        else:
            _subnet = IPv6Network(subnet)
            # todo return full string format without compression
            return str(_subnet.network_address.exploded), str(_subnet.broadcast_address.exploded)

    except Exception as e:
        logger.error(f" err occured when get subnet of {subnet}, err: {e}, "
                     f"stack: {traceback.format_exc()} ")
        return None, None


def to_list(s, sep=',', raise_error=True, fn=None):

    _list = []
    if isinstance(s, (list, tuple)):
        _list = list(s)

    elif isinstance(s, str):
        _list = s.split(sep)

    else:
        if raise_error:
            raise ValueError(f"cannot convert {s} to list")
        else:
            return _list

    try:
        if callable(fn) and _list:
            _list = list(map(fn, _list))

    except Exception as e:
        logger.error(f'got error when to_list, err={e}')
        if raise_error:
            raise e

    return _list


class Regions:
    # https://www.iso.org/glossary-for-iso-3166.html

    HK = 'HK'
    MO = 'MO'
    TW = 'TW'
    CN = 'CN'
    EU = 'EU'
    ZZ = 'ZZ'  # User-assigned codes
    UNKNOWN = 'unknown'

    FOCUSED_REGIONS = {HK, MO, TW}

    @classmethod
    def is_focused_region(cls, cc):
        return cc in cls.FOCUSED_REGIONS

    @classmethod
    def is_hidden_region(cls, cc):
        return cc == cls.EU

    @classmethod
    def is_unknown(cls, cc):
        return cc.lower() == cls.UNKNOWN


def get_b_subnet_v4(s):
    a = int(IPv4Address(s))
    return a & int(IPv4Address('255.255.0.0'))


def get_b_subnet_v4_str(s):
    _i = str(IPv4Address(int(IPv4Address(s)) & int(IPv4Address('255.255.0.0'))))
    return f'{_i}/16'


def get_b_subnets_v4(s, count):
    step = 2 ** 16
    bs = []

    start = get_b_subnet_v4(s)
    n = int(count / step)

    for i in range(n):
        bs.append(start + i)

    return bs


def ip_to_str(ip, v=4):
    # todo only for frontend showing
    if str(v) == '4':
        return str(IPv4Address(ip))

    else:
        return str(IPv6Address(ip))


def str_to_int_v4(i):
    return int(IPv4Address(i))


def str_to_exploded_ipv6(s):
    return ip_address(s).exploded


def extract_limit_offset_from_args(args):
    _limit = args.page_size if args.page_size > 0 else 30
    _offset = (args.page - 1) * _limit if args.page > 0 else 0

    return _limit, _offset


def strip_list(_list: list) -> list:
    # note: _list cannot contain None

    prev = None
    s = []

    for _e in _list:
        if _e == prev:
            continue

        s.append(_e)
        prev = _e
    return s


def strip_and_addup_on_list(_list: list, key: str) -> list:
    prev = None
    l = []
    return []


def to_int(s):
    try:
        return int(s), True
    except:
        return None, False


def to_str_list(_list):
    return list(map(str, _list))


ISO_TIME_FMT = '%Y-%m-%d %H:%M:%S'


def timestring_to_timestamp(string, fmt=ISO_TIME_FMT):
    dt = datetime.strptime(string, fmt)
    return time.mktime(dt.timetuple())


def iter_slice(array, step):
    idx = 0
    num = len(array)
    while idx < num:
        yield array[idx:idx + step]
        idx += step
