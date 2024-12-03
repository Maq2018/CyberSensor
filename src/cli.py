import click
import os
import logging
import time
import json
import requests

from database.services import _bulk_load
from utils.misc import (
    cmd,
    multiprocess_fn,
    split_file,
    iter_slice
)
from app import (
    configure_logs
)
from ip.models import (
    VisIPv4Alloc,
    VisIPv6Alloc,
    VisASAlloc,
    VisIPNetflow,
    VisIPv4Picture,
    VisIPv6Picture
)
from ip.services import _get_diff_date
from database.models import TableSelector
from asn.models import (
    VisEduASHistory,
    VisEduASCityLocation,
    VisAS,
    VisHijackEvent,
    VisEduASPath,
    VisASHijackSimpleEvent
)
from asn.services import load_all_ases_country
from config import Config


logger = logging.getLogger('cli')


@click.group()
def endpoint():
    pass


@endpoint.group(name='alloc')
def ip():
    pass


# todo delete or not
# todo remove ZZ related data and date value with null value
def _load_alloc_file(_file, clean_file=False):
    logger.info(f'loading {_file} ...')
    _ipv4 = []
    _ipv6 = []
    _asn = []
    step = 300

    _table_v4 = TableSelector.get_prefix_alloc_table(4)
    _table_v6 = TableSelector.get_prefix_alloc_table(6)
    _table_asn = TableSelector.get_asn_alloc_table()

    # logger.info('going to delete old data ...')
    # _table_v4.delete_many({})
    # _table_v6.delete_many({})
    # _table_asn.delete_many({})

    idx = 0
    with open(_file, 'r') as fd:
        for line in fd:
            if '240e:2000::' in line:
                pass

            if line.startswith('#'):
                continue

            idx += 1
            if idx < 5:
                # skip version and summary lines
                continue

            item = VisIPv4Alloc.to_item(line)
            # remove item with 0 date
            # item_date = int(item.get('date', '0'))
            # if item_date == 0:
            #     continue
            if item['type'] == 'ipv4':
                _ipv4.append(VisIPv4Alloc.to_mongo(item))

                if len(_ipv4) >= step:
                    _bulk_load(_table_v4, _ipv4)
                    _ipv4 = []

            elif item['type'] == 'ipv6':
                _ipv6.append(VisIPv6Alloc.to_mongo(item))

                if len(_ipv6) >= step:
                    _bulk_load(_table_v6, _ipv6)
                    _ipv6 = []

            else:
                _asn.append(VisASAlloc.to_mongo(item))

                if len(_asn) >= step:
                    _bulk_load(_table_asn, _asn)
                    _asn = []

    if _ipv6:
        _bulk_load(_table_v6, _ipv6)

    if _ipv4:
        _bulk_load(_table_v4, _ipv4)

    if _asn:
        _bulk_load(_table_asn, _asn)

    logger.info(f'finished loading {_file}')

    if clean_file:
        cmd(['rm', '-rf', _file])


@ip.command('import')
@click.option('--file-dir', '-d', type=click.Path(exists=True), required=True)
@click.option('--worker', '-w', type=int)
def load_ip_alloc(file_dir, worker):
    print('going to load alloc files ....')
    tick = time.time()

    if worker < 1:
        worker = 1

    for _file in os.listdir(file_dir):

        _file_path = os.path.join(file_dir, _file)
        print(f'loading {_file_path}')
        logger.info(f'loading {_file_path}')

        if worker > 1:
            _alloc_files = split_file(_file_path, worker)
            args = [(_a, True) for _a in _alloc_files]
            multiprocess_fn(_load_alloc_file, args)

        else:

            _load_alloc_file(_file_path)

    elapsed = time.time() - tick
    print(f'finished loading with elapsed={elapsed}, check log file'
          f' {Config.LOG_PATH} for more information')


def curl_file(url, save_dir):
    url = url.strip('\n')
    # s = 'curl %s --output %s/%s' % (url, save_dir, os.path.basename(url))
    cmd(['curl', url, '--output', '%s/%s' % (save_dir, os.path.basename(url))])


@ip.command('fetch')
@click.option('--save-dir', '-d', type=click.Path(exists=True), required=True)
@click.option('--url-file', '-u', type=click.Path(exists=True))
def fetch_ip(save_dir, url_file):
    configure_logs()

    if not url_file:
        urls = [
            'http://ftp.apnic.net/stats/apnic/delegated-apnic-extended-latest',
            'https://ftp.ripe.net/pub/stats/ripencc/delegated-ripencc-extended-latest',
            'http://ftp.arin.net/pub/stats/arin/delegated-arin-extended-latest',
            'http://ftp.afrinic.net/stats/afrinic/delegated-afrinic-extended-latest',
            'http://ftp.lacnic.net/pub/stats/lacnic/delegated-lacnic-extended-latest'
        ]

    else:
        with open(url_file, 'r') as fd:
            urls = fd.readlines()

    if not urls:
        print("no urls found")
        exit(1)

    args = []
    for url in urls:
        args.append((url, save_dir))

    multiprocess_fn(curl_file, args)


@endpoint.group(name='as-info')
def as_():
    pass


@endpoint.group(name='cernet-history')
def edu():
    pass


def _load_edu_history_file(_file):
    # todo need remove all data when insert
    logger.info(f'loading {_file} ...')
    step = 50
    _edu_table = TableSelector.get_edu_as_history_table()

    logger.info('going to delete old data ...')
    _edu_table.delete_many({})

    abroad = 'abroad' in os.path.basename(_file)
    ops = []
    idx = 0

    with open(_file, 'r') as fd:
        for line in fd:
            idx += 1

            if not line:
                continue

            if idx < 2:
                continue

            item = VisEduASHistory.to_item(line)
            ops.append(VisEduASHistory.to_mongo(item, abroad))

            if len(ops) >= step:
                _bulk_load(_edu_table, ops)
                ops = []

    if ops:
        _bulk_load(_edu_table, ops)

    logger.info(f'finished loading {_file}')


def _load_file_batch(_file, load_op_fn, write_table, skip_lines=None, step=300, clean_file=False):
    logger.info(f'going to load {_file} ...')

    idx = 0
    ops = []

    with open(_file, 'r') as fd:
        for line in fd:
            idx += 1

            if skip_lines is not None and idx <= skip_lines:
                continue

            if not line:
                continue

            ops.append(load_op_fn(line))

            if len(ops) >= step:
                _bulk_load(write_table, ops)
                ops = []

    if ops:
        _bulk_load(write_table, ops)

    if clean_file:
        cmd(['rm', '-rf', _file])

    logger.info(f'finished loading {_file}')


@edu.command('import')
@click.option('--file-dir', '-d', type=click.Path(exists=True), required=True)
def load_edu_history(file_dir):
    print('going to load edu history files ....')
    tick = time.time()

    for _file in os.listdir(file_dir):

        _file_path = os.path.join(file_dir, _file)
        print(f'loading {_file_path}')
        logger.info(f'loading {_file_path}')

        _load_edu_history_file(_file_path)

    elapsed = time.time() - tick
    print(f'finished loading with elapsed={elapsed}, check log file'
          f' {Config.LOG_PATH} for more information')


def _load_as_file(_file, clean_file=False):
    _table = TableSelector.get_as_table()
    _load_file_batch(_file, VisAS.to_op, _table, clean_file=clean_file)


@as_.command('import')
@click.option('--path', '-p', type=click.Path(exists=True))
@click.option('--worker', '-w', type=int)
def load_as(path, worker):
    print('going to load as alloc file ....')
    tick = time.time()

    if worker > 1:
        _alloc_files = split_file(path, worker)
        args = [(_a, True) for _a in _alloc_files]
        multiprocess_fn(_load_as_file, args)

    else:
        _load_as_file(path)

    elapsed = time.time() - tick
    print(f'finished loading with elapsed={elapsed}, check log file'
          f' {Config.LOG_PATH} for more information')


@endpoint.group(name='cernet-path')
def as_path():
    pass


def _load_as_path_file(_file, clean_file=False):
    _table = TableSelector.get_edu_as_path_table()
    _load_file_batch(_file, VisEduASPath.to_op, _table, clean_file=clean_file)


@as_path.command('import')
@click.option('--path', '-p', type=click.Path(exists=True))
@click.option('--worker', '-w', type=int)
def load_as_path(path, worker):
    print('going to load as path file ...')
    tick = time.time()

    if worker > 1:
        _as_path_files = split_file(path, worker)
        args = [(_a, True) for _a in _as_path_files]
        multiprocess_fn(_load_as_path_file, args)
    else:
        _load_as_path_file(path)

    elapsed = time.time() - tick
    print(f'finished loading with elapsed={elapsed}, check log file'
          f' {Config.LOG_PATH} for more information')


@endpoint.group(name='city-location')
def city():
    pass


def _load_city_file(_file):
    _load_file_batch(_file, load_op_fn=VisEduASCityLocation.to_op,
                     write_table=TableSelector.get_edu_as_city_table(),
                     step=50, skip_lines=1)


@city.command('import')
@click.option('--path', '-p', type=click.Path(exists=True))
def load_city(path):
    print(f'going to load city file ....')
    tick = time.time()

    print(f'loading {path}')
    _load_city_file(path)
    elapsed = time.time() - tick
    print(f'finished loading with elapsed={elapsed}, check log file'
          f' {Config.LOG_PATH} for more information')


@endpoint.group(name='hijack')
def hijack():
    pass


def _load_hijack_file(_file):
    print(f'going to load {_file}....')

    ops = []
    step = 50
    _table = TableSelector.get_as_hijack_table(name='default_sync')
    _ases_cc_map = load_all_ases_country()

    with open(_file, 'r') as fd:
        js = json.loads(fd.read().strip('\n '))
        print(f'going to load {len(js)} items')
        for item in js:
            ops.append(VisHijackEvent.to_mongo(item, _ases_cc_map))

            if len(ops) >= step:
                _bulk_load(_table, ops)
                ops = []

    if ops:
        _bulk_load(_table, ops)

    logger.info(f'finished loading {_file} ...')


@hijack.command('import')
@click.option('--path', '-p', type=click.Path(exists=True))
def load_hijack(path):
    print(f'going to load hijack file ...')
    tick = time.time()

    _load_hijack_file(path)
    elapsed = time.time() - tick
    print(f'finished loading with elapsed={elapsed}, check log file'
          f' {Config.LOG_PATH} for more information')


@endpoint.group(name='simple-hijack')
def simple_hijack():
    pass


def _load_simple_hijack_file(_file):
    logger.info(f'going to load {_file}')
    ops = []
    step = 50
    cc_map = load_all_ases_country()

    _table = TableSelector.get_as_simple_hijack_table()
    with open(_file, 'r') as fd:
        for line in fd:
            if not line:
                continue

            ops.append(VisASHijackSimpleEvent.to_op(line, cc_map))
            if len(ops) >= step:
                _bulk_load(_table, ops)
                ops = []

    if ops:
        _bulk_load(_table, ops)

    logger.info(f'finished loading {_file}')


@simple_hijack.command('import')
@click.option('--path', '-p', type=click.Path(exists=True))
def load_simple_hijack(path):
    print(f'going to load simple hijack file ...')
    tick = time.time()
    _load_simple_hijack_file(path)

    elapsed = time.time() - tick
    print(f'finished loading with elapsed={elapsed}, check log file'
          f' {Config.LOG_PATH} for more information')


@endpoint.group(name='cernet-path-result')
def cernet_path():
    pass


@cernet_path.command('import')
@click.option('--path', '-p', type=click.Path(exists=True))
def load_cernet_path_result(path):
    """
        data = {'data': {'paths': paths, 'dependencies': point_map}, 'status': 'ok', 'message': ''}
    :param path:
    :return:
    """
    with open(path, 'r') as fd:
        obj = json.loads(fd.read())
        if not obj:
            print('no object found')
            return

        _table = TableSelector.get_as_cache_table()
        data = {'data': obj, 'status': 'ok', 'message': ''}
        _table.update_one({'key': 'as/cernet/path'}, {'$set': data}, upsert=True)


@endpoint.group(name='netflow')
def netflow():
    pass


def _load_netflow_file(_files, _dir):
    logger.debug(f'going to load netflow files with num={len(_files)}, dir={_dir}, pid={os.getpid()}')

    _table = TableSelector.get_ip_netflow_table()
    ops = []
    step = 200

    for _file in _files:
        if not _file.endswith('.csv'):
            logger.debug(f'file is not csv, will not load, file={_file}')
            continue

        idx = 0
        _path = os.path.join(_dir, _file)

        with open(_path) as fd:
            items = []
            for line in fd:
                idx += 1
                if idx < 2:
                    continue

                items.append(VisIPNetflow.to_item(line))

            ops += VisIPNetflow.to_ops(_file[:-4], items)

        if len(ops) >= step:
            _bulk_load(_table, ops)
            ops = []

    if ops:
        _bulk_load(_table, ops)

    logger.debug(f'finish loading netflow files with num={len(_files)}, dir={_dir}, pid={os.getpid()}')


@netflow.command('import')
@click.option('--file-dir', '-d', type=click.Path(exists=True))
@click.option('--worker', '-w', type=int)
def load_netflow_file(file_dir, worker):
    print(f"going to load netflow file from {file_dir}")

    _files = os.listdir(file_dir)

    if not _files:
        return

    n = len(_files)

    if worker <= 1:
        _load_netflow_file(_files, file_dir)

    else:
        if n > worker:
            step = int(n / worker)
        else:
            step = 1

        args = []
        for _fs in iter_slice(_files, step):
            args.append((_fs, file_dir))

        multiprocess_fn(_load_netflow_file, args)


@endpoint.group(name='ip-alloc-map')
def ip_alloc_map():
    pass


@ip_alloc_map.command('write')
@click.option('--host', '-h', type=str)
@click.option('--port', '-p', type=int)
@click.option('--protocol', '-t', type=str)
def write_ip_alloc_map(host, port, protocol):

    ipv4_time_range = [19810101, 20240101]
    ipv6_time_range = [19980101, 20240101]

    for _date in _get_diff_date(ipv6_time_range[0], ipv6_time_range[1]):
        url = f'{protocol}://{host}:{port}/api/v1/ip/map?v=6&date={_date}'
        print(url)
        requests.get(url)

    for _date in _get_diff_date(ipv4_time_range[0], ipv4_time_range[1]):
        url = f'{protocol}://{host}:{port}/api/v1/ip/map?v=4&date={_date}'
        print(url)
        requests.get(url)


@endpoint.group(name='ipv4-space')
def ipv4_space():
    pass


@ipv4_space.command('write')
@click.option('--host', '-h', type=str)
@click.option('--port', '-p', type=int)
@click.option('--protocol', '-t', type=str)
def write_ipv4_space(host, port, protocol):
    ipv4_time_range = [19821101, 20230901]
    ipv6_time_range = [19990801, 20230901]
    countries = 'US,CN,JP,DE,GB,KR,BR,FR,CA,IT'
    for _date in _get_diff_date(ipv4_time_range[0], ipv4_time_range[1]):
        url = f'{protocol}://{host}:{port}/api/v1/ip/space?v=4&date={_date}&countries={countries}'
        print(url)
        requests.get(url)

    return
    for _date in _get_diff_date(ipv6_time_range[0], ipv6_time_range[1]):
        url = f'{protocol}://{host}:{port}/api/v1/ip/space?v=6&date={_date}&countries={countries}'
        print(url)
        requests.get(url)


@endpoint.group(name='ipv4-picture')
def ipv4_picture():
    pass


def _load_ipv4_picture_file(_file, clean_file=False):
    _table = TableSelector.get_ip_picture(4)
    _load_file_batch(_file, VisIPv4Picture.to_op, _table, clean_file=clean_file)


@ipv4_picture.command('import')
@click.option('--path', '-p', type=click.Path(exists=True))
@click.option('--worker', '-w', type=int)
def load_ipv4_picture(path, worker):
    print(f'going to load ipv4 picture file ...')
    tick = time.time()

    if worker > 1:
        _files = split_file(path, worker)
        args = [(_a, True) for _a in _files]
        multiprocess_fn(_load_ipv4_picture_file, args)
    else:
        _load_ipv4_picture_file(path)

    elapsed = time.time() - tick
    print(f'finished loading with elapsed={elapsed}, check log file'
          f' {Config.LOG_PATH} for more information')


@endpoint.group(name='ipv6-picture')
def ipv6_picture():
    pass


def _load_ipv6_picture_file(_file, clean_file=False):
    _table = TableSelector.get_ip_picture(6)
    _load_file_batch(_file, VisIPv6Picture.to_op, _table, clean_file=clean_file)


@ipv6_picture.command('import')
@click.option('--path', '-p', type=click.Path(exists=True))
@click.option('--worker', '-w', type=int)
def load_ipv6_picture(path, worker):
    print(f'going to load ipv6 picture file ...')
    tick = time.time()

    if worker > 1:
        _files = split_file(path, worker)
        args = [(_a, True) for _a in _files]
        multiprocess_fn(_load_ipv6_picture_file, args)
    else:
        _load_ipv6_picture_file(path)

    elapsed = time.time() - tick
    print(f'finished loading with elapsed={elapsed}, check log file'
          f' {Config.LOG_PATH} for more information')


if __name__ == '__main__':
    endpoint()
