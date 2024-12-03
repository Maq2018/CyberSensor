from fastapi import APIRouter
import logging
from database.models import CacheSelector


router = APIRouter(prefix='/health')
logger = logging.getLogger()


@router.get('/ping')
def ping():
    return {'data': 'pong', 'status': 'ok', 'message': ''}


@router.get('/mem')
async def get_mem():
    _cache = CacheSelector.get_cache()
    _backend = _cache._get_backend('')
    _data = []
    for _k, _v in _backend.store.items():
        _data.append({'key': _k, 'expire': await _backend.get_expire(_k)})

    return {'data': _data, 'size': _backend.size,
            'status': 'ok', 'message': ''}
