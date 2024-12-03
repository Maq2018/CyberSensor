import logging
from cashews import Cache

from .base import ConnectionMap


logger = logging.getLogger('database._cache')


class CacheConnection(ConnectionMap):

    def create_connection(self, config: dict) -> Cache:

        if not config.get('host'):
            url = f"{config.pop('type')}://"
        else:
            url = f"{config.pop('type')}://{config.pop('host')}:{config.pop('port')}/{config.pop('db')}"

        cache = Cache()
        cache.setup(url)
        logger.debug(f'get cache from url={url}')
        return cache
