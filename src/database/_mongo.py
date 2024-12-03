import logging
from pymongo import ReadPreference, MongoClient
from motor.motor_asyncio import AsyncIOMotorClient

from .base import ConnectionMap

logger = logging.getLogger('database.mongo')


class MongoConnection(ConnectionMap):

    DEFAULT_MAX_POOL_SIZE: int = 50
    DEFAULT_SOCKET_TIMEOUT_MS: int = 60 * 1000  # 60 seconds
    DEFAULT_CONNECT_TIMEOUT_MS: int = 30 * 1000  # 30 seconds
    DEFAULT_READ_PREFERENCE: str = "SECONDARY_PREFERRED"
    DEFAULT_AUTH_SOURCE: str = "admin"

    def _get_readpreference(self, read_preference) -> str:
        if not hasattr(ReadPreference, read_preference):
            raise Exception("No such read preference name (%r)" %
                            read_preference)

        # SECONDARY_PREFERRED to secondaryPreferred
        read_preference = read_preference.title().replace('_', '')
        return read_preference[0].lower() + read_preference[1:]

    def create_connection(self, config: dict) -> AsyncIOMotorClient:
        hosts = config['hosts']
        if isinstance(hosts, list):
            hosts = ",".join(hosts)

        if config.get('username') and config.get('password'):
            uri = "mongodb://%s:%s@%s" % (
                config['username'],
                config['password'],
                hosts)

        else:
            uri = "mongodb://%s" % hosts

        read_preference = self._get_readpreference(
            config.get('read_preference', self.DEFAULT_READ_PREFERENCE))

        max_pool_size = config.get(
            'max_pool_size', self.DEFAULT_MAX_POOL_SIZE)

        socket_timeout_ms = config.get(
            'socket_timeout_ms', self.DEFAULT_SOCKET_TIMEOUT_MS)

        connect_timeout_ms = config.get(
            'connect_timeout_ms', self.DEFAULT_CONNECT_TIMEOUT_MS)

        replica_set = config.get('replica_set', None)

        auth_source = config.get('auth_source', self.DEFAULT_AUTH_SOURCE)

        is_aysnc = config.get('async', False)

        options = dict(
            readPreference=read_preference,
            maxPoolSize=max_pool_size,
            socketTimeoutMS=socket_timeout_ms,
            connectTimeoutMS=connect_timeout_ms,
            replicaSet=replica_set,
            authSource=auth_source,
        )

        try:
            if is_aysnc:
                return AsyncIOMotorClient(uri, **options)
            else:
                return MongoClient(uri, **options)

        except Exception as e:
            logger.error('failed to connect to [%s], exception: %s', hosts, e)
            raise Exception('Failed to connect to %s' % hosts)
