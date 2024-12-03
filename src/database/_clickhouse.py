from .base import ConnectionMap
from aiochclient import ChClient
from aiohttp import ClientSession
from typing import Optional


class ChSession:

    def __init__(self, options: dict):
        self._options = options
        self._client: Optional[ChClient] = None
        self._span = None

    async def __aenter__(self):
        session = ClientSession()
        self._client = ChClient(session, **self._options)
        return self._client

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._client.__aexit__(exc_type, exc_val, exc_tb)


class ClickhouseConnection(ConnectionMap):

    def create_connection(self, config: dict) -> ChSession:
        return ChSession(config)
