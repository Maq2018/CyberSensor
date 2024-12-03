from aiobotocore.session import (
    get_session,
    ClientCreatorContext,
    AioSession,
)
from typing import Optional

from .base import ConnectionMap


class S3Session:

    def __init__(self, session: AioSession, options: dict):
        self._session = session
        self._options = options
        self._client: Optional[ClientCreatorContext] = None

    async def __aenter__(self):
        self._client = self._session.create_client('s3', **self._options)
        return await self._client.__aenter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._client.__aexit__(exc_type, exc_val, exc_tb)


class S3Connection(ConnectionMap):

    def create_connection(self, config: dict) -> S3Session:
        session: AioSession = get_session()
        return S3Session(session, config)

