import os
from pydantic import BaseSettings
from typing import Dict, Any, Optional
import logging


class Settings(BaseSettings):
    MODE = 'PROD'

    CACHE_MAP: Dict[str, Any] = dict(
        default=dict(
            TYPE='mem',
            PREFIX='vis-',
        )
    )

    MONGO_MAP: Dict[str, Any] = dict(
        default=dict(
            DATABASE="db",
            USERNAME="qnx",
            PASSWORD="rkRkbE6Ui73LMnTVTpygvXqX",
            READ_PREFERENCE="SECONDARY_PREFERRED",
            MAX_POOL_SIZE=20,
            HOSTS=['58.206.248.145:52234'],
            AUTH_SOURCE="admin",
            ASYNC=True,
        ),
        default_sync=dict(
            DATABASE="db",
            USERNAME="qnx",
            PASSWORD="rkRkbE6Ui73LMnTVTpygvXqX",
            READ_PREFERENCE="SECONDARY_PREFERRED",
            MAX_POOL_SIZE=20,
            HOSTS=['58.206.248.145:52234'],
            AUTH_SOURCE="admin",
            ASYNC=False,
        ),
    )

    IPv4_ALLOC_END: int = 20230710
    IPv6_ALLOC_END: int = 20230710

    LOG_LEVEL: int = logging.DEBUG
    LOG_STDOUT: bool = False
    LOG_SYSLOG_ENDPOINT: Optional[tuple[str, int, int]] = None  # ip, port, msg_size

    LOG_FMT: str = '[%(asctime)s %(name)s:%(lineno)d] %(levelname)s: %(message)s'
    LOG_DATE_FMT: str = "%Y-%m-%d %H:%M:%S"
    LOG_PATH: str = '../app.log'


class DevSettings(BaseSettings):
    MODE = 'DEV'

    CACHE_MAP: Dict[str, Any] = dict(
        default=dict(
            TYPE='mem',
            PREFIX='vis-',
        )
    )

    MONGO_MAP: Dict[str, Any] = dict(
        default=dict(
            DATABASE="db",
            READ_PREFERENCE="SECONDARY_PREFERRED",
            MAX_POOL_SIZE=20,
            HOSTS=['localhost:27017'],
            ASYNC=True,
        ),
        default_sync=dict(
            DATABASE="db",
            READ_PREFERENCE="SECONDARY_PREFERRED",
            MAX_POOL_SIZE=20,
            HOSTS=['localhost:27017'],
            ASYNC=False,
        ),
    )

    CLICKHOUSE_MAP: Dict[str, Any] = dict(
        default=dict(
            URL="http://localhost:7070",
            USER="default",
            PASSWORD="123456",
            DATABASE="default",
            COMPRESS_RESPONSE="lz4",
            TIMEOUT=30,
        )
    )

    """
    service_name: str
    region_name: str
    api_version: str
    use_ssl: bool
    verify
    endpoint_url: str
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_session_token: str
    """
    S3_MAP: Dict[str, Any] = dict(
        default=dict(
            ENDPOINT_URL="http://127.0.0.1:9000",
            AWS_ACCESS_KEY_ID="IjUEz089OqT2F7R6RuEx",
            AWS_SECRET_ACCESS_KEY="VOTvGEY14WPGGj1ItA6lhN1UwjT2RcoHEpt79WYu",
        )
    )

    IPv4_ALLOC_END: int = 20230710
    IPv6_ALLOC_END: int = 20230710

    LOG_LEVEL: int = logging.DEBUG
    LOG_STDOUT: bool = False
    LOG_SYSLOG_ENDPOINT: Optional[tuple[str, int, int]] = None  # ip, port, msg_size

    LOG_FMT: str = '[%(asctime)s %(name)s:%(lineno)d] %(levelname)s: %(message)s'
    LOG_DATE_FMT: str = "%Y-%m-%d %H:%M:%S"
    LOG_PATH: str = '../app.log'


def get_config():
    if os.getenv('APP_MODE', 'PROD') == 'PROD':
        return Settings()

    return DevSettings()


Config = get_config()
