import os
import time
import logging
import traceback
import importlib
from fastapi.responses import (
    JSONResponse,
)
from fastapi import (
    FastAPI,
    APIRouter,
    Request,
    status,
)
from starlette.middleware.base import (
    BaseHTTPMiddleware,
    RequestResponseEndpoint
)
from fastapi.middleware.gzip import GZipMiddleware
from fastapi_profiler import PyInstrumentProfilerMiddleware
from extensions import (
    cache,
    mongo
)
from config import Config
from ip.views import _ip_space_init
from logs import configure_logs


class Tags:

    metadata = [
        {
            "name": "IP",
            "description": "endpoints for IP API",
        },
        {
            "name": "AS",
            "description": "endpoints for AS API",
        },
    ]


app = FastAPI(
    title='app',
    openapi_url='/api/v1/openapi.json',
    redoc_url='/api/v1/redoc',
    description="my fastapi service",
    openapi_tags=Tags.metadata,
)

logger = logging.getLogger('app')


# only support 'http'
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response


class ExceptionMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> JSONResponse:

        try:
            response = await call_next(request)

        except Exception as e:
            logger.exception(f"got exception {e}, stack: {traceback.format_exc()}")
            response = JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={"message": "internal error", "status": "bad"},
            )

        return response


app.add_middleware(ExceptionMiddleware)
app.add_middleware(GZipMiddleware, minimum_size=1000)

if str(os.getenv('APP_PPROF', 'NO')).lower() == 'yes':
    print('enable pprof monitor')
    app.add_middleware(
        PyInstrumentProfilerMiddleware,
        server_app=app,
        profiler_output_type="html",
        is_print_each_request=True,
        open_in_browser=False,
        html_file_name="example_profile.html"
    )


def configure_database():
    conf = Config.dict()
    logger.debug(f"Config mode={Config.MODE}")

    cache.load_config(conf['CACHE_MAP'])
    mongo.load_config(conf['MONGO_MAP'])


def configure_mail():
    pass


def configure_routers():
    endpoints = os.getenv('APP_ENDPOINTS', '')

    if not endpoints:
        return

    for endpoint in endpoints.split(','):

        if ':' not in endpoint:
            router_path = endpoint
            router = 'router'
        else:
            router_path, router = endpoint.split(':', 1)

        try:
            pkg = importlib.import_module(router_path)
            router_instance = getattr(pkg, router)
            assert isinstance(router_instance, APIRouter)

            logger.info(f'loading endpoint: {router_path}:{router}')
            app.include_router(router_instance, prefix='/api/v1')

        except Exception as e:
            logger.exception(e)
            raise e


def config_app():
    configure_logs()
    configure_mail()
    configure_routers()
    configure_database()


config_app()


if str(os.getenv('APP_SPACE_INIT', 'yes')).lower() == 'yes':
    print('init space ...')

    @app.on_event('startup')
    async def ip_space_init():
        await _ip_space_init()
