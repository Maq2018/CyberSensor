#!/bin/bash

NAME=vis-api
DIR=/home/qnx/zgc-vis
USER=qnx
GROUP=qnx
VENV=$DIR/.venv/bin/activate

cd $DIR/src
source $VENV
export PYTHONPATH=$DIR/src APP_ENDPOINTS=asn APP_SPACE_INIT=no APP_MODE=PROD

exec uvicorn app:app \
    --host 0.0.0.0 \
    --port 22223
