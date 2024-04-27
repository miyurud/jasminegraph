#!/bin/ash

MODE=${MODE}
MASTERIP=${MASTERIP}
WORKERS=${WORKERS}
WORKERIP=${WORKERIP}
HOST_NAME=${HOST_NAME}
SERVER_PORT=${SERVER_PORT}
SERVER_DATA_PORT=${SERVER_DATA_PORT}
ENABLE_NMON=${ENABLE_NMON}
DEBUG=${DEBUG}
PROFILE=${PROFILE}

if [ -z "$PROFILE" ]; then
    PROFILE="k8s"
fi

while [ $# -gt 0 ]; do

    if [[ $1 == *"--"* ]]; then
        param="${1/--/}"
        export $param="$2"
        echo $1 $2
    fi

    shift
done

if [ -z "$MODE" ]; then
    exit 1
fi

if [ $MODE -eq 1 ]; then
    if [ "$PROFILE" = "k8s" ]; then
        MASTERIP="x"
        WORKERIP="x"
    fi
    if [ -z "$MASTERIP" ]; then
        exit 1
    fi
    if [ -z "$WORKERS" ]; then
        exit 1
    fi
    if [ -z "$WORKERIP" ]; then
        exit 1
    fi
else
    if [ -z "$MASTERIP" ]; then
        exit 1
    fi
    if [ -z "$SERVER_PORT" ]; then
        exit 1
    fi
    if [ -z "$SERVER_DATA_PORT" ]; then
        exit 1
    fi
fi

if [ ! -d /tmp/jasminegraph ]; then
    mkdir /tmp/jasminegraph
fi

if [ $MODE -eq 1 ]; then
    ./JasmineGraph $PROFILE $MODE $MASTERIP $WORKERS $WORKERIP $ENABLE_NMON
else
    ./JasmineGraph $PROFILE $MODE $HOST_NAME $MASTERIP $SERVER_PORT $SERVER_DATA_PORT $ENABLE_NMON
fi

if [ "$TESTING" = "true" ]; then
    chmod -R go+w /tmp/jasminegraph
fi
