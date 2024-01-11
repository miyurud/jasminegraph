#!/bin/bash

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

# Set default value for PROFILE if not provided
if [ -z "$PROFILE" ]; then
    PROFILE="docker"
fi

while [ $# -gt 0 ]; do

    if [[ $1 == *"--"* ]]; then
        param="${1/--/}"
        declare $param="$2"
        echo $1 $2 // Optional to see the parameter:value result
    fi

    shift
done

if [ -z "$MODE" ]; then
    echo "MODE OF OPERATION SHOULD BE SPECIFIED"
    echo "Use argument 1 <MASTERIP> <NUMBER OF WORKERS> <WORKER IPS> to start JasmineGraph in Master mode."
    echo "Use 2 <hostName> <serverPort> <serverDataPort> to start in Worker mode."
    exit 1
fi

if [ $MODE -eq 1 ]; then
    # If profile is k8s, then master and worker IPs are not required. Set them to dummy values
    if [ "$PROFILE" = "k8s" ]; then
        MASTERIP="x"
        WORKERIP="x"
    fi

    if [ -z "$MASTERIP" ]; then
        echo "MASTER IP SHOULD BE SPECIFIED"
        exit 1
    fi

    if [ -z "$WORKERS" ]; then
        echo "Number of workers SHOULD BE SPECIFIED"
        exit 1
    fi

    if [ -z "$WORKERIP" ]; then
        echo "Worker IPs SHOULD BE SPECIFIED"
        exit 1
    fi
else
    if [ -z "$MASTERIP" ]; then
        echo "MASTER IP SHOULD BE SPECIFIED"
        exit 1
    fi

    if [ -z "$SERVER_PORT" ]; then
        echo "SERVER PORT SHOULD BE SPECIFIED"
        exit 1
    fi

    if [ -z "$SERVER_DATA_PORT" ]; then
        echo "SERVER DATA PORT SHOULD BE SPECIFIED"
        exit 1
    fi
fi

if [ ! -d /tmp/jasminegraph ]; then
    mkdir /tmp/jasminegraph
fi

export LD_LIBRARY_PATH=/usr/local/lib
if [ -n "$DEBUG" ]; then
    if [ $MODE -eq 1 ]; then
        gdbserver 0.0.0.0:$DEBUG ./JasmineGraph $PROFILE $MODE $MASTERIP $WORKERS $WORKERIP $ENABLE_NMON
    else
        gdbserver 0.0.0.0:$DEBUG ./JasmineGraph $PROFILE $MODE $HOST_NAME $MASTERIP $SERVER_PORT $SERVER_DATA_PORT $ENABLE_NMON
    fi
else
    if [ $MODE -eq 1 ]; then
        ./JasmineGraph $PROFILE $MODE $MASTERIP $WORKERS $WORKERIP $ENABLE_NMON
    else
        ./JasmineGraph $PROFILE $MODE $HOST_NAME $MASTERIP $SERVER_PORT $SERVER_DATA_PORT $ENABLE_NMON
    fi
fi

if [ "$TESTING" = "true" ]; then
    chmod -R go+w /tmp/jasminegraph
fi
