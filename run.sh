#!/usr/bin/env bash

JASMINEGRAPH_DIR="`dirname \"$0\"`"
PROFILE=$1;
MODE=$2;

JASMINEGRAPH_DIR="`( cd \"$JASMINEGRAPH_DIR\" && pwd )`"
if [ -z "$JASMINEGRAPH_DIR" ] ;
then
    exit 1  # fail
fi

export JASMINEGRAPH_HOME="$JASMINEGRAPH_DIR"

if [ -z "$MODE" ] ;
then
    echo "MODE OF OPERATION SHOULD BE SPECIFIED"
    echo "Use argument 1 to start JasmineGraph in Master mode."
    echo "Use 2 <hostName> <serverPort> <serverDataPort> to start in Worker mode."
    exit 1
fi

cd $JASMINEGRAPH_DIR

if [ $MODE -eq 1 ] ;
then
    echo "STARTING MASTER MODE"

    MASTER_HOST_NAME=$3;
    NUMBER_OF_WORKERS=$4;
    WORKER_IPS=$5;

    ./JasmineGraph "native" $MODE $MASTER_HOST_NAME $NUMBER_OF_WORKERS $WORKER_IPS
else
    WORKER_HOST=$3;
    MASTER_HOST_NAME=$4;
    SERVER_PORT=$5;
    SERVER_DATA_PORT=$6;

    if [ -z "$SERVER_PORT" ] ;
    then
        echo "SERVER PORT SHOULD BE SPECIFIED"
        exit 1
    fi

    if [ -z "$SERVER_DATA_PORT" ] ;
        then
            echo "SERVER DATA PORT SHOULD BE SPECIFIED"
            exit 1
    fi

    ./JasmineGraph "native" $MODE $WORKER_HOST $MASTER_HOST_NAME $SERVER_PORT $SERVER_DATA_PORT
fi

