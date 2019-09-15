#!/usr/bin/env bash

JASMINEGRAPH_DIR="`dirname \"$0\"`"
MODE=$1;
HOST_NAME=$2;
SERVER_PORT=$3;
SERVER_DATA_PORT=$4;

echo "using mode $MODE"
echo "using server $HOST_NAME"
echo "using server port $SERVER_PORT"
echo "using server data port $SERVER_DATA_PORT"

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

if [ $MODE -eq 1 ] ;
then
    echo "STARTING MASTER MODE"
else
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
fi

echo "using mode $MODE"
if [ $MODE -eq 1 ] ;
then
    echo "using server $HOST_NAME"
    echo "using mode $SERVER_PORT"
    echo "using mode $SERVER_DATA_PORT"
fi
mkdir -p logs


cd $JASMINEGRAPH_DIR

./JasmineGraph $MODE $HOST_NAME $SERVER_PORT $SERVER_DATA_PORT
