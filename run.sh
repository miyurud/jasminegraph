#!/usr/bin/env bash

JASMINEGRAPH_HOME="`dirname \"$0\"`"
MODE=$1;
SERVER_PORT=$2;
SERVER_DATA_PORT=$3;

JASMINEGRAPH_HOME="`( cd \"$JASMINEGRAPH_HOME\" && pwd )`"
if [ -z "$JASMINEGRAPH_HOME" ] ;
then
    exit 1  # fail
fi

if [ -z "$MODE" ] ;
then
    echo "MODE OF OPERATION SHOULD BE SPECIFIED"
    echo "Use argument 1 to start JasmineGraph in Master mode."
    echo "Use 2 <serverPort> <serverDataPort> to start in Worker mode."
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
    echo "using mode $SERVER_PORT"
    echo "using mode $SERVER_DATA_PORT"
fi

cd $JASMINEGRAPH_HOME

./JasmineGraph $MODE $SERVER_PORT $SERVER_DATA_PORT
