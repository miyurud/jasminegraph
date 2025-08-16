#!/bin/bash

# Variables
HOST="localhost"
PORT="7777"

# Establish telnet connection and send command
(
    sleep 5
    echo "shdn"
    sleep 5
    echo "exit"
    sleep 5
) | telnet "$HOST" "$PORT"

docker compose stop kafka prometheus pushgateway &
