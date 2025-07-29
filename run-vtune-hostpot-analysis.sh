#!/bin/bash
set -e
docker cp $(docker ps --filter "publish=7777" --format "{{.Names}}"):/home/ubuntu/software/jasminegraph/JasmineGraph .
CONTAINER_ID=$(docker ps --filter "publish=7777" --format "{{.ID}}")
PID=$(docker inspect --format '{{.State.Pid}}' "$CONTAINER_ID")
sudo nsenter -t PID -p ps aux
