#!/bin/bash

PROJECT_ROOT="$(pwd)"
mkdir -p logs/

run_id="$(date +%y%m%d_%H%M%S)"

stop_and_remove_containers () {
    docker ps -q | xargs docker kill &>/dev/null
    docker container prune -f &>/dev/null
}

build_and_run_docker () {
    stop_and_remove_containers
    cd "$PROJECT_ROOT"
    docker build -t jasminegraph:test . |& tee "logs/${run_id}_build.txt"
    docker compose -f tests/docker-compose.yml up |& tee "logs/${run_id}_run.txt" &
}

rm -rf tests/env
cp -r tests/env_init tests/env
build_and_run_docker &>/dev/null

# sleep until server starts listening
while ! nc -zvn 127.0.0.1 7777 &>/dev/null; do
    sleep .2
done

timeout 1800 python3 -u tests/test.py |& tee "logs/${run_id}_test.txt"
exit_code="${PIPESTATUS[0]}"
rm -rf tests/env
stop_and_remove_containers
exit "$exit_code"