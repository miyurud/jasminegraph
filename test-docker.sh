#!/bin/bash
set -ex
PROJECT_ROOT="$(pwd)"
TIMEOUT_SECONDS=600
run_id="$(date +%y%m%d_%H%M%S)"

mkdir -p logs/

stop_and_remove_containers () {
    if [ "$(docker ps -q)" ]; then
       docker ps -a -q | xargs docker rm -f &>/dev/null
    else
        echo "No containers to stop and remove."
    fi    
}

build_and_run_docker () {
    stop_and_remove_containers
    cd "$PROJECT_ROOT"
    docker build -t jasminegraph:test . |& tee "logs/${run_id}_build.txt"
    docker compose -f tests/integration/docker-compose.yml up |& tee "logs/${run_id}_run.txt" &
}

cd tests/integration
rm -rf env
cp -r env_init env
cd "$PROJECT_ROOT"
build_and_run_docker #&>/dev/null

# sleep until server starts listening
while ! nc -zvn 127.0.0.1 7777 &>/dev/null; do
    sleep .2
done

timeout "${TIMEOUT_SECONDS}" python3.11 -u tests/integration/test.py |& tee "logs/${run_id}_test.txt"
exit_code="${PIPESTATUS[0]}"
rm -rf tests/integration/env
stop_and_remove_containers
exit "$exit_code"
