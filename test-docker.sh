#!/bin/bash
set -ex
PROJECT_ROOT="$(pwd)"
TIMEOUT_SECONDS=600
RUN_ID="$(date +%y%m%d_%H%M%S)"

BUILD_LOG="logs/${RUN_ID}_build.txt"
RUN_LOG="logs/${RUN_ID}_run.txt"
TEST_LOG="logs/${RUN_ID}_test.txt"

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
    docker build -t jasminegraph:test . |& tee "$BUILD_LOG"
    build_status="${PIPESTATUS[0]}"
    if [ "$build_status" != '0' ]; then
        set +e
        echo
        echo -e '\e[31;1mERROR: Build failed\e[0m'
        rm -rf tests/integration/env
        exit "$build_status"
    fi
    docker compose -f tests/integration/docker-compose.yml up |& tee "$RUN_LOG" &>/dev/null &
}

cd tests/integration
rm -rf env
cp -r env_init env
cd "$PROJECT_ROOT"
build_and_run_docker

# sleep until server starts listening
cur_timestamp="$(date +%s)"
end_timestamp="$((cur_timestamp + TIMEOUT_SECONDS))"
while ! nc -zvn 127.0.0.1 7777 &>/dev/null; do
    if [ "$(date +%s)" -gt "$end_timestamp" ]; then
        set +e
        echo "JasmineGraph is not listening"
        echo "Build log:"
        cat "$BUILD_LOG"
        echo "Build log:"
        cat "$RUN_LOG"
        rm -rf tests/integration/env
        stop_and_remove_containers
        exit 1
    fi
    sleep .5
done

timeout "$TIMEOUT_SECONDS" python3 -u tests/integration/test.py |& tee "$TEST_LOG"
exit_code="${PIPESTATUS[0]}"
set +e
if [ "$exit_code" == '124' ]; then
    echo
    echo -e '\e[31;1mERROR: Test Timeout\e[0m'
    echo
fi
if [ "$exit_code" != '0' ]; then
    cat "$RUN_LOG"
fi
rm -rf tests/integration/env
stop_and_remove_containers
exit "$exit_code"
