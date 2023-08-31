#!/bin/bash
set -ex
PROJECT_ROOT="$(pwd)"
TEST_ROOT="${PROJECT_ROOT}/tests/integration"
TIMEOUT_SECONDS=600
RUN_ID="$(date +%y%m%d_%H%M%S)"
LOG_DIR="${PROJECT_ROOT}/logs/${RUN_ID}"
while [ -d "$LOG_DIR"]; do
    tmp_id="$((${tmp_id}+1))"
    new_run="${RUN_ID}_${tmp_id}"
    LOG_DIR="${PROJECT_ROOT}/logs/${RUN_ID}"
done
RUN_ID=''

mkdir -p "${PROJECT_ROOT}/logs"
mkdir "$LOG_DIR"

BUILD_LOG="${LOG_DIR}/build.log"
RUN_LOG="${LOG_DIR}/run_master.log"
TEST_LOG="${LOG_DIR}/test.log"


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
        set +ex
        echo
        echo -e '\e[31;1mERROR: Build failed\e[0m'
        rm -rf "${TEST_ROOT}/env"
        exit "$build_status"
    fi
    docker compose -f "${TEST_ROOT}/docker-compose.yml" up |& tee "$RUN_LOG" &>/dev/null &
}

cd "$TEST_ROOT"
rm -rf env
cp -r env_init env
mkdir -p env/logs
cd "$PROJECT_ROOT"
build_and_run_docker

# sleep until server starts listening
cur_timestamp="$(date +%s)"
end_timestamp="$((cur_timestamp + TIMEOUT_SECONDS))"
while ! nc -zvn 127.0.0.1 7777 &>/dev/null; do
    if [ "$(date +%s)" -gt "$end_timestamp" ]; then
        set +ex
        echo "JasmineGraph is not listening"
        echo "Build log:"
        cat "$BUILD_LOG"
        echo "Build log:"
        cat "$RUN_LOG"
        rm -rf "${TEST_ROOT}/env"
        stop_and_remove_containers
        exit 1
    fi
    sleep .5
done

timeout "$TIMEOUT_SECONDS" python3 -u "${TEST_ROOT}/test.py" |& tee "$TEST_LOG"
exit_code="${PIPESTATUS[0]}"
set +ex
if [ "$exit_code" == '124' ]; then
    echo
    echo -e '\e[31;1mERROR: Test Timeout\e[0m'
    echo
fi

cd "$TEST_ROOT"
for f in env/logs/*; do
    fname="$(basename ${f})"
    cp "$f" "${LOG_DIR}/run_${fname}"
done
cd "$LOG_DIR"
if [ "$exit_code" != '0' ]; then
    echo
    echo -e '\e[33;1mMaster log:\e[0m'
    cat "$RUN_LOG"

    for f in run_worker_*; do
        echo
        echo -e '\e[33;1m'"${f:4:-4}"' log:\e[0m'
        cat "$f"
    done
fi

rm -rf "${TEST_ROOT}/env"
stop_and_remove_containers
exit "$exit_code"
