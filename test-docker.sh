#!/bin/bash

set -e

export TERM=xterm-256color

PROJECT_ROOT="$(pwd)"
TEST_ROOT="${PROJECT_ROOT}/tests/integration"
TIMEOUT_SECONDS=180
RUN_ID="$(date +%y%m%d_%H%M%S)"
LOG_DIR="${PROJECT_ROOT}/logs/${RUN_ID}"
while [ -d "$LOG_DIR" ]; do
    tmp_id="$((tmp_id + 1))"
    new_run="${RUN_ID}_${tmp_id}"
    LOG_DIR="${PROJECT_ROOT}/logs/${RUN_ID}"
done
RUN_ID=''

mkdir -p "${PROJECT_ROOT}/logs"
mkdir "$LOG_DIR"

BUILD_LOG="${LOG_DIR}/build.log"
RUN_LOG="${LOG_DIR}/run_master.log"
TEST_LOG="${LOG_DIR}/test.log"
WORKER_LOG_DIR="/tmp/jasminegraph"
rm -rf "${WORKER_LOG_DIR}"
mkdir -p "${WORKER_LOG_DIR}"

force_remove() {
    local files=("$@")
    for f in "${files[@]}"; do
        rm -rf "$f" &>/dev/null || sudo rm -rf "$f"
    done
}

stop_and_remove_containers() {
    if [ ! -z "$(docker ps -a -q)" ]; then
        docker ps -a -q | xargs docker rm -f &>/dev/null
    else
        echo "No containers to stop and remove."
    fi
    docker run -v '/tmp/jasminegraph:/tmp/jasminegraph' --entrypoint /bin/bash jasminegraph:test -c 'rm -rf /tmp/jasminegraph/*' || echo 'Not removing existing tmp logs'
    if [ ! -z "$(docker ps -a -q)" ]; then
        docker ps -a -q | xargs docker rm -f &>/dev/null
    fi
}

build_and_run_docker() {
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
    docker compose -f "${TEST_ROOT}/docker-compose.yml" up >"$RUN_LOG" 2>&1 &
}

stop_tests_on_failure() {
    set +ex
    failCnt=0
    while true; do
        if nc -zvn 127.0.0.1 7777 &>/dev/null; then
            failCnt=0
        else
            if [ "$failCnt" = 0 ]; then
                failCnt=1
            else
                sleep 5
                if pgrep python3 &>/dev/null; then
                    echo "Master has stopped. Stopping tests..."
                    pkill python3
                fi
                break
            fi
        fi
        sleep 1
    done
}

cd "$TEST_ROOT"
force_remove env
cp -r env_init env
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
        echo -e '\n\e[33;1mMASTER LOG:\e[0m'
        cat "$RUN_LOG"
        force_remove "${TEST_ROOT}/env"
        stop_and_remove_containers
        exit 1
    fi
    sleep .5
done

stop_tests_on_failure &

timeout "$TIMEOUT_SECONDS" python3 -u "${TEST_ROOT}/test.py" |& tee "$TEST_LOG"
exit_code="${PIPESTATUS[0]}"
set +ex
if [ "$exit_code" = '124' ]; then
    echo
    echo -e '\e[31;1mERROR: Test Timeout\e[0m'
    echo
fi

print_log() {
    sed -e 's/^integration-jasminegraph-1  | //g' \
        -e 's/ \[logger\]//g' \
        -e 's/ \[info\] / ['$'\033''[32minfo'$'\033''[0m] /g' \
        -e 's/ \[warn\] / ['$'\033''[1;33mwarn'$'\033''[0m] /g' \
        -e 's/ \[error\] / ['$'\033''[1;31merror'$'\033''[0m] /g' \
        -e 's/ \[INFO\] / ['$'\033''[32mINFO'$'\033''[0m] /g' \
        -e 's/ \[WARNING\] / ['$'\033''[1;33mWARNING'$'\033''[0m] /g' "$1"
}

cd "$TEST_ROOT"
for d in "${WORKER_LOG_DIR}"/worker_*; do
    echo
    worker_name="$(basename ${d})"
    cp -r "$d" "${LOG_DIR}/${worker_name}"
done

cd "$LOG_DIR"
if [ "$exit_code" != '0' ]; then
    echo
    echo -e '\e[33;1mMaster log:\e[0m'
    print_log "$RUN_LOG"

    for d in worker_*; do
        cd "${LOG_DIR}/${d}"
        echo
        echo -e '\e[33;1m'"${d}"' log:\e[0m'
        print_log worker.log

        for f in merge_*.log; do
            echo
            echo -e '\e[33;1m'"${d} ${f::-4}"' log:\e[0m'
            print_log "$f"
        done

        for f in fl_client_*.log; do
            echo
            echo -e '\e[33;1m'"${d} ${f::-4}"' log:\e[0m'
            print_log "$f"
        done

        for f in fl_server_*.log; do
            echo
            echo -e '\e[33;1m'"${d} ${f::-4}"' log:\e[0m'
            print_log "$f"
        done
    done
fi

stop_and_remove_containers
force_remove "${TEST_ROOT}/env" "${WORKER_LOG_DIR}"
exit "$exit_code"
