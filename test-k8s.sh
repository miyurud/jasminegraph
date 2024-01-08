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

build_and_run_on_k8s() {
    cd "$PROJECT_ROOT"
    docker build -t jasminegraph . |& tee "$BUILD_LOG"
    build_status="${PIPESTATUS[0]}"
    if [ "$build_status" != '0' ]; then
        set +ex
        echo
        echo -e '\e[31;1mERROR: Build failed\e[0m'
        rm -rf "${TEST_ROOT}/env"
        exit "$build_status"
    fi

    set +e
    clear_resources >/dev/null 2>&1
    set -e

    metadb_path="${TEST_ROOT}/env/databases/metadb" \
        performancedb_path="${TEST_ROOT}/env/databases/performancedb" \
        data_path="${TEST_ROOT}/env/data" \
        log_path="${LOG_DIR}" \
        envsubst <"${PROJECT_ROOT}/k8s/volumes.yaml" | kubectl apply -f -

    kubectl apply -f "${PROJECT_ROOT}/k8s/master-deployment.yaml"
}

clear_resources() {
    kubectl delete deployments jasminegraph-master-deployment jasminegraph-worker1-deployment \
        jasminegraph-worker0-deployment
    kubectl delete services jasminegraph-master-service jasminegraph-worker0-service jasminegraph-worker1-service
    kubectl delete -f "${PROJECT_ROOT}/k8s/volumes.yaml"
}

cd "$TEST_ROOT"
rm -rf env
cp -r env_init env
chmod 777 -R env

echo "metadb folder"
ls -la env/databases/metadb
echo "performancedb folder"
ls -la env/databases/performancedb
echo

cd "$PROJECT_ROOT"
build_and_run_on_k8s

# sleep until server starts listening
cur_timestamp="$(date +%s)"
end_timestamp="$((cur_timestamp + TIMEOUT_SECONDS))"
while true; do
    if [ "$(date +%s)" -gt "$end_timestamp" ]; then
        set +ex
        echo "JasmineGraph is not listening"
        echo "Build log:"
        cat "$BUILD_LOG"
        echo "Build log:"
        cat "$RUN_LOG"
        rm -rf "${TEST_ROOT}/env"
        clear_resources
        exit 1
    fi
    masterIP="$(kubectl get services |& grep jasminegraph-master-service | tr '\t' ' ' | tr -s ' ' | cut -d ' ' -f 3)"
    if [ ! -z "$masterIP" ]; then
        break
    fi
    sleep .5
done

while ! nc -zvn "$masterIP" 7777 &>/dev/null; do
    if [ "$(date +%s)" -gt "$end_timestamp" ]; then
        set +ex
        echo "JasmineGraph is not listening"
        echo "Build log:"
        cat "$BUILD_LOG"
        echo "Build log:"
        cat "$RUN_LOG"
        rm -rf "${TEST_ROOT}/env"
        clear_resources
        exit 1
    fi
    sleep .5
done

echo
kubectl get pods
kubectl get services
echo

timeout "$TIMEOUT_SECONDS" python3 -u "${TEST_ROOT}/test-k8s.py" |& tee "$TEST_LOG"
exit_code="${PIPESTATUS[0]}"
set +ex
if [ "$exit_code" = '124' ]; then
    echo
    kubectl get pods
    echo

    kubectl logs --previous deployment/jasminegraph-master-deployment |& tee -a "$RUN_LOG"

    echo
    echo -e '\e[31;1mERROR: Test Timeout\e[0m'
    echo
    clear_resources
fi

set +e
clear_resources >/dev/null 2>&1
set -e

rm -rf "${TEST_ROOT}/env" "${WORKER_LOG_DIR}"
exit "$exit_code"
