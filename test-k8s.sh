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

    ./start-k8s.sh --META_DB_PATH "${TEST_ROOT}/env/databases/metadb" \
        --PERFORMANCE_DB_PATH "${TEST_ROOT}/env/databases/performancedb" \
        --DATA_PATH "${TEST_ROOT}/env/data" \
        --LOG_PATH "${LOG_DIR}" \
        --AGGREGATE_PATH "${TEST_ROOT}/env/aggregate" \
        --CONFIG_DIRECTORY_PATH "${TEST_ROOT}/env/config" \
        --NO_OF_WORKERS 2 \
        --ENABLE_NMON false
}

clear_resources() {
    ./start-k8s.sh clean
    # Clean hdfs related deployed components
    kubectl delete statefulset,deployments,svc,pvc,pv -l app=hdfs
}

ready_hdfs() {
    echo "Applying HDFS configurations..."

    # Clean residual resources before setting up the deployments
    kubectl delete statefulset,deployments,svc,pvc,pv -l app=hdfs >/dev/null 2>&1

    kubectl apply -f ./k8s/hdfs/pv.yaml
    kubectl apply -f ./k8s/hdfs/namenode-pvc.yaml
    kubectl apply -f ./k8s/hdfs/namenode-deployment.yaml
    kubectl apply -f ./k8s/hdfs/namenode-service.yaml
    kubectl apply -f ./k8s/hdfs/datanode-pvc.yaml
    kubectl apply -f ./k8s/hdfs/datanode-deployment.yaml
    kubectl apply -f ./k8s/hdfs/datanode-service.yaml

    # Deploy YARN ResourceManager and NodeManager
    kubectl apply -f ./k8s/hdfs/resourcemanager-deployment.yaml
    kubectl apply -f ./k8s/hdfs/resourcemanager-service.yaml
    kubectl apply -f ./k8s/hdfs/nodemanager-deployment.yaml
    kubectl apply -f ./k8s/hdfs/nodemanager-service.yaml

    echo "Fetching JasmineGraph Master pod name..."
    MASTER_POD=$(kubectl get pods | grep jasminegraph-master | awk '{print $1}')

    if [[ -z ${MASTER_POD} ]]; then
        echo "Error: JasmineGraph Master pod not found. Exiting."
        return 1
    fi

    echo "Master pod found: ${MASTER_POD}"

    FILE_NAME="powergrid.dl"
    LOCAL_DIRECTORY="/var/tmp/data/"
    LOCAL_FILE_PATH="${LOCAL_DIRECTORY}${FILE_NAME}"
    HDFS_DIRECTORY="/home/"
    HDFS_FILE_PATH="${HDFS_DIRECTORY}${FILE_NAME}"

    # Ensure local directory exists
    mkdir -p "${LOCAL_DIRECTORY}"

    # Copy the file from the master pod
    kubectl cp "${MASTER_POD}:${LOCAL_FILE_PATH}" "${LOCAL_FILE_PATH}" || {
        echo "Error copying file from JasmineGraph Master pod."
        return 1
    }

    #find namenode
    echo "Fetching HDFS namenode pod name..."
    NAMENODE_POD=$(kubectl get pods | grep hdfs-namenode | awk '{print $1}')

    if [[ -z ${NAMENODE_POD} ]]; then
        echo "Error: HDFS namenode pod not found. Exiting."
        return 1
    fi

    echo "Namenode pod found: ${NAMENODE_POD}"

    # Wait until the NameNode service is ready
    echo "Waiting for HDFS NameNode service to be available..."
    while ! kubectl exec "${NAMENODE_POD}" -- hadoop dfsadmin -report &>/dev/null; do
        echo "HDFS NameNode service is not ready yet. Retrying in 5 seconds..."
        sleep 5
    done

    echo "HDFS NameNode service is available."

    # Create the HDFS directory (ensure it exists)
    kubectl exec -i "${NAMENODE_POD}" -- hadoop fs -mkdir -p "${HDFS_DIRECTORY}"
    echo "Created directory: $HDFS_DIRECTORY in Name node"

    # Copy the file from local to the HDFS namenode pod
    kubectl cp "${LOCAL_FILE_PATH}" "${NAMENODE_POD}":"${HDFS_FILE_PATH}" || {
        echo "Error copying file to HDFS namenode pod."
        return 1
    }
    echo "Copied $HDFS_FILE_PATH"

    # Check if the file exists in HDFS
    if ! kubectl exec -i "${NAMENODE_POD}" -- hadoop fs -test -e "${HDFS_FILE_PATH}"; then
        kubectl exec -i "${NAMENODE_POD}" -- hadoop fs -put "${HDFS_FILE_PATH}" "${HDFS_DIRECTORY}"
        echo "File successfully uploaded to HDFS at ${HDFS_FILE_PATH}."
    else
        echo "File already exists in HDFS at ${HDFS_FILE_PATH}. Skipping upload."
    fi
}

cd "$TEST_ROOT"
force_remove env
cp -r env_init env

cd "$PROJECT_ROOT"
build_and_run_on_k8s
ready_hdfs

# Wait till JasmineGraph server start listening
cur_timestamp="$(date +%s)"
end_timestamp="$((cur_timestamp + TIMEOUT_SECONDS))"
masterIP="$(kubectl get services |& grep jasminegraph-master-service | tr '\t' ' ' | tr -s ' ' | cut -d ' ' -f 3)"
while ! nc -zvn -w 1 "$masterIP" 7777 &>/dev/null; do
    if [ "$(date +%s)" -gt "$end_timestamp" ]; then
        set +ex
        echo "JasmineGraph is not listening"
        echo "Build log:"
        cat "$BUILD_LOG"
        echo "Run log:"
        cat "$RUN_LOG"
        force_remove "${TEST_ROOT}/env"
        clear_resources
        exit 1
    fi
    echo "Waiting for JasmineGraph master to be started"
    sleep .5
done

echo '-------------------- pods -------------------------------------'
kubectl get pods -o wide
echo
echo '------------------ services -----------------------------------'
kubectl get services -o wide
echo

timeout "$TIMEOUT_SECONDS" python3 -u "${TEST_ROOT}/test-k8s.py" "$masterIP" |& tee "$TEST_LOG"
exit_code="${PIPESTATUS[0]}"
set +ex
if [ "$exit_code" = '124' ]; then
    echo
    kubectl get pods -o wide

    echo -e '\n\e[33;1mMASTER LOG:\e[0m' |& tee -a "$RUN_LOG"
    kubectl logs --previous deployment/jasminegraph-master-deployment |& tee -a "$RUN_LOG"

    echo -e '\n\e[33;1mWORKER-0 LOG:\e[0m' |& tee -a "$RUN_LOG"
    kubectl logs deployment/jasminegraph-worker0-deployment |& tee -a "$RUN_LOG"

    echo -e '\n\e[33;1mWORKER-1 LOG:\e[0m' |& tee -a "$RUN_LOG"
    kubectl logs deployment/jasminegraph-worker1-deployment |& tee -a "$RUN_LOG"

    echo
    echo -e '\e[31;1mERROR: Test Timeout\e[0m'
    echo
    clear_resources
fi

set +e
clear_resources >/dev/null 2>&1
set -e

force_remove "${TEST_ROOT}/env" "${WORKER_LOG_DIR}"
exit "$exit_code"
