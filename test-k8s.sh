#!/bin/bash

set -e
export TERM=xterm-256color

PROJECT_ROOT="$(pwd)"
TEST_ROOT="${PROJECT_ROOT}/tests/integration"

TIMEOUT_SECONDS=2400

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
MASTER_LOG="${LOG_DIR}/master.log"
TEST_LOG="${LOG_DIR}/test.log"
WORKER_LOG_DIR="/tmp/jasminegraph"
rm -rf "${WORKER_LOG_DIR}"
mkdir -p "${WORKER_LOG_DIR}"

MASTER_LOG_PID=""

# Function to start capturing master logs
start_master_logs() {
    echo "Starting to capture JasmineGraph master logs..."
    MASTER_POD=$(kubectl get pods | grep jasminegraph-master | awk '{print $1}')
    if [[ -z ${MASTER_POD} ]]; then
        echo "Error: JasmineGraph Master pod not found."
        return 1
    fi
    kubectl logs -f "$MASTER_POD" >"$MASTER_LOG" 2>&1 &
    MASTER_LOG_PID=$!
    echo "Master log capture started with PID $MASTER_LOG_PID"
}

# Function to stop master log capture
stop_master_logs() {
    if [[ -n $MASTER_LOG_PID ]]; then
        echo "Stopping master log capture..."
        kill "$MASTER_LOG_PID" >/dev/null 2>&1 || true
        wait "$MASTER_LOG_PID" 2>/dev/null || true
        MASTER_LOG_PID=""
    fi
}

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
ready_hdfs() {
    export HOST_IP=$(hostname -I | awk '{print $1}')
    echo "Detected Host IP: $HOST_IP"
    HDFS_CONF_FILE="${TEST_ROOT}/env_init/config/hdfs/hdfs_config.txt"

    # Replace the IP in the file
    sed -i "s/^hdfs\.host=.*/hdfs.host=${HOST_IP}/" $HDFS_CONF_FILE

    echo "Updated hdfs_cnf.txt:"
    cat $HDFS_CONF_FILE
    MASTER_POD=$(kubectl get pods | grep jasminegraph-master | awk '{print $1}')

    kubectl cp "${TEST_ROOT}/env_init/config/hdfs/hdfs_config.txt" ${MASTER_POD}:/var/tmp/config/hdfs_config.txt
    echo "Starting HDFS using Docker Compose..."
    docker compose -f "${TEST_ROOT}/docker-compose-k8s-hdfs.yaml" up >"$RUN_LOG" 2>&1 &

    echo "Waiting for Hadoop Namenode to be ready..."
    while ! curl -s http://localhost:9870 &>/dev/null; do
        echo "Hadoop Namenode is not ready yet. Retrying in 5 seconds..."
        sleep 5
    done
    echo "Hadoop Namenode is ready."

    echo "Checking and exiting safe mode if necessary..."
    if docker exec -i hdfs-namenode hadoop dfsadmin -safemode get | grep -q "Safe mode is ON"; then
        echo "Exiting safe mode..."
        docker exec -i hdfs-namenode hadoop dfsadmin -safemode leave || {
            echo "Error exiting safe mode."
            return 1
        }
    else
        echo "Namenode is not in safe mode."
    fi

    echo "Fetching JasmineGraph Master pod name..."
    MASTER_POD=$(kubectl get pods | grep jasminegraph-master | awk '{print $1}')
    if [[ -z ${MASTER_POD} ]]; then
        echo "Error: JasmineGraph Master pod not found."
        return 1
    fi
    echo "Master pod found: ${MASTER_POD}"

    FILE_NAME="powergrid.dl"
    LOCAL_DIRECTORY="/var/tmp/data/"
    LOCAL_FILE_PATH="${LOCAL_DIRECTORY}${FILE_NAME}"
    HDFS_DIRECTORY="/home/"
    HDFS_FILE_PATH="${HDFS_DIRECTORY}${FILE_NAME}"

    echo "Ensuring local directory exists..."
    mkdir -p "${LOCAL_DIRECTORY}" || {
        echo "Error creating local directory."
        return 1
    }

    echo "Copying file from JasmineGraph Master pod..."
    kubectl cp "${MASTER_POD}:${LOCAL_FILE_PATH}" "${LOCAL_FILE_PATH}" || {
        echo "Error copying file from JasmineGraph Master pod."
        return 1
    }

    echo "Fetching HDFS Namenode container name..."
    NAMENODE_CONTAINER=$(docker ps --format '{{.Names}}' | grep namenode)
    if [[ -z ${NAMENODE_CONTAINER} ]]; then
        echo "Error: HDFS Namenode container not found."
        return 1
    fi
    echo "Namenode container found: ${NAMENODE_CONTAINER}"

    docker exec -i "${NAMENODE_CONTAINER}" mkdir -p "${LOCAL_DIRECTORY}"

    echo "Copying file to HDFS Namenode container..."
    docker cp "${LOCAL_FILE_PATH}" "${NAMENODE_CONTAINER}:${LOCAL_FILE_PATH}" || {
        echo "Error copying file to Namenode container."
        return 1
    }

    echo "Uploading file to HDFS..."
    docker exec -i "${NAMENODE_CONTAINER}" hdfs dfs -mkdir -p "${HDFS_DIRECTORY}" || {
        echo "Error creating HDFS directory."
        return 1
    }
    docker exec -i "${NAMENODE_CONTAINER}" hdfs dfs -put -f "${LOCAL_FILE_PATH}" "${HDFS_FILE_PATH}" || {
        echo "Error uploading file to HDFS."
        return 1
    }

    echo "File successfully uploaded to HDFS at ${HDFS_FILE_PATH}"

    CUSTOM_GRAPH_FILE="graph_with_properties.txt"
    CUSTOM_GRAPH_LOCAL_PATH="${LOCAL_DIRECTORY}${CUSTOM_GRAPH_FILE}"
    CUSTOM_GRAPH_HDFS_PATH="${HDFS_DIRECTORY}${CUSTOM_GRAPH_FILE}"
    echo "Copying custom graph file from JasmineGraph Master pod..."
    kubectl cp "${MASTER_POD}:${CUSTOM_GRAPH_LOCAL_PATH}" "${CUSTOM_GRAPH_LOCAL_PATH}" || {
        echo "Error copying custom graph file from JasmineGraph Master pod."
        return 1
    }
    echo "Copying custom graph file to HDFS Namenode container..."
    docker cp "${CUSTOM_GRAPH_LOCAL_PATH}" "${NAMENODE_CONTAINER}:${CUSTOM_GRAPH_LOCAL_PATH}" || {
        echo "Error copying custom graph file to Namenode container."
        return 1
    }
    echo "Uploading custom graph file to HDFS..."
    docker exec -i "${NAMENODE_CONTAINER}" hdfs dfs -mkdir -p "${HDFS_DIRECTORY}" || {
        echo "Error creating HDFS directory for custom graph file."
        return 1
    }
    docker exec -i "${NAMENODE_CONTAINER}" hdfs dfs -put -f "${CUSTOM_GRAPH_LOCAL_PATH}" "${CUSTOM_GRAPH_HDFS_PATH}" || {
        echo "Error uploading custom graph file to HDFS."
        return 1
    }
    echo "Custom graph file successfully uploaded to HDFS at ${CUSTOM_GRAPH_HDFS_PATH}"

    CUSTOM_GRAPH_FILE="graph_with_properties_large.txt"
    CUSTOM_GRAPH_LOCAL_PATH="${LOCAL_DIRECTORY}${CUSTOM_GRAPH_FILE}"
    CUSTOM_GRAPH_HDFS_PATH="${HDFS_DIRECTORY}${CUSTOM_GRAPH_FILE}"
    echo "Copying custom graph file from JasmineGraph Master pod..."
    kubectl cp "${MASTER_POD}:${CUSTOM_GRAPH_LOCAL_PATH}" "${CUSTOM_GRAPH_LOCAL_PATH}" || {
        echo "Error copying custom graph file from JasmineGraph Master pod."
        return 1
    }
    echo "Copying custom graph file to HDFS Namenode container..."
    docker cp "${CUSTOM_GRAPH_LOCAL_PATH}" "${NAMENODE_CONTAINER}:${CUSTOM_GRAPH_LOCAL_PATH}" || {
        echo "Error copying custom graph file to Namenode container."
        return 1
    }
    echo "Uploading custom graph file to HDFS..."
    docker exec -i "${NAMENODE_CONTAINER}" hdfs dfs -mkdir -p "${HDFS_DIRECTORY}" || {
        echo "Error creating HDFS directory for custom graph file."
        return 1
    }
    docker exec -i "${NAMENODE_CONTAINER}" hdfs dfs -put -f "${CUSTOM_GRAPH_LOCAL_PATH}" "${CUSTOM_GRAPH_HDFS_PATH}" || {
        echo "Error uploading custom graph file to HDFS."
        return 1
    }
    echo "Custom graph file successfully uploaded to HDFS at ${CUSTOM_GRAPH_HDFS_PATH}"

    CUSTOM_GRAPH_FILE="text.txt"
    CUSTOM_GRAPH_LOCAL_PATH="${LOCAL_DIRECTORY}${CUSTOM_GRAPH_FILE}"
    CUSTOM_GRAPH_HDFS_PATH="${HDFS_DIRECTORY}${CUSTOM_GRAPH_FILE}"
    echo "Copying text file from JasmineGraph Master pod..."
    kubectl cp "${MASTER_POD}:${CUSTOM_GRAPH_LOCAL_PATH}" "${CUSTOM_GRAPH_LOCAL_PATH}" || {
        echo "Error copying custom graph file from JasmineGraph Master pod."
        return 1
    }
    echo "Copying text file to HDFS Namenode container..."
    docker cp "${CUSTOM_GRAPH_LOCAL_PATH}" "${NAMENODE_CONTAINER}:${CUSTOM_GRAPH_LOCAL_PATH}" || {
        echo "Error copying custom graph file to Namenode container."
        return 1
    }
    echo "Uploading text file to HDFS..."
    docker exec -i "${NAMENODE_CONTAINER}" hdfs dfs -mkdir -p "${HDFS_DIRECTORY}" || {
        echo "Error creating HDFS directory for custom graph file."
        return 1
    }
    docker exec -i "${NAMENODE_CONTAINER}" hdfs dfs -put -f "${CUSTOM_GRAPH_LOCAL_PATH}" "${CUSTOM_GRAPH_HDFS_PATH}" || {
        echo "Error uploading textfile to HDFS."
        return 1
    }
    echo "Text file successfully uploaded to HDFS at ${CUSTOM_GRAPH_HDFS_PATH}"

}
cd "$TEST_ROOT"
force_remove env
cp -r env_init env

cd "$PROJECT_ROOT"
build_and_run_on_k8s
ready_hdfs
start_master_logs

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
        stop_master_logs
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
if [ "$exit_code" != '0' ]; then
    echo -e '\n\e[34;1m=== Pods Status ===\e[0m'
    kubectl get pods -o wide

    echo -e '\n\e[34;1m=== Describe Master Pod ===\e[0m'
    kubectl describe pod $(kubectl get pods | grep jasminegraph-master | awk '{print $1}')

    echo -e '\n\e[34;1m=== Describe Worker Pods ===\e[0m'
    for pod in $(kubectl get pods | grep jasminegraph-worker | awk '{print $1}'); do
        echo "--- $pod ---"
        kubectl describe pod "$pod"
    done

    echo -e '\n\e[33;1m=== Worker Logs ===\e[0m'
    for pod in $(kubectl get pods | grep jasminegraph-worker | awk '{print $1}'); do
        echo "--- Logs for $pod ---"
        kubectl logs "$pod" || echo "No logs for $pod"
    done

    echo
    kubectl get pods -o wide

    echo -e '\n\e[34;1mCaptured JasmineGraph Master Logs:\e[0m'
    cat "$MASTER_LOG"

    echo -e '\n\e[33;1mWORKER-0 LOG:\e[0m' |& tee -a "$RUN_LOG"
    kubectl logs deployment/jasminegraph-worker0-deployment |& tee -a "$RUN_LOG"

    echo -e '\n\e[33;1mWORKER-1 LOG:\e[0m' |& tee -a "$RUN_LOG"
    kubectl logs deployment/jasminegraph-worker1-deployment |& tee -a "$RUN_LOG"

    echo -e '\n\e[34;1m=== Node Resources (if available) ===\e[0m'
    kubectl top pods || echo "kubectl top not available"
    free -h
    df -h
    du -sh /* 2>/dev/null | sort -hr | head -n 10

    echo
    echo -e '\e[31;1mERROR: Test Timeout\e[0m'
    echo
    clear_resources
fi

stop_master_logs
set +e
clear_resources >/dev/null 2>&1
set -e

force_remove "${TEST_ROOT}/env" "${WORKER_LOG_DIR}"

exit "$exit_code"
