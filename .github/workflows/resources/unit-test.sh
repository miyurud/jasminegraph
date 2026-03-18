#!/bin/bash

# Function to check if pod is in Pending/Running state
pod_status=""
check_pod_status() {
    pod_status=$(kubectl get pod jasminegraph-unit-test-pod --no-headers -o custom-columns=":status.phase")

    if [[ $pod_status == "Pending" || $pod_status == "ContainerCreating" ]]; then
        echo 0
    else
        echo 1
    fi
}

delete_jasminegraph_resources() {
    # Label-based cleanup (fast path)
    kubectl delete deployment -l deployment=jasminegraph-worker --ignore-not-found=true
    kubectl delete service -l service=jasminegraph-worker --ignore-not-found=true
    kubectl delete service jasminegraph-master-service --ignore-not-found=true
    kubectl delete pvc -l application=jasminegraph -n default --ignore-not-found=true
    kubectl delete pv -l application=jasminegraph --ignore-not-found=true

    # Name-based fallback for stale resources that might not have expected labels
    kubectl get deployment -o name 2>/dev/null | grep "jasminegraph-worker" | xargs -r kubectl delete --ignore-not-found=true
    kubectl get service -o name 2>/dev/null | grep "jasminegraph-worker" | xargs -r kubectl delete --ignore-not-found=true
    kubectl get pvc -o name -n default 2>/dev/null | grep "jasminegraph-worker" | xargs -r kubectl delete -n default --ignore-not-found=true
    kubectl get pv -o name 2>/dev/null | grep "jasminegraph-worker" | xargs -r kubectl delete --ignore-not-found=true
}

wait_for_jasminegraph_cleanup() {
    local timeout_seconds="${1:-180}"
    local start_time
    start_time=$(date +%s)

    while true; do
        local deployments services pvcs pvs
        deployments=$(kubectl get deployment --no-headers 2>/dev/null | grep -c "jasminegraph-worker" || true)
        services=$(kubectl get service --no-headers 2>/dev/null | grep -c "jasminegraph-worker" || true)
        pvcs=$(kubectl get pvc -n default --no-headers 2>/dev/null | grep -c "jasminegraph-worker" || true)
        pvs=$(kubectl get pv --no-headers 2>/dev/null | grep -c "jasminegraph-worker" || true)

        if [[ "$deployments" -eq 0 && "$services" -eq 0 && "$pvcs" -eq 0 && "$pvs" -eq 0 ]]; then
            break
        fi

        if [[ $(( $(date +%s) - start_time )) -ge "$timeout_seconds" ]]; then
            echo "Timed out waiting for JasmineGraph worker resources to be deleted"
            kubectl get deployment -o wide || true
            kubectl get service -o wide || true
            kubectl get pvc -n default || true
            kubectl get pv || true
            return 1
        fi

        sleep 5
    done
}

cleanup_test_resources() {
    kubectl delete pod jasminegraph-unit-test-pod --ignore-not-found=true
    kubectl delete pvc host-volume-claim --ignore-not-found=true
    kubectl delete pv host-volume --ignore-not-found=true
    delete_jasminegraph_resources
}

# Clean up any existing test resources before starting
echo "Cleaning up existing test resources..."
kubectl delete pod jasminegraph-unit-test-pod --ignore-not-found=true
kubectl delete pvc host-volume-claim --ignore-not-found=true
kubectl delete pv host-volume --ignore-not-found=true
kubectl wait --for=delete pv host-volume --timeout=60s 2>/dev/null || true
delete_jasminegraph_resources

# Wait for resources to be fully deleted
echo "Waiting for cleanup to complete..."
if ! wait_for_jasminegraph_cleanup 180; then
    echo "Pre-test cleanup did not complete successfully. Aborting unit tests."
    cleanup_test_resources
    exit 1
fi
kubectl wait --for=delete pv -l application=jasminegraph --timeout=120s 2>/dev/null || true
sleep 10

mkdir -p coverage
export max_worker_count="${max_worker_count:-4}"
export pushgateway_address="${pushgateway_address:-}"
export prometheus_address="${prometheus_address:-}"
envsubst < ./k8s/configs.yaml | kubectl apply -f -
kubectl apply -f ./.github/workflows/resources/unit-test-conf.yaml

timeout=300 # Set the timeout in seconds (adjust as needed)
start_time=$(date +%s)

while [[ $(check_pod_status) == "0" ]]; do
    sleep 10

    current_time=$(date +%s)
    elapsed_time=$((current_time - start_time))

    if [[ $elapsed_time -ge $timeout ]]; then
        echo "Timeout reached. Exiting loop."
        break
    fi
done
free -h
df -h
du -sh /* 2>/dev/null | sort -hr | head -n 10
echo "----------------------------- logs -----------------------------"
kubectl logs -f jasminegraph-unit-test-pod

check_pod_status
if [[ $pod_status != "Running" && $pod_status != "Completed" ]]; then
    echo "Pod jasminegraph-unit-test-pod is in $pod_status state"
    echo "Unit tests failed"
    echo "----------------------------- details --------------------------"
    kubectl describe pod jasminegraph-unit-test-pod

    # Cleanup on failure
    cleanup_test_resources

    exit 1
fi

# Pull coverage report from pod into workspace so GitHub Actions can upload it.
coverage_src="/home/ubuntu/software/jasminegraph/coverage/coverage.xml"
coverage_dst="./coverage/coverage.xml"
if ! kubectl cp "jasminegraph-unit-test-pod:${coverage_src}" "${coverage_dst}"; then
    # The coverage directory is host-mounted, so report can still exist even if container is gone.
    if [[ -s "${coverage_dst}" ]]; then
        echo "kubectl cp failed, but coverage file already exists at ${coverage_dst}. Continuing."
    else
        echo "Failed to copy coverage report from pod and no local fallback found: ${coverage_src}"
        cleanup_test_resources
        exit 1
    fi
fi

if [[ ! -s ${coverage_dst} ]]; then
    echo "Coverage file is missing or empty at ${coverage_dst}"

    # Cleanup on validation failure
    cleanup_test_resources

    exit 1
fi

echo "Coverage report copied to ${coverage_dst}"

# Cleanup test resources after successful run
echo "Cleaning up test resources..."
cleanup_test_resources
