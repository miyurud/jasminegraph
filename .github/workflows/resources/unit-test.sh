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

# Clean up any existing test resources before starting
echo "Cleaning up existing test resources..."
kubectl delete pod jasminegraph-unit-test-pod --ignore-not-found=true
kubectl delete pvc host-volume-claim --ignore-not-found=true
kubectl delete pv host-volume --ignore-not-found=true
kubectl delete deployment -l deployment=jasminegraph-worker --ignore-not-found=true
kubectl delete service -l service=jasminegraph-worker --ignore-not-found=true
kubectl delete service jasminegraph-master-service --ignore-not-found=true

# Wait for resources to be fully deleted
echo "Waiting for cleanup to complete..."
sleep 10

mkdir -p coverage
kubectl apply -f ./k8s/configs.yaml
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
    kubectl delete pod jasminegraph-unit-test-pod --ignore-not-found=true
    kubectl delete pvc host-volume-claim --ignore-not-found=true
    kubectl delete pv host-volume --ignore-not-found=true
    kubectl delete deployment -l deployment=jasminegraph-worker --ignore-not-found=true
    kubectl delete service -l service=jasminegraph-worker --ignore-not-found=true
    kubectl delete service jasminegraph-master-service --ignore-not-found=true

    exit 1
fi

# Cleanup test resources after successful run
echo "Cleaning up test resources..."
kubectl delete pod jasminegraph-unit-test-pod --ignore-not-found=true
kubectl delete pvc host-volume-claim --ignore-not-found=true
kubectl delete pv host-volume --ignore-not-found=true
kubectl delete deployment -l deployment=jasminegraph-worker --ignore-not-found=true
kubectl delete service -l service=jasminegraph-worker --ignore-not-found=true
kubectl delete service jasminegraph-master-service --ignore-not-found=true