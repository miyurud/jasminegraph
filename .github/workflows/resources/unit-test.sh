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

mkdir coverage
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

echo "----------------------------- logs -----------------------------"
kubectl logs -f jasminegraph-unit-test-pod

check_pod_status
if [[ $pod_status != "Running" && $pod_status != "Completed" ]]; then
    echo "Pod jasminegraph-unit-test-pod is in $pod_status state"
    echo "Unit tests failed"
    echo "----------------------------- details --------------------------"
    kubectl describe pod jasminegraph-unit-test-pod
    exit 1
fi
