#!/bin/bash
set -e
#rm -rf  /var/tmp/jasminegraph-localstore/* /var/tmp/jasminegraph-localstore/* /var/tmp/jasminegraph-aggregate/* /var/tmp/hdfs/filechunks/*  ./env/databases/metadb/* ./env/databases/performancedb/* /tmp/jasminegraph/*
echo "=== Backing up original run-docker.sh ==="
cp run-docker.sh run-docker.sh.bak

echo "=== Patching run-docker.sh to use valgrind if DEBUG is unset ==="
sed -i '
/if \[ -n "\$DEBUG" \]; then/,/else/ {
    # Do not change DEBUG block
}
/else/,/fi/ {
    /if \[ \$MODE -eq 1 \]; then/,/else/ {
        /valgrind/d
        s|./JasmineGraph|valgrind --leak-check=full --show-leak-kinds=all ./JasmineGraph|
    }
}
' run-docker.sh
chmod +x run-docker.sh

# Function to restore run-docker.sh on exit (even if failed)
restore_original() {
    echo "=== Restoring original run-docker.sh ==="
    mv run-docker.sh.bak run-docker.sh
}
trap restore_original EXIT

echo "=== Building Docker image jasminegraph ==="
docker build -t jasminegraph .

echo "=== Running integration tests ==="
chmod +x test-docker.sh
./test-docker.sh
## sleep for a while to ensure all logs are written
echo "=== Waiting for logs to be written ==="
sleep 60
echo "=== Locating latest run_master.log ==="
LATEST_LOG_DIR=$(ls -td logs/*/ | head -n 1)
LOG_FILE="${LATEST_LOG_DIR}run_master.log"

if [ -f "$LOG_FILE" ]; then
    echo "== BEGIN LOG: $LOG_FILE =="
    cat "$LOG_FILE"
    echo "== END LOG =="
else
    echo "❌ run_master.log not found in $LATEST_LOG_DIR"
    exit 1
fi

echo "=== Checking Valgrind output for memory leaks ==="
DEFINITELY_LOST=$(grep "definitely lost:" "$LOG_FILE" | awk '{print $4}')

if [ "$DEFINITELY_LOST" != "0" ]; then
    echo "❌ Memory leaks detected: definitely lost = $DEFINITELY_LOST"
    grep -A5 "LEAK SUMMARY:" "$LOG_FILE"
    exit 1
else
    echo "✅ No memory leaks detected (definitely lost = 0)"
fi
