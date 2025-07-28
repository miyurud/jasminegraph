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
        s|./JasmineGraph|valgrind --leak-check=full --show-leak-kinds=all --gen-suppressions=all ./JasmineGraph|
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

#echo "=== Checking Valgrind output for memory leaks ==="
#DEFINITELY_LOST=$(grep "definitely lost:" "$LOG_FILE" | sed -E 's/.*definitely lost:\s+([0-9,]+) bytes.*/\1/' | tr -d ',')
#
#
#if [ "$DEFINITELY_LOST" != "0" ]; then
#    echo "❌ Memory leaks detected: definitely lost = $DEFINITELY_LOST"
#    grep -A5 "LEAK SUMMARY:" "$LOG_FILE"
#    exit 1
#else
#    echo "✅ No memory leaks detected (definitely lost = 0)"
#fi
#echo "=== Checking Valgrind output for memory leaks ==="

# Define threshold for number of detailed "definitely lost" issues
THRESHOLD=30

# Find detailed loss records with "definitely lost"
DETAILED_LOST_LINES=$(grep -E '[0-9,]+.*\(.*direct.*,.*indirect.*\).*are definitely lost' "$LOG_FILE")
DETAILED_LOST_COUNT=$(echo "$DETAILED_LOST_LINES" | grep -c "definitely lost")

echo "Found $DETAILED_LOST_COUNT detailed definitely lost memory issues"

if [ "$DETAILED_LOST_COUNT" -gt "$THRESHOLD" ]; then
    echo "❌ Memory leak threshold exceeded! (Threshold: $THRESHOLD)"
    echo "$DETAILED_LOST_LINES"
    echo
    grep -A5 "LEAK SUMMARY:" "$LOG_FILE"
    exit 1
else
    echo "✅ Memory leak count within acceptable threshold (<= $THRESHOLD)"
fi