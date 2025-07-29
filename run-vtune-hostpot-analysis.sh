#!/bin/bash
set -e
docker cp $(docker ps --filter "publish=7777" --format "{{.Names}}"):/home/ubuntu/software/jasminegraph/JasmineGraph .

