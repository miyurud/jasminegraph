#!/bin/bash

SERVICE_DIR="./loki"

PROJECT_NAME="grafana-loki"

cd "$SERVICE_DIR" || { echo "Directory $SERVICE_DIR not found"; exit 1; }

docker-compose -p "$PROJECT_NAME" up -d
