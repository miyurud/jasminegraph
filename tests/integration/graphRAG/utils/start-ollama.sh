#!/bin/bash
# start-ollama.sh
# Start a single Ollama container with gemma3:4b and nomic-embed-text, GPU if available, else CPU
# Usage: ./start-ollama.sh <NUM_PARALLEL> [HOST_PORT]

NUM_PARALLEL=${1:-4}  # Default to 4 parallel queries
HOST_PORT=${2:-11441} # Default port 11441
CONTAINER_NAME="gemma3_container"
DOCKER_IMAGE="ollama/ollama"
MODELS=("gemma3:4b-it-qat" "nomic-embed-text") # List of models to launch

# Detect GPU support
if command -v nvidia-smi &>/dev/null && nvidia-smi >/dev/null 2>&1; then
    echo "GPU detected, running with GPU support."
    GPU_FLAG="--gpus all"
else
    echo "No GPU detected, running on CPU only."
    GPU_FLAG=""
fi

# Check if container already exists
EXISTING_CONTAINER=$(docker ps -a -q -f name="$CONTAINER_NAME")

if [ -n "$EXISTING_CONTAINER" ]; then
    echo "Container '$CONTAINER_NAME' exists. Starting it..."
    docker start "$CONTAINER_NAME"
else
    echo "Container '$CONTAINER_NAME' does not exist. Creating and starting it..."
    # Use a dedicated host directory for Ollama models to avoid filling root
    MODEL_DIR="/mnt/ollama_models"
    MODEL_DIR="$(pwd)/ollama_models"
    mkdir -p "$MODEL_DIR"
    docker run -d $GPU_FLAG \
        -p "${HOST_PORT}:11434" \
        --name "$CONTAINER_NAME" \
        -v "${MODEL_DIR}:/root/.ollama" \
        -e OLLAMA_MODELS=/root/.ollama/models \
        -e OLLAMA_NUM_PARALLEL="$NUM_PARALLEL" \
        -e OLLAMA_MAX_LOADED_MODELS=2 \
        -e OLLAMA_VRAM_RECOVERY_TIMEOUT=15 \
        -e OLLAMA_KEEP_ALIVE=30s \
        "$DOCKER_IMAGE"
fi

# Wait a few seconds for the container to initialize
echo "Waiting for container to initialize..."
sleep 5

# Start pulling each model in background
for MODEL_NAME in "${MODELS[@]}"; do
    echo "Launching model '$MODEL_NAME' inside container..."
    docker exec -d "$CONTAINER_NAME" ollama run "$MODEL_NAME"
done

# Poll until all models are downloaded
for MODEL_NAME in "${MODELS[@]}"; do
    echo "Waiting for model '$MODEL_NAME' to finish downloading..."
    while true; do
        STATUS=$(docker exec "$CONTAINER_NAME" ollama list | grep "$MODEL_NAME" || true)
        if [[ -n $STATUS && $STATUS != *"downloading"* ]]; then
            echo "Model '$MODEL_NAME' is downloaded and ready!"
            break
        else
            echo "… still downloading '$MODEL_NAME', checking again in 10s"
            sleep 10
        fi
    done
done
docker network connect --ip 172.30.5.100 jasminegraph_net "$CONTAINER_NAME"

echo "Container started and all models are ready!"
echo "Access API at http://localhost:${HOST_PORT}"
