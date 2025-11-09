#!/bin/bash
# start-ollama.sh
# Start a single Ollama container with gemma3:4b and nomic-embed-text, GPU if available, else CPU
# Usage: ./start-ollama.sh <NUM_PARALLEL> [HOST_PORT]

NUM_PARALLEL=${1:-4}  # Default to 4 parallel queries
HOST_PORT=${2:-11441} # Default port 11441
CONTAINER_NAME="gemma3_container"
DOCKER_IMAGE="ollama/ollama"
NETWORK_NAME="jasminegraph_net"   # <-- your Compose network name
MODELS=("gemma3:4b-it-qat" "nomic-embed-text")

# Detect GPU support
if command -v nvidia-smi &>/dev/null && nvidia-smi >/dev/null 2>&1; then
    echo "GPU detected, running with GPU support."
    GPU_FLAG="--gpus all"
else
    echo "No GPU detected, running on CPU only."
    GPU_FLAG=""
fi

# Ensure network exists (Compose creates it, but this is a safety net)
if ! docker network inspect "$NETWORK_NAME" >/dev/null 2>&1; then
    echo "Creating network $NETWORK_NAME..."
    docker network create "$NETWORK_NAME"
fi

# Check if container already exists
EXISTING_CONTAINER=$(docker ps -a -q -f name="^${CONTAINER_NAME}$")

if [ -n "$EXISTING_CONTAINER" ]; then
    echo "Container '$CONTAINER_NAME' exists. Starting it..."
    docker start "$CONTAINER_NAME"
else
    echo "Container '$CONTAINER_NAME' does not exist. Creating and starting it..."
    docker run -d $GPU_FLAG \
        --network "$NETWORK_NAME" \   # <-- attach to the same network
        --hostname "$CONTAINER_NAME" \ # internal name inside the network
        -p "${HOST_PORT}:11434" \
        --name "$CONTAINER_NAME" \
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
            echo "✅ Model '$MODEL_NAME' is downloaded and ready!"
            break
        else
            echo "… still downloading '$MODEL_NAME', checking again in 10s"
            sleep 10
        fi
    done
done

echo "✅ Container started, attached to network '$NETWORK_NAME', and all models are ready!"
echo "Access API inside the network at:  http://${CONTAINER_NAME}:11434"
echo "Access from host at:              http://localhost:${HOST_PORT}"
