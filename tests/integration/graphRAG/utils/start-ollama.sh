#!/bin/bash
# start-olama.sh
# Start a single Ollama container with gemma3:4b, GPU if available, else CPU
# Usage: ./start-olama.sh <NUM_PARALLEL> [HOST_PORT]

NUM_PARALLEL=${1:-2}     # Default to 2 parallel queries
HOST_PORT=${2:-11441}    # Default port 11441
CONTAINER_NAME="gemma3_container"
DOCKER_IMAGE="ollama/ollama"
MODEL_NAME="gemma3:4b-it-qat"

# Detect GPU support
if command -v nvidia-smi &> /dev/null && nvidia-smi > /dev/null 2>&1; then
    echo "✅ GPU detected, running with GPU support."
    GPU_FLAG="--gpus all"
else
    echo "⚠️ No GPU detected, running on CPU only."
    GPU_FLAG=""
fi

# Check if container already exists
EXISTING_CONTAINER=$(sudo docker ps -a -q -f name=$CONTAINER_NAME)

if [ -n "$EXISTING_CONTAINER" ]; then
    echo "Container '$CONTAINER_NAME' exists. Starting it..."
    sudo docker start $CONTAINER_NAME
else
    echo "Container '$CONTAINER_NAME' does not exist. Creating and starting it..."
    sudo docker run -d $GPU_FLAG \
        -p ${HOST_PORT}:11434 \
        --name $CONTAINER_NAME \
        -e OLLAMA_NUM_PARALLEL=$NUM_PARALLEL \
        -e OLLAMA_MAX_LOADED_MODELS=2 \
        -e OLLAMA_VRAM_RECOVERY_TIMEOUT=15 \
        -e OLLAMA_KEEP_ALIVE=30s \
        $DOCKER_IMAGE
fi

# Wait a few seconds for the container to initialize
echo "Waiting for container to initialize..."
sleep 5

# Check if model is already loaded inside container

echo "Launching model '$MODEL_NAME' inside container..."
sudo docker exec -d $CONTAINER_NAME ollama run $MODEL_NAME

echo "✅ Container started (or resumed) and model '$MODEL_NAME' is ready!"
echo "Access API at http://localhost:${HOST_PORT}"
