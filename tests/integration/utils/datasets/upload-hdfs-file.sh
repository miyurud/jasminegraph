#!/bin/bash
#Copyright 2025 JasmineGraph Team
#Licensed under the Apache License, Version 2.0 (the "License");
#you may not use this file except in compliance with the License.
#You may obtain a copy of the License at
#    http://www.apache.org/licenses/LICENSE-2.0
#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.
#

# Usage: ./upload_to_hdfs.sh <local_file_path> <hdfs_target_path>

# Check for two arguments
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <local_file_path> <hdfs_target_path>"
    exit 1
fi

LOCAL_FILE=$1
HDFS_PATH=$2
DOCKER_CONTAINER_NAME="hdfs-namenode"

# Step 1: Copy the file into the Hadoop container
echo "Copying $LOCAL_FILE into container..."
docker cp "$LOCAL_FILE" "${DOCKER_CONTAINER_NAME}:/tmp/"

FILENAME=$(basename "$LOCAL_FILE")

# Step 2: Upload the file to HDFS from inside the container
echo "Uploading $FILENAME to HDFS path $HDFS_PATH..."
docker exec -it "$DOCKER_CONTAINER_NAME" hadoop fs -mkdir -p "$HDFS_PATH"
docker exec -it "$DOCKER_CONTAINER_NAME" hadoop fs -put "/tmp/$FILENAME" "$HDFS_PATH"

# Optional: Remove the temp file inside container
docker exec -it "$DOCKER_CONTAINER_NAME" rm "/tmp/$FILENAME"

echo "File uploaded successfully to HDFS!"
