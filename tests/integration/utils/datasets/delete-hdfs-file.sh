#!/bin/bash

# Usage: ./delete_from_hdfs.sh <hdfs_file_path>

# Check for one argument
if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <hdfs_file_path>"
  exit 1
fi

HDFS_FILE_PATH=$1
DOCKER_CONTAINER_NAME="hdfs-namenode"

# Step 1: Delete the file from HDFS
echo "Deleting $HDFS_FILE_PATH from HDFS..."
docker exec -it "$DOCKER_CONTAINER_NAME" hadoop fs -rm -f "$HDFS_FILE_PATH"

if [ $? -eq 0 ]; then
  echo "✅ File $HDFS_FILE_PATH deleted successfully from HDFS!"
else
  echo "❌ Failed to delete $HDFS_FILE_PATH from HDFS."
  exit 1
fi
