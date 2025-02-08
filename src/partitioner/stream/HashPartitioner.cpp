/*
 * Copyright 2024 JasminGraph Team
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "HashPartitioner.h"
#include "../../server/JasmineGraphServer.h"
#include <nlohmann/json.hpp>

Logger hash_partitioner_logger;

int PARTITION_FILE_EDGE_COUNT_THRESHOLD = 1000000;

HashPartitioner::HashPartitioner(int numberOfPartitions, int graphID, std::string masterIp, bool isDirected)
        : numberOfPartitions(numberOfPartitions), graphId(graphID),
          partitionLocks(numberOfPartitions), vertexCount(0), edgeCount(0),
          localEdgeArrays(numberOfPartitions), edgeCutsArrays(numberOfPartitions),
          localEdgeMutexes(numberOfPartitions), edgeAvailableCV(numberOfPartitions),
          edgeReady(numberOfPartitions, false), edgeCutsMutexes(numberOfPartitions),
          edgeCutsAvailableCV(numberOfPartitions), edgeCutsReady(numberOfPartitions, false),
          terminateConsumers(false), masterIp(masterIp), partitionMutexArray(numberOfPartitions),
          isDirected(isDirected) {
    this->outputFilePath = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.hdfs.tempfolder")
            + "/" + std::to_string(this->graphId);
    Utils::createDirectory(this->outputFilePath);

    JasmineGraphServer *server = JasmineGraphServer::getInstance();
    std::vector<JasmineGraphServer::worker> workers = server->workers(numberOfPartitions);

    // Start consumer threads and store them
    for (int i = 0; i < numberOfPartitions; i++) {
        this->partitions.push_back(Partition(i, numberOfPartitions));
        localEdgeThreads.emplace_back(&HashPartitioner::consumeLocalEdges, this, i, workers[i]);
        edgeCutThreads.emplace_back(&HashPartitioner::consumeEdgeCuts, this, i, workers[i]);
        Utils::assignPartitionToWorker(graphId,i,workers.at(i).hostname,workers.at(i).port);
    }
}

HashPartitioner::~HashPartitioner() {
    stopConsumerThreads();
}

void HashPartitioner::addLocalEdge(const std::pair<std::string, std::string> &edge, int index) {
    if (index < numberOfPartitions) {
        std::lock_guard<std::mutex> lock(localEdgeMutexes[index]);
        localEdgeArrays[index].push_back(edge);
        edgeReady[index] = true;
        edgeAvailableCV[index].notify_one();
    } else {
        hash_partitioner_logger.info("Invalid partition index in addLocalEdge");
    }
}

void HashPartitioner::addEdgeCut(const std::pair<std::string, std::string> &edge, int index) {
    if (index < numberOfPartitions) {
        std::lock_guard<std::mutex> lock(edgeCutsMutexes[index]);
        edgeCutsArrays[index].push_back(edge);
        edgeCutsReady[index] = true;
        edgeCutsAvailableCV[index].notify_one();
    } else {
        hash_partitioner_logger.info("Invalid partition index in addEdgeCut");
    }
}

void HashPartitioner::stopConsumerThreads() {
    terminateConsumers = true;

    for (auto &cv : edgeAvailableCV) {
        cv.notify_all();
    }
    for (auto &cv : edgeCutsAvailableCV) {
        cv.notify_all();
    }

    // Join all threads to ensure clean termination
    for (auto &thread : localEdgeThreads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    for (auto &thread : edgeCutThreads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

void HashPartitioner::consumeLocalEdges(int partitionIndex, JasmineGraphServer::worker worker) {
    int threadEdgeCount = 0;
    int fileIndex = 0;
    std::ofstream partitionFile;
    string fileName =
            std::to_string(graphId) + "_" + std::to_string(partitionIndex) + "_localstore_" + std::to_string(fileIndex);
    string filePath =
            this->outputFilePath + "/" + fileName;

    // Open the first partition file initially
    partitionFile.open(filePath);
    if (!partitionFile.is_open()) {
        hash_partitioner_logger.error("Error opening file for partition " + std::to_string(partitionIndex));
        return;
    }

    while (true) {  // Check for termination flag
        std::unique_lock<std::mutex> lock(localEdgeMutexes[partitionIndex]);

        // Wait until there are edges available or the thread is signaled to terminate
        edgeAvailableCV[partitionIndex].wait(lock, [this, partitionIndex] {
            return edgeReady[partitionIndex] || terminateConsumers;
        });

        // If the consumer is terminating, close the file and break out of the loop
        if (terminateConsumers) {
            if (partitionFile.is_open()) {
                partitionFile.close();
                hash_partitioner_logger.debug("Local edge consumer " + std::to_string(partitionIndex) +
                                              " generated file of " +
                                              std::to_string(threadEdgeCount) +
                                              " edges: " + filePath);
                partitionMutexArray[partitionIndex].lock();
                Utils::sendFileChunkToWorker(worker.hostname, worker.port, worker.dataPort, filePath, masterIp,
                                             JasmineGraphInstanceProtocol::HDFS_LOCAL_STREAM_START);
                partitionMutexArray[partitionIndex].unlock();
            }
            break;
        }

        // Process the edges from the local array
        while (!localEdgeArrays[partitionIndex].empty()) {
            std::pair<std::string, std::string> edge = localEdgeArrays[partitionIndex].back();
            localEdgeArrays[partitionIndex].pop_back();

            // Write the edge to the current partition file
            partitionFile << edge.first << " " << edge.second << std::endl;
            threadEdgeCount++;

            // Check if the edge count has reached the threshold
            if (threadEdgeCount == PARTITION_FILE_EDGE_COUNT_THRESHOLD) {
                threadEdgeCount = 0;
                partitionFile.close();  // Close the file after reaching the threshold

                partitionMutexArray[partitionIndex].lock();
                Utils::sendFileChunkToWorker(worker.hostname, worker.port, worker.dataPort, filePath, masterIp,
                                             JasmineGraphInstanceProtocol::HDFS_LOCAL_STREAM_START);
                partitionMutexArray[partitionIndex].unlock();

                hash_partitioner_logger.debug("Local edge consumer " + std::to_string(partitionIndex) +
                                              " generated file of " +
                                              std::to_string(PARTITION_FILE_EDGE_COUNT_THRESHOLD) +
                                              " edges: " + filePath);

                // Increment file index and open the next file
                fileIndex++;
                fileName = std::to_string(graphId) + "_" + std::to_string(partitionIndex) + "_localstore_" +
                           std::to_string(fileIndex);
                filePath = this->outputFilePath + "/" + fileName;
                partitionFile.open(filePath);
                if (!partitionFile.is_open()) {
                    hash_partitioner_logger.error("Error opening file for partition " + std::to_string(partitionIndex));
                    break;
                }
            }

            std::lock_guard<std::mutex> partitionLock(partitionLocks[partitionIndex]);
            partitions[partitionIndex].addEdge(edge,isDirected);
        }

        // Reset the flag after processing the current batch of edges
        edgeReady[partitionIndex] = false;
    }

    // Ensure the file is closed if it remains open
    if (partitionFile.is_open()) {
        partitionFile.close();
    }
    hash_partitioner_logger.debug("Local edge consumer " + std::to_string(partitionIndex) + " finished processing.");
}

void HashPartitioner::consumeEdgeCuts(int partitionIndex, JasmineGraphServer::worker worker) {
    int threadEdgeCount = 0;
    int fileIndex = 0;
    std::ofstream edgeCutsFile;
    string fileName = std::to_string(graphId) + "_" + std::to_string(partitionIndex) + "_centralstore_" +
                      std::to_string(fileIndex);
    string filePath =
            this->outputFilePath + "/" + fileName;

    // Open the first file for edge cuts
    edgeCutsFile.open(filePath);
    if (!edgeCutsFile.is_open()) {
        hash_partitioner_logger.error("Error opening edge cuts file for partition " + std::to_string(partitionIndex));
        return;
    }

    while (!terminateConsumers) {  // Check for termination flag
        std::unique_lock<std::mutex> lock(edgeCutsMutexes[partitionIndex]);
        edgeCutsAvailableCV[partitionIndex].wait(lock, [this, partitionIndex] {
            return edgeCutsReady[partitionIndex] || terminateConsumers;  // Break if termination is signaled
        });

        if (terminateConsumers) {
            if (edgeCutsFile.is_open()) {
                edgeCutsFile.close();
                hash_partitioner_logger.debug("Central edge consumer " + std::to_string(partitionIndex) +
                                              " generated file of " +
                                              std::to_string(threadEdgeCount) +
                                              " edges: " + filePath);
                partitionMutexArray[partitionIndex].lock();
                Utils::sendFileChunkToWorker(worker.hostname, worker.port, worker.dataPort, filePath, masterIp,
                                             JasmineGraphInstanceProtocol::HDFS_CENTRAL_STREAM_START);
                partitionMutexArray[partitionIndex].unlock();
            }
            break;
        }

        // Process edges from edgeCutsArrays
        while (!edgeCutsArrays[partitionIndex].empty()) {
            std::pair<std::string, std::string> edge = edgeCutsArrays[partitionIndex].back();
            edgeCutsArrays[partitionIndex].pop_back();

            // Write the edge to the file
            edgeCutsFile << edge.first << " " << edge.second << std::endl;
            threadEdgeCount++;

            // If threshold reached, close current file and open a new one
            if (threadEdgeCount == PARTITION_FILE_EDGE_COUNT_THRESHOLD) {
                threadEdgeCount = 0;
                edgeCutsFile.close();

                partitionMutexArray[partitionIndex].lock();
                Utils::sendFileChunkToWorker(worker.hostname, worker.port, worker.dataPort, filePath, masterIp,
                                             JasmineGraphInstanceProtocol::HDFS_CENTRAL_STREAM_START);
                partitionMutexArray[partitionIndex].unlock();

                hash_partitioner_logger.debug("Central edge consumer " + std::to_string(partitionIndex) +
                                              " generated file of " +
                                              std::to_string(PARTITION_FILE_EDGE_COUNT_THRESHOLD) +
                                              " edges: " + filePath);

                // Open the next file
                fileIndex++;
                fileName = std::to_string(graphId) + "_" + std::to_string(partitionIndex) + "_centralstore_" +
                           std::to_string(fileIndex);
                filePath = this->outputFilePath + "/" + fileName;
                edgeCutsFile.open(filePath);
                if (!edgeCutsFile.is_open()) {
                    hash_partitioner_logger.error(
                            "Error opening edge cuts file for partition " + std::to_string(partitionIndex));
                    break;
                }
            }

            // Add edge cuts to the partition
            std::lock_guard<std::mutex> partitionLock(partitionLocks[partitionIndex]);
            partitions[partitionIndex].addToEdgeCuts(edge.first, edge.second, partitionIndex);
        }

        edgeCutsReady[partitionIndex] = false;  // Reset the flag after processing
    }

//     Ensure the file is closed when the consumer is done
    if (edgeCutsFile.is_open()) {
        edgeCutsFile.close();
    }
    hash_partitioner_logger.debug("Central edge consumer " + std::to_string(partitionIndex) + " finished processing.");
}

void HashPartitioner::updatePartitionTable() {
    auto *sqlite = new SQLiteDBInterface();
    sqlite->init();

    std::mutex dbLock;
    for (int i = 0; i < numberOfPartitions; i++) {
        string sqlStatement =
                "INSERT INTO partition (idpartition,graph_idgraph,vertexcount,central_vertexcount,edgecount) VALUES(\""
                + std::to_string(i) + "\", \"" + std::to_string(this->graphId) + "\", \"" +
                std::to_string(partitions.at(i).getVertextCount()) +
                "\",\"" + std::to_string(partitions.at(i).getVertextCount()) + "\",\"" +
                std::to_string(partitions.at(i).getEdgesCount(isDirected)) + "\")";

        dbLock.lock();
        sqlite->runUpdate(sqlStatement);
        dbLock.unlock();

    }

    sqlite->finalize();
    delete sqlite;
}

long HashPartitioner::getVertexCount() {
    int totalVertices = 0;
    for (auto & partition : this->partitions) {
        totalVertices += partition.getVertextCount();
    }
    return totalVertices;
}

long HashPartitioner::getEdgeCount() {
    int totalEdges = 0;
    int edgeCuts = 0;
    for (auto & partition : this->partitions) {
        totalEdges += partition.getEdgesCount(isDirected);
        edgeCuts += partition.edgeCutsCount();
    }
   return  totalEdges + edgeCuts / 2;
}
