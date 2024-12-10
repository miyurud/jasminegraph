#include "HashPartitioner.h"
#include "../../server/JasmineGraphServer.h"
#include <nlohmann/json.hpp>

Logger hash_partitioner_logger;

int PARTITION_FILE_EDGE_COUNT_THRESHOLD = 1000000;

HashPartitioner::HashPartitioner(int numberOfPartitions, int graphID, std::string masterIp)
        : numberOfPartitions(numberOfPartitions), graphId(graphID),
          partitionLocks(numberOfPartitions), vertexCount(0), edgeCount(0),
          localEdgeArrays(numberOfPartitions), edgeCutsArrays(numberOfPartitions),
          localEdgeMutexes(numberOfPartitions), edgeAvailableCV(numberOfPartitions),
          edgeReady(numberOfPartitions, false), edgeCutsMutexes(numberOfPartitions),
          edgeCutsAvailableCV(numberOfPartitions), edgeCutsReady(numberOfPartitions, false),
          terminateConsumers(false), masterIp(masterIp), partitionMutexArray(numberOfPartitions) {

    this->outputFilePath = Utils::getHomeDir() + "/.jasminegraph/tmp/hdfsstore/" + std::to_string(this->graphId);
    Utils::createDirectory(Utils::getHomeDir() + "/.jasminegraph/tmp/hdfsstore");
    Utils::createDirectory(this->outputFilePath);

    JasmineGraphServer *server = JasmineGraphServer::getInstance();
    std::vector<JasmineGraphServer::worker> workers = server->workers(numberOfPartitions);

    // Start consumer threads and store them
    for (int i = 0; i < numberOfPartitions; i++) {
        this->partitions.push_back(Partition(i, numberOfPartitions));
        localEdgeThreads.emplace_back(&HashPartitioner::consumeLocalEdges, this, i, workers[i]);
        edgeCutThreads.emplace_back(&HashPartitioner::consumeEdgeCuts, this, i, workers[i]);
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

    for (auto &cv: edgeAvailableCV) {
        cv.notify_all();
    }
    for (auto &cv: edgeCutsAvailableCV) {
        cv.notify_all();
    }

    // Join all threads to ensure clean termination
    for (auto &thread: localEdgeThreads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    for (auto &thread: edgeCutThreads) {
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
            partitions[partitionIndex].addEdge(edge);
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

long HashPartitioner::getVertexCount() {
    if (this->vertexCount == 0 && this->edgeCount == 0) {
        int totalVertices = 0;
        int totalEdges = 0;
        int edgeCuts = 0;
        for (auto & partition : this->partitions) {
            totalVertices += partition.getVertextCount();
            totalEdges += partition.edgeC();
            edgeCuts += partition.edgeCutC();
        }
        this->vertexCount = totalVertices;
        this->edgeCount = totalEdges + edgeCuts / 2;
    } else if (this->vertexCount == 0) {
        int totalVertices = 0;
        for (auto & partition : this->partitions) {
            totalVertices += partition.getVertextCount();
        }
        this->vertexCount = totalVertices;
    }
    return this->vertexCount;
}

long HashPartitioner::getEdgeCount() {
    if (this->edgeCount == 0 && this->vertexCount == 0) {
        int totalVertices = 0;
        int totalEdges = 0;
        int edgeCuts = 0;
        for (auto & partition : this->partitions) {
            totalVertices += partition.getVertextCount();
            totalEdges += partition.edgeC();
            edgeCuts += partition.edgeCutC();
        }
        this->vertexCount = totalVertices;
        this->edgeCount = totalEdges + edgeCuts / 2;
    } else if (this->edgeCount == 0) {
        int totalEdges = 0;
        int edgeCuts = 0;
        for (auto & partition : this->partitions) {
            totalEdges += partition.edgeC();
            edgeCuts += partition.edgeCutC();
        }
        this->edgeCount = totalEdges + edgeCuts / 2;
    }
    return this->edgeCount;
}