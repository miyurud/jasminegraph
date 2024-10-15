#include "HashPartitioner.h"
#include "../../server/JasmineGraphServer.h"

Logger hash_partitioner_logger;
std::mutex partitionFileMutex;
std::mutex centralStoreFileMutex;

int PARTITION_FILE_EDGE_COUNT_THRESHOLD = 1000;

HashPartitioner::HashPartitioner(int numberOfPartitions, int graphID, std::string masterIp)
        : numberOfPartitions(numberOfPartitions), graphId(graphID),
          partitionLocks(numberOfPartitions), vertexCount(0), edgeCount(0),
          localEdgeArrays(numberOfPartitions), edgeCutsArrays(numberOfPartitions),
          localEdgeMutexes(numberOfPartitions), edgeAvailableCV(numberOfPartitions),
          edgeReady(numberOfPartitions, false), edgeCutsMutexes(numberOfPartitions),
          edgeCutsAvailableCV(numberOfPartitions), edgeCutsReady(numberOfPartitions, false),
          terminateConsumers(false), masterIp(masterIp) {
    this->outputFilePath = Utils::getHomeDir() + "/.jasminegraph/tmp/hdfsstore/" + std::to_string(this->graphId);
    Utils::createDirectory(Utils::getHomeDir() + "/.jasminegraph/tmp/hdfsstore");
    Utils::createDirectory(this->outputFilePath);

    JasmineGraphServer *server = JasmineGraphServer::getInstance();
    workers = server->workers(numberOfPartitions);

    for (int i = 0; i < numberOfPartitions; i++) {
        this->partitions.push_back(Partition(i, numberOfPartitions));
    }
    // Start consumer threads and store them
    for (int i = 0; i < numberOfPartitions; ++i) {
        localEdgeThreads.emplace_back(&HashPartitioner::consumeLocalEdges, this, i);
        edgeCutThreads.emplace_back(&HashPartitioner::consumeEdgeCuts, this, i);
    }
}

HashPartitioner::~HashPartitioner() {
    stopConsumerThreads();
}

void HashPartitioner::addLocalEdge(const std::pair<std::string, std::string> &edge, int index) {
    if (index < numberOfPartitions) {
        std::lock_guard<std::mutex> lock(localEdgeMutexes[index]);
        localEdgeArrays[index].push_back(edge);
        edgeReady[index] = true;  // Mark that there are new edges
        edgeAvailableCV[index].notify_one();  // Notify the consumer
    } else {
        hash_partitioner_logger.info("Invalid partition index in addLocalEdge");
    }
}

void HashPartitioner::addEdgeCut(const std::pair<std::string, std::string> &edge, int index) {
    if (index < numberOfPartitions) {
        std::lock_guard<std::mutex> lock(edgeCutsMutexes[index]);
        edgeCutsArrays[index].push_back(edge);
        edgeCutsReady[index] = true;  // Mark that there are new edge cuts
        edgeCutsAvailableCV[index].notify_one();  // Notify the consumer
    } else {
        hash_partitioner_logger.info("Invalid partition index in addEdgeCut");
    }
}

void HashPartitioner::stopConsumerThreads() {
    terminateConsumers = true;  // Set the termination flag to true

    // Notify all threads to wake up and check termination flag
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

void HashPartitioner::consumeLocalEdges(int partitionIndex) {
    int threadEdgeCount = 0;
    int fileIndex = 0;
    std::ofstream partitionFile;
    string fileName =
            this->outputFilePath + "/" + std::to_string(partitionIndex) + "_localstore_" + std::to_string(fileIndex);

    // Open the first partition file initially
    partitionFile.open(fileName);
    if (!partitionFile.is_open()) {
        hash_partitioner_logger.error("Error opening file for partition " + std::to_string(partitionIndex));
        return;
    }

    while (!terminateConsumers) {  // Check for termination flag
        std::unique_lock<std::mutex> lock(localEdgeMutexes[partitionIndex]);

        // Wait until there are edges available or the thread is signaled to terminate
        edgeAvailableCV[partitionIndex].wait(lock, [this, partitionIndex] {
            return edgeReady[partitionIndex] || terminateConsumers;
        });

        // If the consumer is terminating, close the file and break out of the loop
        if (terminateConsumers) {
            if (partitionFile.is_open()) {
                partitionFile.close();
                Utils::sendFileChunkToWorker(workers[partitionIndex].hostname, workers[partitionIndex].port,
                                             workers[partitionIndex].dataPort, fileName, masterIp, true,JasmineGraphInstanceProtocol::SEND_WORKER_LOCAL_FILE_CHUNK);
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
                Utils::sendFileChunkToWorker(workers[partitionIndex].hostname, workers[partitionIndex].port,
                                             workers[partitionIndex].dataPort, fileName, masterIp, false,JasmineGraphInstanceProtocol::SEND_WORKER_LOCAL_FILE_CHUNK);
                hash_partitioner_logger.info("Local edge consumer " + std::to_string(partitionIndex) +
                                             " generated file of " +
                                             std::to_string(PARTITION_FILE_EDGE_COUNT_THRESHOLD) +
                                             " edges: " + fileName);

                // Increment file index and open the next file
                fileIndex++;
                fileName = this->outputFilePath + "/" + std::to_string(partitionIndex) + "_localstore_" +
                           std::to_string(fileIndex);
                partitionFile.open(fileName);
                if (!partitionFile.is_open()) {
                    hash_partitioner_logger.error("Error opening file for partition " + std::to_string(partitionIndex));
                    break;
                }
            }

//             Add the edge to the partition structure
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

    hash_partitioner_logger.info("Local edge consumer " + std::to_string(partitionIndex) + " finished processing.");
}

void HashPartitioner::consumeEdgeCuts(int partitionIndex) {
    int threadEdgeCount = 0;
    int fileIndex = 0;
    std::ofstream edgeCutsFile;
    string fileName =
            this->outputFilePath + "/" + std::to_string(partitionIndex) + "_centralstore_" + std::to_string(fileIndex);

    // Open the first file for edge cuts
    edgeCutsFile.open(
            this->outputFilePath + "/" + std::to_string(partitionIndex) + "_" + std::to_string(fileIndex));
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
                hash_partitioner_logger.info("Central edge consumer " + std::to_string(partitionIndex) +
                                             " generated file of " +
                                             std::to_string(PARTITION_FILE_EDGE_COUNT_THRESHOLD) +
                                             " edges: " + fileName);

                // Open the next file
                fileIndex++;
                fileName = this->outputFilePath + "/" + std::to_string(partitionIndex) + "_centralstore_" +
                           std::to_string(fileIndex);
                edgeCutsFile.open(fileName);
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

    hash_partitioner_logger.info("Central edge consumer " + std::to_string(partitionIndex) + " finished processing.");
}

void HashPartitioner::printStats() {
    int totalVertices = 0;
    int totalLocalEdges = 0;
    int totalCentralEdges = 0;
    for (auto &partition: this->partitions) {
        totalVertices += partition.getVertextCount();
        totalLocalEdges += partition.getLocalEdgeCount();
        totalCentralEdges += partition.getEdgeCutCount();
    }
    this->vertexCount = totalVertices;
    this->edgeCount = totalLocalEdges + totalCentralEdges / 2;
    hash_partitioner_logger.info("Total vertices: " + std::to_string(this->vertexCount));
    hash_partitioner_logger.info("Total edges: " + std::to_string(this->edgeCount));
}

long HashPartitioner::getVertexCount() {
    if (this->vertexCount == 0) {
        int totalVertices = 0;
        int totalEdges = 0;
        for (auto &partition: this->partitions) {
            totalVertices += partition.getVertextCount();
            totalEdges += partition.getEdgesCount();
        }
        this->vertexCount = totalVertices;
        this->edgeCount = totalEdges;
    }
    return this->vertexCount;
}

long HashPartitioner::getEdgeCount() {
    if (this->edgeCount == 0) {
        int totalVertices = 0;
        int totalEdges = 0;
        for (auto &partition: this->partitions) {
            totalVertices += partition.getVertextCount();
            totalEdges += partition.getEdgesCount();
        }
        this->vertexCount = totalVertices;
        this->edgeCount = totalEdges;
    }
    return this->edgeCount;
}

std::vector<std::map<int, std::string>> HashPartitioner::generateFullFileList() {
    hash_partitioner_logger.info("Generating full file list..");
    std::thread threads[numberOfPartitions * 3];
    int count = 0;
    for (int i = 0; i < numberOfPartitions; i++) {
        threads[count++] = std::thread(&HashPartitioner::writeSerializedPartitionFiles, this, i);
        threads[count++] = std::thread(&HashPartitioner::writeSerializedMasterFiles, this, i);
    }

    for (int i = 0; i < count; i++) {
        threads[i].join();
    }

    // Generate full file list
    this->fullFileList.push_back(this->partitionFileMap);
    this->fullFileList.push_back(this->centralStoreFileList);
    this->fullFileList.push_back(this->centralStoreDuplicateFileList);
    this->fullFileList.push_back(this->partitionAttributeFileList);
    this->fullFileList.push_back(this->centralStoreAttributeFileList);
    this->fullFileList.push_back(this->compositeCentralStoreFileList);

    hash_partitioner_logger.info("Successfully generated full file list");
    return this->fullFileList;
}

void HashPartitioner::uploadGraphLocally(std::string masterIP) {
    hash_partitioner_logger.info("Uploading graph locally..");
    JasmineGraphServer *server = JasmineGraphServer::getInstance();
    server->uploadGraphLocally(graphId, Conts::GRAPH_TYPE_NORMAL, generateFullFileList(), masterIP);
    hash_partitioner_logger.info("Successfully uploaded the graph.");
}

void HashPartitioner::writeSerializedPartitionFiles(int partition) {
    std::string outputFilePartition =
            outputFilePath + "/" + std::to_string(this->graphId) + "_" + std::to_string(partition);

    std::map<int, std::vector<int>> partitionEdgeMap = partitions[partition].getLocalStorageMap();

    JasmineGraphHashMapLocalStore::storePartEdgeMap(partitionEdgeMap, outputFilePartition);

    Utils::compressFile(outputFilePartition);
    std::lock_guard<std::mutex> lock(partitionFileMutex);
    partitionFileMap[partition] = outputFilePartition + ".gz";
    hash_partitioner_logger.log("Serializing done for local part " + std::to_string(partition), "info");
}

void HashPartitioner::writeSerializedMasterFiles(int partition) {
    std::string outputFilePartMaster =
            outputFilePath + "/" + std::to_string(this->graphId) + "_centralstore_" + std::to_string(partition);

    std::map<int, std::vector<int>> partitionMasterEdgeMap = partitions[partition].getPartitionMasterEdgeMap();

    JasmineGraphHashMapCentralStore::storePartEdgeMap(partitionMasterEdgeMap, outputFilePartMaster);

    Utils::compressFile(outputFilePartMaster);
    std::lock_guard<std::mutex> lock(centralStoreFileMutex);
    centralStoreFileList[partition] = outputFilePartMaster + ".gz";
    hash_partitioner_logger.log("Serializing done for central part " + std::to_string(partition), "info");
}



