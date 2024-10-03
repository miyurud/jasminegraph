#include "HashPartitioner.h"
#include "../../server/JasmineGraphServer.h"

Logger hash_partitioner_logger;
std::mutex partitionFileMutex;
std::mutex centralStoreFileMutex;

HashPartitioner::HashPartitioner(int numberOfPartitions, int graphID)
        : numberOfPartitions(numberOfPartitions), graphId(graphID),
          partitionLocks(numberOfPartitions), vertexCount(0), edgeCount(0),
          localEdgeArrays(numberOfPartitions), edgeCutsArrays(numberOfPartitions),
          localEdgeMutexes(numberOfPartitions), edgeAvailableCV(numberOfPartitions),
          edgeReady(numberOfPartitions, false), edgeCutsMutexes(numberOfPartitions),
          edgeCutsAvailableCV(numberOfPartitions), edgeCutsReady(numberOfPartitions, false),
          terminateConsumers(false) {
    for (int i = 0; i < numberOfPartitions; i++) {
        this->partitions.push_back(Partition(i, numberOfPartitions));
    }
    // Start consumer threads and store them
    for (int i = 0; i < numberOfPartitions; ++i) {
        localEdgeThreads.emplace_back(&HashPartitioner::consumeLocalEdges, this, i);
        edgeCutThreads.emplace_back(&HashPartitioner::consumeEdgeCuts, this, i);
    }
    this->outputFilePath = Utils::getHomeDir() + "/.jasminegraph/tmp/" + std::to_string(this->graphId);
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
    while (!terminateConsumers) {  // Check for termination flag
        std::unique_lock<std::mutex> lock(localEdgeMutexes[partitionIndex]);
        edgeAvailableCV[partitionIndex].wait(lock, [this, partitionIndex] {
            return edgeReady[partitionIndex] || terminateConsumers;  // Break if termination is signaled
        });

        if (terminateConsumers) {
            hash_partitioner_logger.info("Shutting down Local edge consumer " + to_string(partitionIndex)+"\r\n");
            break;
        };  // Exit loop if termination flag is set

        // Process all edges in localEdgeArrays[partitionIndex]
        while (!localEdgeArrays[partitionIndex].empty()) {
            std::pair<std::string, std::string> edge = localEdgeArrays[partitionIndex].back();
            localEdgeArrays[partitionIndex].pop_back();
            threadEdgeCount++;
            if (threadEdgeCount % 100000 == 0) {
                hash_partitioner_logger.info(
                        "Local edge consumer thread " + to_string(partitionIndex) + " processed 100 thousand edges\r\n");
            }
            std::lock_guard<std::mutex> partitionLock(partitionLocks[partitionIndex]);
            partitions[partitionIndex].addEdge(edge);
        }
        edgeReady[partitionIndex] = false;  // Reset the flag after processing
    }
}

void HashPartitioner::consumeEdgeCuts(int partitionIndex) {
    int threadEdgeCount = 0;
    while (!terminateConsumers) {  // Check for termination flag
        std::unique_lock<std::mutex> lock(edgeCutsMutexes[partitionIndex]);
        edgeCutsAvailableCV[partitionIndex].wait(lock, [this, partitionIndex] {
            return edgeCutsReady[partitionIndex] || terminateConsumers;  // Break if termination is signaled
        });

        if (terminateConsumers) {
            hash_partitioner_logger.info("Shutting down Central edge consumer " + to_string(partitionIndex)+"\r\n");
            break;
        };  // Exit loop if termination flag is set

        while (!edgeCutsArrays[partitionIndex].empty()) {
            std::pair<std::string, std::string> edge = edgeCutsArrays[partitionIndex].back();
            edgeCutsArrays[partitionIndex].pop_back();
            threadEdgeCount++;
            if (threadEdgeCount % 100000 == 0) {
                hash_partitioner_logger.info(
                        "Central edge consumer thread " + to_string(partitionIndex) + " processed 100 thousand edges\r\n");
            }

            std::lock_guard<std::mutex> partitionLock(partitionLocks[partitionIndex]);
            partitions[partitionIndex].addToEdgeCuts(edge.first, edge.second, partitionIndex);
        }

        edgeCutsReady[partitionIndex] = false;  // Reset the flag after processing
    }
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
//        threads[count++] = std::thread(&HashPartitioner::writeSerializedDuplicateMasterFiles, this, i);
    }

    for (int i = 0; i < count; i++) {
        threads[i].join(); // Join threads to ensure serialization is complete
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
    std::string outputFilePartition = outputFilePath + "/" + std::to_string(this->graphId) + "_" + std::to_string(partition);

    std::map<int, std::vector<int>> partitionEdgeMap = partitions[partition].getLocalStorageMap();

    JasmineGraphHashMapLocalStore::storePartEdgeMap(partitionEdgeMap, outputFilePartition);

    Utils::compressFile(outputFilePartition);
    std::lock_guard<std::mutex> lock(partitionFileMutex);
    partitionFileMap[partition] = outputFilePartition + ".gz";
    hash_partitioner_logger.log("Serializing done for local part " + std::to_string(partition), "info");
}

void HashPartitioner::writeSerializedMasterFiles(int partition) {
    std::string outputFilePartMaster = outputFilePath + "/" + std::to_string(this->graphId) + "_centralstore_" + std::to_string(partition);

    std::map<int, std::vector<int>> partitionMasterEdgeMap = partitions[partition].getPartitionMasterEdgeMap();

    JasmineGraphHashMapCentralStore::storePartEdgeMap(partitionMasterEdgeMap, outputFilePartMaster);

    Utils::compressFile(outputFilePartMaster);
    std::lock_guard<std::mutex> lock(centralStoreFileMutex);
    centralStoreFileList[partition] = outputFilePartMaster + ".gz";
    hash_partitioner_logger.log("Serializing done for central part " + std::to_string(partition), "info");
}
