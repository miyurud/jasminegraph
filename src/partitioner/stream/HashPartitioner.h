#ifndef JASMINE_HASH_PARTITIONER_HEADER
#define JASMINE_HASH_PARTITIONER_HEADER

#include "Partitioner.h"
#include "../../util/logger/Logger.h"
#include "../../util/Utils.h"
#include "../../server/JasmineGraphServer.h"
#include "../../nativestore/DataPublisher.h"
#include <vector>
#include <string>
#include <map>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <thread>

class HashPartitioner {

    std::vector<Partition> partitions;

    std::atomic<bool> terminateConsumers;
    std::vector<std::thread> localEdgeThreads;
    std::vector<std::thread> edgeCutThreads;

    std::vector<std::mutex> partitionLocks; // Array of mutexes for each partition
    std::vector<std::vector<std::pair<std::string, std::string>>> localEdgeArrays; // Array of arrays of edges for local partitioning
    std::vector<std::vector<std::pair<std::string, std::string>>> edgeCutsArrays;  // Array of arrays of edges for edge cuts

    std::vector<std::mutex> localEdgeMutexes;
    std::vector<std::condition_variable> edgeAvailableCV;
    std::vector<bool> edgeReady;

    std::vector<std::mutex> edgeCutsMutexes;
    std::vector<std::condition_variable> edgeCutsAvailableCV;
    std::vector<bool> edgeCutsReady;

    int numberOfPartitions;
    int graphId;
    std::string masterIp;

    std::vector<std::mutex> partitionMutexArray;

public:
    ~HashPartitioner();
    HashPartitioner(int numberOfPartitions, int graphID,std::string masterIp);
    long getVertexCount();
    long getEdgeCount();
    void addEdgeCut(const pair<std::string, std::string> &edge, int index);
    void addLocalEdge(const pair<std::string, std::string> &edge, int index);

private:
    long vertexCount;
    long edgeCount;
    std::string outputFilePath;

    void consumeLocalEdges(int partitionIndex,JasmineGraphServer::worker worker);
    void consumeEdgeCuts(int partitionIndex,JasmineGraphServer::worker worker);
    void stopConsumerThreads();
};

#endif  // !JASMINE_HASH_PARTITIONER_HEADER
