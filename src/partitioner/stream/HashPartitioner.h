#ifndef JASMINE_HASH_PARTITIONER_HEADER
#define JASMINE_HASH_PARTITIONER_HEADER

#include "Partitioner.h"
#include "../../util/logger/Logger.h"
#include "../../util/Utils.h"
#include <vector>
#include <string>
#include <map>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <thread>

class HashPartitioner {

    std::vector<Partition> partitions; // Holds partition objects

    std::atomic<bool> terminateConsumers;  // Termination flag
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

public:
    ~HashPartitioner();
    HashPartitioner(int numberOfPartitions, int graphID);
    void uploadGraphLocally(std::string masterIP);
    void writeSerializedPartitionFiles(int partition);
    void writeSerializedMasterFiles(int partition);
    void printStats();
    long getVertexCount();
    long getEdgeCount();
    void addEdgeCut(const pair<std::string, std::string> &edge, int index);
    void addLocalEdge(const pair<std::string, std::string> &edge, int index);

private:
    long vertexCount;
    long edgeCount;
    std::string outputFilePath;
    std::map<int, std::string> partitionFileMap;
    std::map<int, std::string> centralStoreFileList;
    std::map<int, std::string> centralStoreDuplicateFileList;
    std::map<int, std::string> partitionAttributeFileList;
    std::map<int, std::string> centralStoreAttributeFileList;
    std::map<int, std::string> compositeCentralStoreFileList;
    std::vector<std::map<int, std::string>> fullFileList;
    std::map<int, std::map<int, std::vector<int>>> partitionedLocalGraphStorageMap;

    std::vector<std::map<int, std::string>> generateFullFileList();
    void consumeLocalEdges(int partitionIndex);
    void consumeEdgeCuts(int partitionIndex);
    void stopConsumerThreads();
};

#endif  // !JASMINE_HASH_PARTITIONER_HEADER
