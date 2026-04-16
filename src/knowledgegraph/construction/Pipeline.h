/**
Copyright 2025 JasmineGraph Team
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */
#ifndef JASMINEGRAPH_HDFSPIPELINE_H
#define JASMINEGRAPH_HDFSPIPELINE_H
#include <hdfs.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <queue>

#include "../../server/JasmineGraphServer.h"

struct Chunk {
    std::string doc_id;
    std::string text;
    int64_t chunk_size;
};

class Pipeline {
 public:
    Pipeline(hdfsFS fileSystem, const std::string& filePath, int numberOfPartitions, int graphId,

             std::string masterIP, vector<JasmineGraphServer::worker>& workerList, std::vector<std::string> llmRunners);
    Pipeline(int connFd, hdfsFS fileSystem, const std::string& filePath, int numberOfPartitions, int graphId,
             std::string masterIP, vector<JasmineGraphServer::worker>& workerList, std::vector<std::string> llmRunners,
             std::string llmInferenceEngine, std::string llm, string chunkSize, std::string chunksPerBatch,
             long startFromBytes);
    Pipeline(int connFd,
               const std::string& filePath,
               int numberOfPartitions,
               int graphId,
               const std::string& masterIP,
               std::vector<JasmineGraphServer::worker>& workerList,
               const std::vector<std::string>& llmRunners,
               const std::string& llmInferenceEngine,
               const std::string& llm,
               const std::string& chunkSize,
               const std::string& chunksPerBatch,
               long startFromBytes);


    void init();
    void startStreamingFromBufferToPartitions();


    static bool streamGraphToDesignatedWorker(std::string host, int port, std::string masterIP, std::string graphId,
                                              int numberOfPartitions, std::string hdfsServerIp, std::string hdfsPort,
                                              std::string hostNamePort, std::string llmInferenceEngine, std::string llm,
                                              std::string chunkSize, std::string hdfsFilePath,
                                              bool continueKGConstruction, SQLiteDBInterface* sqlite,
                                              shared_ptr<atomic<bool>>& stopFlag,
                                              shared_ptr<KGConstructionRate>& kgConstructionRates);
     static bool streamLocalGraphToDesignatedWorker(string host, int port, int dataPort, string masterIP,
                                                    string graphId, int numberOfPartitions, string hostNamePort,
                                                   string llmInferenceEngine, string llm, string chunkSize,
                                                   string localFilePath, bool continueKGConstruction,
                                                   SQLiteDBInterface* sqlite, shared_ptr<atomic<bool>>& stopFlag,
                                                   shared_ptr<KGConstructionRate>& kgConstructionRates);

 private:
    void streamFromHDFSIntoBuffer();
    void streamFromLocalFileIntoBuffer();
    void streamChunckToWorker(const std::string& chunk, int partitionId);
    void startStreamingFromBufferToWorkers();
    json processTupleAndSaveInPartition(const std::vector<std::unique_ptr<SharedBuffer>>& tupleBuffer);
    void extractTuples(std::string host, int port, std::string masterIP, int graphID, int partitionId,
    std::queue<Chunk>& dataBuffer, SharedBuffer& sharedBuffer);

    hdfsFS fileSystem;

    std::string filePath;
    std::queue<Chunk> dataBuffer;

    std::mutex dataBufferMutex;
    std::mutex realTimeBytesMutex;
    std::mutex realTimeBytesUpdateMutex;
    std::mutex dataBufferMutexForWorker;
    std::condition_variable dataBufferCV;

    std::string masterIP;
    SQLiteDBInterface* sqlite;

    bool isReading;
    bool isProcessing;
    bool isDirected;
    bool isEdgeListType;
    int graphId;
    int numberOfPartitions;
    vector<JasmineGraphServer::worker>& workerList;
    std::vector<std::string> llmRunners;
    std::string llmInferenceEngine;
    std::string llm;
    int connFd;
    std::mutex dbLock;
    long startFromBytes;
    std::string chunkSize;
    std::string chunksPerBatch;
    int64_t bytes_read_so_far = 0;
    int64_t realtime_bytes_read_so_far = 0;
    bool stopFlag = false;
    std::unordered_map<std::string, long> nodeIndex;
    std::unordered_map<std::string, long> edgeIndex;

    unsigned int nextNodeIndex = 0;
    unsigned int nextEdgeIndex = 0;
    std::mutex entityResolutionMutex;

    string currentTraceContext;
    std::atomic<bool> metaThreadRunning{true};
    std::atomic<long> vertexCount{0};
    std::atomic<long> edgeCount{0};
};

#endif  // JASMINEGRAPH_HDFSPIPELINE_H
