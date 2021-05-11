/**
Copyright 2019 JasmineGraph Team
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

#ifndef JASMINEGRAPH_JASMINEGRAPHSERVER_H
#define JASMINEGRAPH_JASMINEGRAPHSERVER_H

#include <map>
#include <thread>
#include <stdio.h>
#include <stdlib.h>
#include "../frontend/JasmineGraphFrontEnd.h"
#include "../backend/JasmineGraphBackend.h"
#include "../metadb/SQLiteDBInterface.h"
#include "../performancedb/PerformanceSQLiteDBInterface.h"
#include "../util/Conts.h"

using std::map;

class JasmineGraphServer {
private:
    map<std::string, long> hostPlaceMap;
    std::string profile;
    std::string workerHosts;
    std::string enableNmon;
    static const int BUFFER_SIZE = 128;
    int serverPort;
    int serverDataPort;
    std::map<std::string, std::vector<int>> workerPortsMap;
    std::map<std::string, std::vector<int>> workerDataPortsMap;

    static void startRemoteWorkers(std::vector<int> workerPortsVector, std::vector<int> workerDataPortsVector, std::string host, string profile, string masterHost, string enableNmon);

    void addHostsToMetaDB(std::string host, std::vector<int> portVector, std::vector<int> dataPortVector);

    void updateOperationalGraphList();

    std::map<std::string, std::string> getLiveHostIDList();

    static void copyArtifactsToWorkers(std::string workerPath, std::string artifactLocation, std::string remoteWorker);
    static void createWorkerPath (std::string workerHost, std::string workerPath);
    static void createLogFilePath (std::string workerHost, std::string workerPath);
    static void deleteWorkerPath (std::string workerHost, std::string workerPath);

    static bool hasEnding(std::string const &fullString, std::string const &ending);
    std::vector<std::string> getWorkerVector(std::string workerList);
    void deleteNonOperationalGraphFragment(int graphID);
public:
    ~JasmineGraphServer();

    JasmineGraphServer();

    void init();

    void start_workers();

    void waitForAcknowledgement(int numberOfWorkers);

    void resolveOperationalGraphs();

    void backupPerformanceDB();

    void clearPerformanceDB();

    void addInstanceDetailsToPerformanceDB(std::string host, std::vector<int> portVector, std::string isMaster);

    int shutdown_workers();

    int run(std::string profile, std::string masterIp, int numberofWorkers, std::string workerIps, std::string enableNmon);

    bool isRunning();

    void uploadGraphLocally(int graphID, const std::string graphType, std::vector<std::map<int,std::string>> fullFileList, std::string masterIP);

    void removeGraph(std::vector<std::pair<std::string, std::string>> hostHasPartition, std::string graphID, std::string masterIP);

    void assignPartitionsToWorkers(int numberOfWorkers);

    static bool batchUploadFile(std::string host, int port, int dataPort, int graphID, std::string filePath, std::string masterIP);

    static bool batchUploadCentralStore(std::string host, int port, int dataPort, int graphID, std::string filePath, std::string masterIP);

    static void copyCentralStoreToAggregateLocation(std::string filePath);

    static bool batchUploadAttributeFile(std::string host, int port, int dataPort, int graphID, std::string filePath, std::string masterIP);

    static bool batchUploadCentralAttributeFile(std::string host, int port, int dataPort, int graphID, std::string filePath, std::string masterIP);

    static bool batchUploadCompositeCentralstoreFile(std::string host, int port, int dataPort, int graphID, std::string filePath, std::string masterIP);
sendFileThroughService
    static int removePartitionThroughService(std::string host, int port, std::string graphID, std::string partitionID, std::string masterIP);

    static int removeFragmentThroughService(std::string host, int port, std::string graphID, std::string masterIP);

    static int initiateEntityResolution(std::vector<std::pair<std::string, std::string>> hostHasPartition, std::string graphID, std::string masterIP);

    static int createBloomFilters(std::string host, int port, std::string graphID, std::string partitionID, std::string masterIP);

    static int bucketLocalClusers(std::string host, int port, std::string graphID, std::vector<int> clusters, std::string masterIP);

    static bool sendFileThroughService(std::string host, int dataPort, std::string fileName, std::string filePath, std::string masterIP);

    void assignPartitionToWorker (std::string fileName, int graphId, std::string workerHost, int workerPort, int workerDataPort);

    bool spawnNewWorker(string host, string port, string dataPort, string profile, string masterHost, string enableNmon);

    JasmineGraphFrontEnd *frontend;
    SQLiteDBInterface sqlite;
    PerformanceSQLiteDBInterface performanceSqlite;
    JasmineGraphBackend *backend;
    std::string masterHost;
    int numberOfWorkers = -1;

    struct workers {
        std::string hostname;
        int port;
        int dataPort;
    };

    struct workerPartitions {
        int port;
        int dataPort;
        std::vector<std::string> partitionID;
    };

    static void updateMetaDB(std::vector<workers> hostWorkerMap,  std::map<int,std::string> partitionFileList, int graphID,
                 std::string uploadEndTime);

    //return hostWorkerMap
    static std::vector<JasmineGraphServer::workers> getHostWorkerMap();

    std::map<std::string, workerPartitions> getGraphPartitionedHosts(std::string graphID);
};


#endif //JASMINEGRAPH_JASMINEGRAPHSERVER_H
