/**
Copyright 2020-2024 JasmineGraph Team
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

#include "JasmineGraphServer.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>

#include <iostream>
#include <map>
#include <string>

#include "../../globals.h"
#include "../k8s/K8sWorkerController.h"
#include "../ml/trainer/JasmineGraphTrainingSchedular.h"
#include "../scale/scaler.h"
#include "JasmineGraphInstance.h"
#include "JasmineGraphInstanceProtocol.h"

Logger server_logger;

static void copyArtifactsToWorkers(const std::string &workerPath, const std::string &artifactLocation,
                                   const std::string &remoteWorker);
static void createLogFilePath(const std::string &workerHost, const std::string &workerPath);
static void deleteWorkerPath(const std::string &workerHost, const std::string &workerPath);
static void assignPartitionToWorker(std::string fileName, int graphId, std::string workerHost, int workerPort);
static void updateMetaDB(int graphID, std::string uploadEndTime);
static bool batchUploadFile(std::string host, int port, int dataPort, int graphID, std::string filePath,
                            std::string masterIP);
static bool batchUploadCentralStore(std::string host, int port, int dataPort, int graphID, std::string filePath,
                                    std::string masterIP);
static bool batchUploadAttributeFile(std::string host, int port, int dataPort, int graphID, std::string filePath,
                                     std::string masterIP);
static bool batchUploadCentralAttributeFile(std::string host, int port, int dataPort, int graphID, std::string filePath,
                                            std::string masterIP);
static bool batchUploadCompositeCentralstoreFile(std::string host, int port, int dataPort, int graphID,
                                                 std::string filePath, std::string masterIP);
static bool removeFragmentThroughService(string host, int port, string graphID, string masterIP);
static bool removePartitionThroughService(string host, int port, string graphID, string partitionID, string masterIP);
static bool initiateCommon(std::string host, int port, std::string trainingArgs, int iteration, std::string masterIP,
                           std::string initType);
static bool initiateTrain(std::string host, int port, std::string trainingArgs, int iteration, std::string masterIP);
static bool initiatePredict(std::string host, int port, std::string trainingArgs, int iteration, std::string masterIP);
static bool initiateServer(std::string host, int port, std::string trainingArgs, int iteration, std::string masterIP);
static bool initiateClient(std::string host, int port, std::string trainingArgs, int iteration, std::string masterIP);
static bool initiateAggregator(std::string host, int port, std::string trainingArgs, int iteration,
                               std::string masterIP);
static bool initiateOrgServer(std::string host, int port, std::string trainingArgs, int iteration,
                              std::string masterIP);
static void degreeDistributionCommon(std::string graphID, std::string command);
static int getPortByHost(const std::string &host);
static int getDataPortByHost(const std::string &host);
static size_t getWorkerCount();

static std::vector<JasmineGraphServer::worker> hostWorkerList;
static unordered_map<string, pair<int, int>> hostPortMap;
std::map<int, int> aggregateWeightMap;

static int graphUploadWorkerTracker = 0;

void *runfrontend(void *dummyPt) {
    JasmineGraphServer *refToServer = (JasmineGraphServer *)dummyPt;
    refToServer->frontend = new JasmineGraphFrontEnd(refToServer->sqlite, refToServer->performanceSqlite,
                                                     refToServer->masterHost, refToServer->jobScheduler);
    refToServer->frontend->run();
    delete refToServer->frontend;
    return NULL;
}

void *runbackend(void *dummyPt) {
    JasmineGraphServer *refToServer = (JasmineGraphServer *)dummyPt;
    refToServer->backend = new JasmineGraphBackend(refToServer->sqlite, refToServer->numberOfWorkers);
    refToServer->backend->run();
    delete refToServer->backend;
    return NULL;
}

JasmineGraphServer::JasmineGraphServer() {
    this->sqlite = new SQLiteDBInterface();
    this->sqlite->init();
    this->performanceSqlite = new PerformanceSQLiteDBInterface();
    this->performanceSqlite->init();
    this->jobScheduler = new JobScheduler(this->sqlite, this->performanceSqlite);
    this->jobScheduler->init();
}

static std::mutex instanceMutex;
JasmineGraphServer *JasmineGraphServer::getInstance() {
    static JasmineGraphServer *instance = nullptr;
    if (instance == nullptr) {
        instanceMutex.lock();
        if (instance == nullptr) {  // double-checking lock
            instance = new JasmineGraphServer();
        }
        instanceMutex.unlock();
    }
    return instance;
}

JasmineGraphServer::~JasmineGraphServer() {
    this->sqlite->finalize();
    this->performanceSqlite->finalize();
    delete this->sqlite;
    delete this->performanceSqlite;
    delete this->jobScheduler;
}

int JasmineGraphServer::run(std::string masterIp, int numberofWorkers, std::string workerIps, std::string enableNmon) {
    server_logger.info("Running the server...");
    std::vector<int> masterPortVector;
    if (masterIp.empty()) {
        this->masterHost = Utils::getJasmineGraphProperty("org.jasminegraph.server.host");
    } else {
        this->masterHost = masterIp;
    }
    server_logger.info("masterHost = " + this->masterHost);
    this->numberOfWorkers = numberofWorkers;
    this->workerHosts = workerIps;
    this->enableNmon = enableNmon;
    masterPortVector.push_back(Conts::JASMINEGRAPH_FRONTEND_PORT);
    updateOperationalGraphList();

    if (jasminegraph_profile == PROFILE_K8S) {
        // Create K8s worker controller
        (void)K8sWorkerController::getInstance(masterIp, numberofWorkers, sqlite);
        start_scale_down(this->sqlite);
        hostWorkerList = K8sWorkerController::workerList;
    } else {
        start_workers();
        addInstanceDetailsToPerformanceDB(masterHost, masterPortVector, "true");
    }

    init();
    std::thread *myThreads = new std::thread[1];
    myThreads[0] = std::thread(StatisticCollector::logLoadAverage, "Load Average");
    sleep(1);
    waitForAcknowledgement(numberofWorkers);
    resolveOperationalGraphs();
    initiateAggregateMap();
    return 0;
}

void JasmineGraphServer::init() {
    pthread_t frontendthread;
    pthread_t backendthread;
    pthread_create(&frontendthread, NULL, runfrontend, this);
    pthread_detach(frontendthread);
    pthread_create(&backendthread, NULL, runbackend, this);
    pthread_detach(backendthread);
}

void JasmineGraphServer::start_workers() {
    // Not used in K8s mode
    int hostListModeNWorkers = 0;
    int numberOfWorkersPerHost;
    std::vector<std::string> hostsList;
    std::string nWorkers;
    if (jasminegraph_profile == PROFILE_NATIVE) {
        hostsList = Utils::getHostListFromProperties();
        if ((this->numberOfWorkers) == -1) {
            nWorkers = Utils::getJasmineGraphProperty("org.jasminegraph.server.nworkers");
        }
        enableNmon = Utils::getJasmineGraphProperty("org.jasminegraph.server.enable.nmon");
    } else if (jasminegraph_profile == PROFILE_DOCKER) {
        hostsList = getWorkerVector(workerHosts);
    }

    if (hostsList.size() == 0) {
        server_logger.error("At least one host needs to be specified");
        exit(-1);
    }

    this->sqlite->runUpdate("DELETE FROM host");

    std::vector<std::string>::iterator it;
    std::string hostString = "";
    std::string sqlString = "INSERT INTO host (idhost,name,ip,is_public) VALUES ";
    int counter = 0;

    for (it = hostsList.begin(); it < hostsList.end(); it++) {
        std::string hostItem = (*it);
        std::string ip_address = "";
        std::string user = "";
        if (hostItem.find('@') != std::string::npos) {
            vector<string> splitted = Utils::split(hostItem, '@');
            ip_address = splitted[1];
            user = splitted[0];
        } else {
            ip_address = hostItem;
        }
        hostString += "(" + std::to_string(counter) + ", '" + ip_address + "', '" + ip_address + "', 'false'),";
        counter++;
    }

    hostString = hostString.substr(0, hostString.length() - 1);
    sqlString = sqlString + hostString;
    this->sqlite->runInsert(sqlString);

    int workerPort = Conts::JASMINEGRAPH_INSTANCE_PORT;
    int workerDataPort = Conts::JASMINEGRAPH_INSTANCE_DATA_PORT;

    if (this->numberOfWorkers == -1) {
        if (Utils::is_number(nWorkers)) {
            numberOfWorkers = atoi(nWorkers.c_str());
        }
    }

    if (this->numberOfWorkers == 0) {
        server_logger.error("Number of Workers is not specified");
        return;
    }

    if (numberOfWorkers > 0 && hostsList.size() > 0) {
        numberOfWorkersPerHost = numberOfWorkers / hostsList.size();
        hostListModeNWorkers = numberOfWorkers % hostsList.size();
    }

    std::string schedulerEnabled = Utils::getJasmineGraphProperty("org.jasminegraph.scheduler.enabled");

    if (schedulerEnabled == "true") {
        backupPerformanceDB();
        clearPerformanceDB();
    }

    this->sqlite->runUpdate("DELETE FROM worker");

    int workerIDCounter = 0;
    for (it = hostsList.begin(); it < hostsList.end(); it++) {
        string sqlStatement =
            "INSERT INTO worker (idworker,host_idhost,name,ip,user,is_public,server_port,server_data_port) VALUES ";
        string valuesString;
        std::string hostName = *it;
        string user = "";
        string ip = hostName;
        if (hostName.find('@') != std::string::npos) {
            vector<string> splitted = Utils::split(hostName, '@');
            ip = splitted[1];
            user = splitted[0];
        }
        int portCount = 0;
        std::string hostID = Utils::getHostID(ip, this->sqlite);
        std::vector<int> portVector = workerPortsMap[hostName];
        std::vector<int> dataPortVector = workerDataPortsMap[hostName];

        while (portCount < numberOfWorkersPerHost) {
            portVector.push_back(workerPort);
            dataPortVector.push_back(workerDataPort);
            hostWorkerList.push_back({*it, workerPort, workerDataPort});
            // FIXME: When there are more than 1 worker in the same host, one workers ports will replace the entries of
            // other workers port entries in hostPortMap
            hostPortMap[*it] = make_pair(workerPort, workerDataPort);
            portCount++;
            // ToDO: Here for the moment we use host name as the IP address as the third parameter.
            // ToDO: We also keep user as empty string
            string is_public = "false";
            valuesString += "(" + std::to_string(workerIDCounter) + ", " + hostID + ", \"" + hostName + "\", \"" + ip +
                            "\",\"" + user + "\", '" + is_public + "',\"" + std::to_string(workerPort) + "\", \"" +
                            std::to_string(workerDataPort) + "\"),";
            workerPort = workerPort + 2;
            workerDataPort = workerDataPort + 2;
            workerIDCounter++;
        }

        if (hostListModeNWorkers > 0) {
            portVector.push_back(workerPort);
            dataPortVector.push_back(workerDataPort);
            hostWorkerList.push_back({*it, workerPort, workerDataPort});
            hostPortMap[*it] = make_pair(workerPort, workerDataPort);
            hostListModeNWorkers--;
            string is_public = "false";
            valuesString += "(" + std::to_string(workerIDCounter) + ", " + hostID + ", \"" + hostName + "\", \"" + ip +
                            "\",\"" + user + "\", '" + is_public + "',\"" + std::to_string(workerPort) + "\", \"" +
                            std::to_string(workerDataPort) + "\"),";
            workerPort = workerPort + 2;
            workerDataPort = workerDataPort + 2;
            workerIDCounter++;
        }

        valuesString = valuesString.substr(0, valuesString.length() - 1);
        sqlStatement = sqlStatement + valuesString;
        this->sqlite->runInsert(sqlStatement);

        workerPortsMap[hostName] = portVector;
        workerDataPortsMap[hostName] = dataPortVector;
    }

    Utils::assignPartitionsToWorkers(numberOfWorkers, this->sqlite);

    int hostListSize = hostsList.size();
    std::vector<std::string>::iterator hostListIterator;
    hostListIterator = hostsList.begin();

    std::thread *myThreads = new std::thread[hostListSize];
    int count = 0;
    server_logger.info("Starting threads for workers");
    for (hostListIterator = hostsList.begin(); hostListIterator < hostsList.end(); hostListIterator++) {
        std::string host = *hostListIterator;
        addHostsToMetaDB(host, workerPortsMap[host], workerDataPortsMap[host]);
        addInstanceDetailsToPerformanceDB(host, workerPortsMap[host], "false");
        myThreads[count] = std::thread(startRemoteWorkers, workerPortsMap[host], workerDataPortsMap[host], host,
                                       masterHost, enableNmon);
        count++;
    }

    for (int threadCount = 0; threadCount < hostListSize; threadCount++) {
        myThreads[threadCount].join();
    }
    delete[] myThreads;
}

static int getPortByHost(const std::string &host) {
    if (jasminegraph_profile == PROFILE_K8S) {
        return Conts::JASMINEGRAPH_INSTANCE_PORT;
    }
    return hostPortMap[host].first;
}

static int getDataPortByHost(const std::string &host) {
    if (jasminegraph_profile == PROFILE_K8S) {
        return Conts::JASMINEGRAPH_INSTANCE_DATA_PORT;
    }
    return hostPortMap[host].second;
}

static size_t getWorkerCount() {
    if (jasminegraph_profile == PROFILE_K8S) {
        return K8sWorkerController::workerList.size();
    }
    return hostWorkerList.size();
}

void JasmineGraphServer::waitForAcknowledgement(int numberOfWorkers) {
    auto begin = chrono::high_resolution_clock::now();
    int timeDifference = 0;
    while (timeDifference < Conts::JASMINEGRAPH_WORKER_ACKNOWLEDGEMENT_TIMEOUT) {
        sleep(2);  // Sleep for two seconds
        std::string selectQuery = "select idworker from worker where status='started'";
        std::vector<vector<pair<string, string>>> output = this->sqlite->runSelect(selectQuery);
        int startedWorkers = output.size();
        if (numberOfWorkers == startedWorkers) {
            break;
        }
        auto end = chrono::high_resolution_clock::now();
        auto dur = end - begin;
        auto msDuration = std::chrono::duration_cast<std::chrono::milliseconds>(dur).count();
        timeDifference = msDuration;
    }
}

void JasmineGraphServer::startRemoteWorkers(std::vector<int> workerPortsVector, std::vector<int> workerDataPortsVector,
                                            string host, string masterHost, string enableNmon) {
    std::string executableFile;
    std::string workerPath = Utils::getJasmineGraphProperty("org.jasminegraph.worker.path");
    std::string artifactPath = Utils::getJasmineGraphProperty("org.jasminegraph.artifact.path");
    std::string instanceDataFolder = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    std::string aggregateDataFolder =
        Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.aggregatefolder");
    std::string nmonFileLocation = Utils::getJasmineGraphProperty("org.jasminegraph.server.nmon.file.location");
    std::string federatedLearningLocation = Utils::getJasmineGraphProperty("org.jasminegraph.fl.location");

    std::string instanceFolder = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance");
    std::string instanceFolderLocal = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.local");

    std::string jasmineGraphExecutableName = Conts::JASMINEGRAPH_EXECUTABLE;
    server_logger.info("###MASTER#### Starting remote workers");
    if (hasEnding(workerPath, "/")) {
        executableFile = workerPath + jasmineGraphExecutableName;
    } else {
        executableFile = workerPath + "/" + jasmineGraphExecutableName;
    }
    std::string serverStartScript;
    char buffer[128];
    std::string result = "";

    if (artifactPath.empty() || artifactPath.find_first_not_of(' ') == artifactPath.npos) {
        artifactPath = Utils::getJasmineGraphHome();
    }

    if (jasminegraph_profile == PROFILE_NATIVE) {
        copyArtifactsToWorkers(workerPath, artifactPath, host);
        for (int i = 0; i < workerPortsVector.size(); i++) {
            if (host.find("localhost") != std::string::npos) {
                serverStartScript = executableFile + " native 2 " + host + " " + masterHost + " " +
                                    std::to_string(workerPortsVector.at(i)) + " " +
                                    std::to_string(workerDataPortsVector.at(i)) + " " + enableNmon;
            } else {
                serverStartScript = "ssh " + host + " " + executableFile + " native 2 " + host + " " + masterHost +
                                    " " + std::to_string(workerPortsVector.at(i)) + " " +
                                    std::to_string(workerDataPortsVector.at(i)) + " " + enableNmon;
            }
            const char *commandStr = serverStartScript.c_str();
            pid_t child = fork();
            if (child == 0) {
                execl("/bin/sh", "sh", "-c", commandStr, nullptr);
                _exit(1);
            }
        }
    } else if (jasminegraph_profile == PROFILE_DOCKER) {
        char *env_testing = getenv("TESTING");
        bool is_testing = (env_testing != nullptr && strcasecmp(env_testing, "true") == 0);
        for (int i = 0; i < workerPortsVector.size(); i++) {
            std::string worker_logdir = "/tmp/jasminegraph/worker_" + to_string(i);
            if (access(worker_logdir.c_str(), F_OK) != 0) {
                if (mkdir(worker_logdir.c_str(), 0777)) {
                    server_logger.error("Couldn't create worker log dir: " + worker_logdir);
                }
            } else {
                chmod(worker_logdir.c_str(), 0777);
            }
            if (masterHost == host || host == "localhost") {
                if (is_testing) {
                    serverStartScript = "docker run -p " + std::to_string(workerPortsVector.at(i)) + ":" +
                                        std::to_string(workerPortsVector.at(i)) + " -p " +
                                        std::to_string(workerDataPortsVector.at(i)) + ":" +
                                        std::to_string(workerDataPortsVector.at(i)) + " -v " + worker_logdir +
                                        ":/tmp/jasminegraph" + " -e WORKER_ID=" + to_string(i) +
                                        " jasminegraph:test --MODE 2 --HOST_NAME " + host + " --MASTERIP " +
                                        masterHost + " --SERVER_PORT " + std::to_string(workerPortsVector.at(i)) +
                                        " --SERVER_DATA_PORT " + std::to_string(workerDataPortsVector.at(i)) +
                                        " --ENABLE_NMON " + enableNmon + " >" + worker_logdir + "/worker.log 2>&1";
                } else {
                    serverStartScript =
                        "docker run -v " + instanceDataFolder + ":" + instanceDataFolder + " -v " +
                        aggregateDataFolder + ":" + aggregateDataFolder + " -v " + nmonFileLocation + ":" +
                        nmonFileLocation + " -v " + instanceDataFolder + "/" + to_string(i) + "/logs" + ":" +
                        "/var/tmp/jasminegraph/logs" + " -p " + std::to_string(workerPortsVector.at(i)) + ":" +
                        std::to_string(workerPortsVector.at(i)) + " -p " + std::to_string(workerDataPortsVector.at(i)) +
                        ":" + std::to_string(workerDataPortsVector.at(i)) + " -e WORKER_ID=" + to_string(i) +
                        " jasminegraph1:latest --MODE 2 --HOST_NAME " + host + " --MASTERIP " + masterHost +
                        " --SERVER_PORT " + std::to_string(workerPortsVector.at(i)) + " --SERVER_DATA_PORT " +
                        std::to_string(workerDataPortsVector.at(i)) + " --ENABLE_NMON " + enableNmon;
                }
            } else {
                if (is_testing) {
                    serverStartScript =
                        "docker -H ssh://" + host + " run -p " + std::to_string(workerPortsVector.at(i)) + ":" +
                        std::to_string(workerPortsVector.at(i)) + " -p " + std::to_string(workerDataPortsVector.at(i)) +
                        ":" + std::to_string(workerDataPortsVector.at(i)) + " -e WORKER_ID=" + to_string(i) +
                        " jasminegraph1:test --MODE 2 --HOST_NAME " + host + " --MASTERIP " + masterHost +
                        " --SERVER_PORT " + std::to_string(workerPortsVector.at(i)) + " --SERVER_DATA_PORT " +
                        std::to_string(workerDataPortsVector.at(i)) + " --ENABLE_NMON " + enableNmon + " >" +
                        worker_logdir + "/worker.log 2>&1";
                } else {
                    serverStartScript =
                        "docker -H ssh://" + host + " run -v " + instanceDataFolder + ":" + instanceDataFolder +
                        " -v " + aggregateDataFolder + ":" + aggregateDataFolder + " -v " + nmonFileLocation + ":" +
                        nmonFileLocation + " -v " + instanceDataFolder + "/" + to_string(i) + "/logs" + ":" +
                        "/var/tmp/jasminegraph/logs" + " -p " + std::to_string(workerPortsVector.at(i)) + ":" +
                        std::to_string(workerPortsVector.at(i)) + " -p " + std::to_string(workerDataPortsVector.at(i)) +
                        ":" + std::to_string(workerDataPortsVector.at(i)) + " -e WORKER_ID=" + to_string(i) +
                        " jasminegraph1:latest --MODE 2 --HOST_NAME " + host + " --MASTERIP " + masterHost +
                        " --SERVER_PORT " + std::to_string(workerPortsVector.at(i)) + " --SERVER_DATA_PORT " +
                        std::to_string(workerDataPortsVector.at(i)) + " --ENABLE_NMON " + enableNmon;
                }
            }
            const char *serverStartCmd = serverStartScript.c_str();
            pid_t child = fork();
            if (child == 0) {
                execl("/bin/sh", "sh", "-c", serverStartCmd, nullptr);
                _exit(1);
            }
        }
    }
}

bool JasmineGraphServer::spawnNewWorker(string host, string port, string dataPort, string masterHost,
                                        string enableNmon) {
    auto *refToSqlite = new SQLiteDBInterface();
    refToSqlite->init();
    string selectHostSQL = "SELECT idhost from host where name='" + host + "'";
    string selectWorkerSQL =
        "SELECT idworker from worker where server_port = '" + port + "' or server_data_port = '" + dataPort + "'";
    std::vector<vector<pair<string, string>>> checkWorkerOutput = refToSqlite->runSelect(selectWorkerSQL);

    if (checkWorkerOutput.size() > 0) {
        return false;
    }

    std::vector<vector<pair<string, string>>> selectHostOutput = refToSqlite->runSelect(selectHostSQL);
    string idHost = "";

    if (selectHostOutput.size() > 0) {
        idHost = selectHostOutput[0][0].second;
    } else {
        string maxHostIDSQL = "select max(idhost) from host";
        std::vector<vector<pair<string, string>>> selectMaxHostOutput = refToSqlite->runSelect(maxHostIDSQL);
        idHost = selectMaxHostOutput[0][0].second;

        int hostId = atoi(idHost.c_str());
        hostId++;
        std::string hostInsertString = "INSERT INTO host (idhost,name,ip,is_public) VALUES ('" +
                                       std::to_string(hostId) + "','" + host + "','" + host + "','false')";

        refToSqlite->runInsert(hostInsertString);

        idHost = to_string(hostId);
    }

    string maxWorkerIDSQL = "select max(idworker) from worker";
    std::vector<vector<pair<string, string>>> selectMaxWorkerIdOutput = refToSqlite->runSelect(maxWorkerIDSQL);
    string maxIdWorker = selectMaxWorkerIdOutput[0][0].second;
    int maxWorkerId = atoi(maxIdWorker.c_str());
    maxWorkerId++;
    string workerInsertSqlStatement =
        "INSERT INTO worker (idworker,host_idhost,name,ip,user,is_public,server_port,server_data_port) VALUES ('" +
        to_string(maxWorkerId) + "','" + idHost + "','" + host + "','" + host + "','','false','" + port + "','" +
        dataPort + "')";

    refToSqlite->runInsert(workerInsertSqlStatement);
    refToSqlite->finalize();
    delete refToSqlite;

    std::vector<int> workerPortsVector;
    std::vector<int> workerDataPortsVector;

    workerPortsVector.push_back(atoi(port.c_str()));
    workerDataPortsVector.push_back(atoi(dataPort.c_str()));

    JasmineGraphServer::startRemoteWorkers(workerPortsVector, workerDataPortsVector, host, masterHost, enableNmon);
    return true;
}

void JasmineGraphServer::initiateAggregateMap() {
    for (int i = 1; i <= numberOfWorkers; i++) {
        aggregateWeightMap[i] = 0;
    }
}

void JasmineGraphServer::resolveOperationalGraphs() {
    string sqlStatement =
        "SELECT partition_graph_idgraph,partition_idpartition,worker_idworker FROM worker_has_partition ORDER BY "
        "worker_idworker";
    std::vector<vector<pair<string, string>>> output = this->sqlite->runSelect(sqlStatement);
    std::map<int, vector<string>> partitionMap;

    for (auto i = output.begin(); i != output.end(); ++i) {
        int workerID = -1;
        string graphID;
        string partitionID;
        auto j = i->begin();
        graphID = j->second;
        ++j;
        partitionID = j->second;
        ++j;
        workerID = std::stoi(j->second);
        std::vector<string> &partitionList = partitionMap[workerID];
        partitionList.push_back(graphID + "_" + partitionID);
    }

    int RECORD_AGGREGATION_FREQUENCY = 5;
    int counter = 0;
    std::stringstream ss;
    std::map<int, vector<string>> partitionAggregatedMap;
    for (map<int, vector<string>>::iterator it = partitionMap.begin(); it != partitionMap.end(); ++it) {
        int len = (it->second).size();
        int workerID = it->first;

        for (std::vector<string>::iterator x = (it->second).begin(); x != (it->second).end(); ++x) {
            if (counter >= RECORD_AGGREGATION_FREQUENCY) {
                std::vector<string> &partitionList = partitionAggregatedMap[workerID];
                string data = ss.str();
                std::stringstream().swap(ss);
                counter = 0;
                data = data.substr(0, data.find_last_of(","));
                partitionList.push_back(data);
            }
            ss << x->c_str() << ",";
            counter++;
        }

        std::vector<string> &partitionList = partitionAggregatedMap[workerID];
        string data = ss.str();
        std::stringstream().swap(ss);
        counter = 0;
        data = data.substr(0, data.find_last_of(","));
        partitionList.push_back(data);
    }

    sqlStatement = "SELECT idworker,ip,server_port FROM worker";
    output = this->sqlite->runSelect(sqlStatement);

    std::set<int> graphIDsFromWorkersSet;
    for (std::vector<vector<pair<string, string>>>::iterator i = output.begin(); i != output.end(); ++i) {
        int workerID = -1;
        string host;
        int workerPort = -1;
        string partitionID;
        std::vector<pair<string, string>>::iterator j = i->begin();
        workerID = std::stoi(j->second);
        ++j;
        host = j->second;
        ++j;
        workerPort = std::stoi(j->second);

        int sockfd;
        char data[FED_DATA_LENGTH + 1];
        bool loop = false;
        socklen_t len;
        struct sockaddr_in serv_addr;
        struct hostent *server;

        sockfd = socket(AF_INET, SOCK_STREAM, 0);

        if (sockfd < 0) {
            server_logger.error("Cannot create socket");
            continue;
        }

        if (host.find('@') != std::string::npos) {
            host = Utils::split(host, '@')[1];
        }

        server = gethostbyname(host.c_str());
        if (server == NULL) {
            server_logger.error("ERROR, no host named " + host);
            continue;
        }

        bzero((char *)&serv_addr, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
        serv_addr.sin_port = htons(workerPort);
        if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
            continue;
        }

        if (!Utils::performHandshake(sockfd, data, FED_DATA_LENGTH, this->masterHost)) {
            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
            close(sockfd);
            continue;
        }

        if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH,
                                       JasmineGraphInstanceProtocol::INITIATE_FRAGMENT_RESOLUTION,
                                       JasmineGraphInstanceProtocol::OK)) {
            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
            close(sockfd);
            continue;
        }

        std::vector<string> partitionList = partitionAggregatedMap[workerID];

        bool success = true;
        string response;
        for (std::vector<string>::iterator x = partitionList.begin(); x != partitionList.end(); ++x) {
            string partitionsList = x->c_str();
            if (!Utils::send_str_wrapper(sockfd, partitionsList)) {
                success = false;
                break;
            }
            server_logger.info("Sent: " + partitionsList);

            response = Utils::read_str_trim_wrapper(sockfd, data, INSTANCE_DATA_LENGTH);
            if (response.compare(JasmineGraphInstanceProtocol::FRAGMENT_RESOLUTION_CHK) != 0) {
                server_logger.error("Error in fragment resolution process. Received: " + response);
                success = false;
                break;
            }
        }
        if (!success) {
            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
            close(sockfd);
            continue;
        }
        if (!Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::FRAGMENT_RESOLUTION_DONE)) {
            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
            close(sockfd);
            continue;
        }
        server_logger.info("Sent: " + JasmineGraphInstanceProtocol::FRAGMENT_RESOLUTION_DONE);

        response = Utils::read_str_trim_wrapper(sockfd, data, INSTANCE_DATA_LENGTH);
        if (response.compare("") != 0) {
            std::vector<string> listOfOperationalItems = Utils::split(response, ',');
            for (std::vector<string>::iterator it = listOfOperationalItems.begin(); it != listOfOperationalItems.end();
                 it++) {
                graphIDsFromWorkersSet.insert(atoi(it->c_str()));
            }
        }
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
    }

    sqlStatement = "SELECT idgraph FROM graph";
    std::vector<vector<pair<string, string>>> output2 = this->sqlite->runSelect(sqlStatement);
    std::set<int> graphIDsFromMetDBSet;
    for (auto i = output2.begin(); i != output2.end(); ++i) {
        auto j = i->begin();
        graphIDsFromMetDBSet.insert(atoi(j->second.c_str()));
    }

    for (std::set<int>::iterator itr = graphIDsFromWorkersSet.begin(); itr != graphIDsFromWorkersSet.end(); itr++) {
        if (graphIDsFromMetDBSet.find(*itr) == graphIDsFromMetDBSet.end()) {
            deleteNonOperationalGraphFragment(*itr);
        }
    }
}

/** Method used in master node to commence deletion of a graph fragment
 *
 * @param graphID ID of graph fragments to be deleted
 */
void JasmineGraphServer::deleteNonOperationalGraphFragment(int graphID) {
    server_logger.info("Deleting non-operational fragment " + to_string(graphID));
    int count = 0;
    // Define threads for each host
    std::thread *deleteThreads = new std::thread[getWorkerCount()];
    std::vector<JasmineGraphServer::worker> *workerListAll;
    if (jasminegraph_profile == PROFILE_K8S) {
        workerListAll = &(K8sWorkerController::workerList);
    } else {
        workerListAll = &hostWorkerList;
    }
    // Iterate through all hosts
    for (auto it = (*workerListAll).begin(); it != (*workerListAll).end(); it++) {
        // Fetch hostname and port
        const string &hostname = it->hostname;
        int port = it->port;
        // Initialize threads for host
        // Each thread runs the service to remove the given graph ID fragments in their datafolders
        deleteThreads[count++] =
            std::thread(removeFragmentThroughService, hostname, port, to_string(graphID), this->masterHost);
    }
    sleep(1);

    for (int threadCount = 0; threadCount < count; threadCount++) {
        if (deleteThreads[threadCount].joinable()) {
            deleteThreads[threadCount].join();
        }
    }
    server_logger.info("Deleted graph fragments of graph ID " + to_string(graphID));
}

void JasmineGraphServer::shutdown_workers() {
    server_logger.info("Shutting down workers");
    auto *server = JasmineGraphServer::getInstance();

    if (jasminegraph_profile == PROFILE_K8S) {
        K8sWorkerController::getInstance()->setNumberOfWorkers(0);
        return;
    }
    for (auto listIterator = hostWorkerList.begin(); listIterator < hostWorkerList.end(); listIterator++) {
        worker worker = *listIterator;
        server_logger.info("Host:" + worker.hostname + " Port:" + to_string(worker.port) +
                           " DPort:" + to_string(worker.dataPort));

        std::string host = worker.hostname;

        if (worker.hostname.find('@') != std::string::npos) {
            host = Utils::split(host, '@')[1];
        }
        shutdown_worker(host, worker.port);
    }
}

int JasmineGraphServer::shutdown_worker(std::string workerIP, int port) {
    int sockfd;
    char data[FED_DATA_LENGTH + 1];
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        server_logger.error("Cannot create socket");
        return -1;
    }

    server = gethostbyname(workerIP.c_str());
    if (server == NULL) {
        server_logger.error("ERROR, no host named " + workerIP);
        return -1;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) != 0) {
        return -1;
    }

    if (!Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::SHUTDOWN)) {
        return -1;
    }
    server_logger.info("Sent: " + JasmineGraphInstanceProtocol::SHUTDOWN);

    string response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
    server_logger.info("Response: " + response);
    close(sockfd);
    return 0;
}

static map<string, float> scaleK8s(size_t npart) {
    std::vector<JasmineGraphServer::worker> &workerList = K8sWorkerController::workerList;
    const map<string, string> &cpu_map = Utils::getMetricMap("cpu_usage");
    // Convert strings to float
    map<string, float> cpu_loads;
    for (auto it = cpu_map.begin(); it != cpu_map.end(); it++) {
        cpu_loads[it->first] = (float)atof(it->second.c_str());
    }

    for (auto it = workerList.begin(); it != workerList.end(); it++) {
        auto &worker = *it;
        // 0.8 depends on num cpu cores and other factors
        if (npart > 0 && cpu_loads[worker.hostname + ":" + to_string(worker.port)] < 0.8) npart--;
    }
    if (npart <= 0) return cpu_loads;
    K8sWorkerController *controller = K8sWorkerController::getInstance();
    controller->scaleUp((int)npart);
    server_logger.info("Scalled up with " + to_string(npart) + " new workers");
    for (auto it = K8sWorkerController::workerList.begin(); it != K8sWorkerController::workerList.end(); it++) {
        auto &worker = *it;
        string workerHostPort = worker.hostname + ":" + to_string(worker.port);
        if (cpu_loads.find(workerHostPort) == cpu_loads.end()) {
            cpu_loads[workerHostPort] = 0.;
        }
    }
    return cpu_loads;
}

static std::vector<JasmineGraphServer::worker> getWorkers(size_t npart) {
    // TODO: get the workers with lowest load from workerList
    std::vector<JasmineGraphServer::worker> *workerListAll;
    map<string, float> cpu_loads;
    if (jasminegraph_profile == PROFILE_K8S) {
        std::unique_ptr<K8sInterface> k8sInterface(new K8sInterface());
        if (k8sInterface->getJasmineGraphConfig("scale_on_adgr") != "true") {
            return K8sWorkerController::workerList;
        }
        workerListAll = &(K8sWorkerController::workerList);
        cpu_loads = scaleK8s(npart);
    } else {
        workerListAll = &hostWorkerList;
        for (auto it = hostWorkerList.begin(); it != hostWorkerList.end(); it++) {
            auto &worker = *it;
            string workerHostPort = worker.hostname + ":" + to_string(worker.port);
            cpu_loads[workerHostPort] = 0.;
        }
    }
    size_t len = workerListAll->size();
    std::vector<JasmineGraphServer::worker> workerList;
    for (int i = 0; i < npart; i++) {
        JasmineGraphServer::worker worker_min;
        float cpu_min = 100.;
        for (auto it = (*workerListAll).begin(); it != (*workerListAll).end(); it++) {
            auto &worker = *it;
            string workerHostPort = worker.hostname + ":" + to_string(worker.port);
            float cpu = cpu_loads[workerHostPort];
            if (cpu < cpu_min) {
                worker_min = worker;
                cpu_min = cpu;
            }
        }
        string workerHostPort = worker_min.hostname + ":" + to_string(worker_min.port);
        cpu_loads[workerHostPort] += 0.25;  // 0.25 = 1/nproc
        workerList.push_back(worker_min);
    }
    return workerList;
}

void JasmineGraphServer::uploadGraphLocally(int graphID, const string graphType,
                                            vector<std::map<int, string>> fullFileList, std::string masterIP) {
    server_logger.info("Uploading the graph locally..");
    std::map<int, string> partitionFileMap = fullFileList[0];
    std::map<int, string> centralStoreFileMap = fullFileList[1];
    std::map<int, string> centralStoreDuplFileMap = fullFileList[2];
    std::map<int, string> compositeCentralStoreFileMap = fullFileList[5];
    std::map<int, string> attributeFileMap;
    std::map<int, string> centralStoreAttributeFileMap;
    if (masterHost.empty()) {
        masterHost = Utils::getJasmineGraphProperty("org.jasminegraph.server.host");
    }
    int total_threads = partitionFileMap.size() + centralStoreFileMap.size() + centralStoreDuplFileMap.size() +
                        compositeCentralStoreFileMap.size();
    if (graphType == Conts::GRAPH_WITH_ATTRIBUTES) {
        attributeFileMap = fullFileList[3];
        total_threads += attributeFileMap.size();
        centralStoreAttributeFileMap = fullFileList[4];
        total_threads += centralStoreAttributeFileMap.size();
    }
    int count = 0;
    int file_count = 0;
    std::thread *workerThreads = new std::thread[total_threads];
    while (count < total_threads) {
        const auto &workerList = getWorkers(partitionFileMap.size());
        while (true) {
            if (count >= total_threads) {
                break;
            }
            worker worker = workerList[graphUploadWorkerTracker];
            std::string partitionFileName = partitionFileMap[file_count];
            workerThreads[count++] = std::thread(batchUploadFile, worker.hostname, worker.port, worker.dataPort,
                                                 graphID, partitionFileName, masterHost);
            copyCentralStoreToAggregateLocation(centralStoreFileMap[file_count]);
            workerThreads[count++] = std::thread(batchUploadCentralStore, worker.hostname, worker.port, worker.dataPort,
                                                 graphID, centralStoreFileMap[file_count], masterHost);

            if (compositeCentralStoreFileMap.find(file_count) != compositeCentralStoreFileMap.end()) {
                copyCentralStoreToAggregateLocation(compositeCentralStoreFileMap[file_count]);
                workerThreads[count++] =
                    std::thread(batchUploadCompositeCentralstoreFile, worker.hostname, worker.port, worker.dataPort,
                                graphID, compositeCentralStoreFileMap[file_count], masterHost);
            }

            workerThreads[count++] = std::thread(batchUploadCentralStore, worker.hostname, worker.port, worker.dataPort,
                                                 graphID, centralStoreDuplFileMap[file_count], masterHost);
            if (graphType == Conts::GRAPH_WITH_ATTRIBUTES) {
                workerThreads[count++] =
                    std::thread(batchUploadAttributeFile, worker.hostname, worker.port, worker.dataPort, graphID,
                                attributeFileMap[file_count], masterHost);
                workerThreads[count++] =
                    std::thread(batchUploadCentralAttributeFile, worker.hostname, worker.port, worker.dataPort, graphID,
                                centralStoreAttributeFileMap[file_count], masterHost);
            }
            assignPartitionToWorker(partitionFileName, graphID, worker.hostname, worker.port);
            file_count++;

            graphUploadWorkerTracker++;
            graphUploadWorkerTracker %= workerList.size();
        }
    }

    server_logger.info("Total number of threads to join : " + to_string(count));
    for (int threadCount = 0; threadCount < count; threadCount++) {
        workerThreads[threadCount].join();
        server_logger.info("Thread " + to_string(threadCount) + " joined");
    }

    std::time_t time = chrono::system_clock::to_time_t(chrono::system_clock::now());
    string uploadEndTime = ctime(&time);

    // The following function updates the 'worker_has_partition' table and 'graph' table only
    updateMetaDB(graphID, uploadEndTime);
    server_logger.info("Upload Graph Locally done");
    delete[] workerThreads;
}

static void assignPartitionToWorker(std::string fileName, int graphId, std::string workerHost, int workerPort) {
    auto *refToSqlite = new SQLiteDBInterface();
    refToSqlite->init();
    size_t lastindex = fileName.find_last_of(".");
    string rawname = fileName.substr(0, lastindex);
    string partitionID = rawname.substr(rawname.find_last_of("_") + 1);

    if (workerHost.find('@') != std::string::npos) {
        workerHost = Utils::split(workerHost, '@')[1];
    }

    std::string workerSearchQuery = "SELECT idworker FROM worker WHERE ip='" + workerHost + "' AND server_port='" +
                                    std::to_string(workerPort) + "'";

    std::vector<vector<pair<string, string>>> results = refToSqlite->runSelect(workerSearchQuery);

    std::string workerID = results[0][0].second;

    std::string partitionToWorkerQuery =
        "INSERT INTO worker_has_partition (partition_idpartition, partition_graph_idgraph, worker_idworker) VALUES "
        "('" +
        partitionID + "','" + std::to_string(graphId) + "','" + workerID + "')";

    refToSqlite->runInsert(partitionToWorkerQuery);
    refToSqlite->finalize();
    delete refToSqlite;
}

static bool batchUploadFile(std::string host, int port, int dataPort, int graphID, std::string filePath,
                            std::string masterIP) {
    return Utils::uploadFileToWorker(host, port, dataPort, graphID, filePath, masterIP,
                                     JasmineGraphInstanceProtocol::BATCH_UPLOAD);
}

static bool batchUploadCentralStore(std::string host, int port, int dataPort, int graphID, std::string filePath,
                                    std::string masterIP) {
    return Utils::uploadFileToWorker(host, port, dataPort, graphID, filePath, masterIP,
                                     JasmineGraphInstanceProtocol::BATCH_UPLOAD_CENTRAL);
}

void JasmineGraphServer::copyCentralStoreToAggregateLocation(std::string filePath) {
    std::string result = "SUCCESS";
    std::string aggregatorDirPath = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.aggregatefolder");

    if (access(aggregatorDirPath.c_str(), F_OK)) {
        Utils::createDirectory(aggregatorDirPath);
    }

    std::string copyCommand = "cp " + filePath + " " + aggregatorDirPath;
    if (system(copyCommand.c_str())) {
        server_logger.error("Copying " + filePath + " into " + aggregatorDirPath + " failed");
    }
}

static bool batchUploadAttributeFile(std::string host, int port, int dataPort, int graphID, std::string filePath,
                                     std::string masterIP) {
    return Utils::uploadFileToWorker(host, port, dataPort, graphID, filePath, masterIP,
                                     JasmineGraphInstanceProtocol::UPLOAD_RDF_ATTRIBUTES);
}

static bool batchUploadCentralAttributeFile(std::string host, int port, int dataPort, int graphID, std::string filePath,
                                            std::string masterIP) {
    return Utils::uploadFileToWorker(host, port, dataPort, graphID, filePath, masterIP,
                                     JasmineGraphInstanceProtocol::UPLOAD_RDF_ATTRIBUTES_CENTRAL);
}

static bool batchUploadCompositeCentralstoreFile(std::string host, int port, int dataPort, int graphID,
                                                 std::string filePath, std::string masterIP) {
    return Utils::uploadFileToWorker(host, port, dataPort, graphID, filePath, masterIP,
                                     JasmineGraphInstanceProtocol::BATCH_UPLOAD_COMPOSITE_CENTRAL);
}

static void copyArtifactsToWorkers(const std::string &workerPath, const std::string &artifactLocation,
                                   const std::string &remoteWorker) {
    // Not used in docker or k8s modes
    if (artifactLocation.empty() || artifactLocation.find_first_not_of(' ') == artifactLocation.npos) {
        server_logger.error("Received `" + artifactLocation + "` for `artifactLocation` value!");
        return;
    }

    std::string pathCheckCommand = "test -e " + workerPath;
    if (remoteWorker.find("localhost") == std::string::npos) {
        pathCheckCommand = "ssh " + remoteWorker + " " + pathCheckCommand;
    }

    if (system(pathCheckCommand.c_str())) {
        deleteWorkerPath(remoteWorker, workerPath);
    }
    // Creating the log file path will automatically create its parent worker path
    createLogFilePath(remoteWorker, workerPath);

    // Copy artifacts
    const int ARTIFACTS_COUNT = 4;
    std::string artifactsArray[ARTIFACTS_COUNT] = {"JasmineGraph", "run.sh", "conf"};

    for (int i = 0; i < ARTIFACTS_COUNT; i++) {
        std::string artifactCopyCommand;
        if (remoteWorker.find("localhost") != std::string::npos) {
            artifactCopyCommand = "cp -r " + artifactLocation + "/" + artifactsArray[i] + " " + workerPath;
        } else {
            artifactCopyCommand =
                "scp -r " + artifactLocation + "/" + artifactsArray[i] + " " + remoteWorker + ":" + workerPath;
        }

        if (system(artifactCopyCommand.c_str())) {
            server_logger.error("Error executing command: " + artifactCopyCommand);
        }
    }
}

static void deleteWorkerPath(const std::string &workerHost, const std::string &workerPath) {
    // Not used in docker or k8s modes
    std::string pathDeletionCommand = "rm -rf " + workerPath;
    if (workerHost.find("localhost") == std::string::npos) {
        pathDeletionCommand = "ssh " + workerHost + " " + pathDeletionCommand;
    }

    if (system(pathDeletionCommand.c_str())) {
        server_logger.error("Error executing command: " + pathDeletionCommand);
    }
}

static void createLogFilePath(const std::string &workerHost, const std::string &workerPath) {
    // Not used in docker or k8s modes
    std::string pathCreationCommand = "mkdir -p " + workerPath + "/logs";
    if (workerHost.find("localhost") == std::string::npos) {
        pathCreationCommand = "ssh " + workerHost + " " + pathCreationCommand;
    }

    if (system(pathCreationCommand.c_str())) {
        server_logger.error("Error executing command: " + pathCreationCommand);
    }
}

void JasmineGraphServer::addHostsToMetaDB(std::string host, std::vector<int> portVector,
                                          std::vector<int> dataPortVector) {
    string name = host;
    string sqlStatement = "";
    string ip_address = "";
    string user = "";
    if (host.find('@') != std::string::npos) {
        vector<string> splitted = Utils::split(host, '@');
        ip_address = splitted[1];
        user = splitted[0];
    } else {
        ip_address = host;
    }

    for (int i = 0; i < portVector.size(); i++) {
        int workerPort = portVector.at(i);
        int workerDataPort = dataPortVector.at(i);

        if (!Utils::hostExists(name, ip_address, std::to_string(workerPort), this->sqlite)) {
            string hostID = Utils::getHostID(name, this->sqlite);
            sqlStatement =
                ("INSERT INTO worker (host_idhost,name,ip,user,is_public,server_port,server_data_port) VALUES (\"" +
                 hostID + "\", \"" + name + "\", \"" + ip_address + "\",\"" + user + "\", \"\",\"" +
                 std::to_string(workerPort) + "\", \"" + std::to_string(workerDataPort) + "\")");
            this->sqlite->runInsert(sqlStatement);
        }
    }
}

static void updateMetaDB(int graphID, string uploadEndTime) {
    std::unique_ptr<SQLiteDBInterface> sqliteDBInterface(new SQLiteDBInterface());
    sqliteDBInterface->init();
    string sqlStatement = "UPDATE graph SET upload_end_time = '" + uploadEndTime +
                          "' ,graph_status_idgraph_status = '" + to_string(Conts::GRAPH_STATUS::OPERATIONAL) +
                          "' WHERE idgraph = '" + to_string(graphID) + "'";
    sqliteDBInterface->runUpdate(sqlStatement);
}

void JasmineGraphServer::removeGraph(vector<pair<string, string>> hostHasPartition, string graphID,
                                     std::string masterIP) {
    server_logger.info("Deleting graph " + graphID);
    int count = 0;
    std::thread *deleteThreads = new std::thread[hostHasPartition.size()];
    for (std::vector<pair<string, string>>::iterator j = (hostHasPartition.begin()); j != hostHasPartition.end(); ++j) {
        deleteThreads[count] =
            std::thread(removePartitionThroughService, j->first, getPortByHost(j->first), graphID, j->second, masterIP);
        count++;
    }

    for (int threadCount = 0; threadCount < count; threadCount++) {
        deleteThreads[threadCount].join();
        server_logger.info("Thread " + to_string(threadCount) + " joined");
    }
    delete[] deleteThreads;
}

/** Used to delete graph fragments of a given graph ID for a particular host running at a particular port
 *
 *  @param host Hostname of worker
 *  @param port Port host is running on
 *  @param graphID ID of graph fragments to be deleted
 *  @param masterIP IP of master node
 */
static bool removeFragmentThroughService(string host, int port, string graphID, string masterIP) {
    server_logger.info("Host:" + host + " Port:" + to_string(port));
    int sockfd;
    char data[FED_DATA_LENGTH + 1];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        server_logger.error("Cannot create socket");
        return false;
    }

    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        server_logger.error("ERROR, no host named " + host);
        return false;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        return false;
    }

    if (!Utils::performHandshake(sockfd, data, FED_DATA_LENGTH, masterIP)) {
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return false;
    }

    if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH,
                                   JasmineGraphInstanceProtocol::DELETE_GRAPH_FRAGMENT,
                                   JasmineGraphInstanceProtocol::OK)) {
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return false;
    }

    if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH, graphID, JasmineGraphInstanceProtocol::OK)) {
        close(sockfd);
        return false;
    }
    Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
    close(sockfd);
    return true;
}

static bool removePartitionThroughService(string host, int port, string graphID, string partitionID, string masterIP) {
    server_logger.info("Host:" + host + " Port:" + to_string(port));
    int sockfd;
    char data[FED_DATA_LENGTH + 1];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        server_logger.error("Cannot create socket");
        return false;
    }

    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        server_logger.error("ERROR, no host named " + host);
        return false;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        return false;
    }

    if (!Utils::performHandshake(sockfd, data, FED_DATA_LENGTH, masterIP)) {
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return false;
    }

    if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH, JasmineGraphInstanceProtocol::DELETE_GRAPH,
                                   JasmineGraphInstanceProtocol::OK)) {
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return false;
    }

    if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH, graphID,
                                   JasmineGraphInstanceProtocol::SEND_PARTITION_ID)) {
        close(sockfd);
        return false;
    }

    if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH, partitionID, JasmineGraphInstanceProtocol::OK)) {
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return false;
    }

    Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
    close(sockfd);
    return true;
}

void JasmineGraphServer::updateOperationalGraphList() {
    string hosts = "";
    string graphIDs = "";
    std::vector<std::string> hostsList;

    if (jasminegraph_profile == PROFILE_NATIVE) {
        hostsList = Utils::getHostListFromProperties();
    } else if (jasminegraph_profile == PROFILE_DOCKER) {
        hostsList = getWorkerVector(workerHosts);
    } else {
        return;  // TODO(thevindu-w): implement.for k8s
    }
    for (auto it = hostsList.begin(); it < hostsList.end(); it++) {
        string host = *it;
        hosts += ("'" + host + "', ");
    }
    hosts = hosts.substr(0, hosts.size() - 2);
    string sqlStatement =
        "SELECT b.partition_graph_idgraph FROM worker_has_partition AS b "
        "JOIN worker WHERE worker.idworker = b.worker_idworker AND worker.name IN "
        "(" +
        hosts +
        ") GROUP BY b.partition_graph_idgraph HAVING COUNT(b.partition_idpartition)= "
        "(SELECT COUNT(a.idpartition) FROM partition AS a "
        "WHERE a.graph_idgraph = b.partition_graph_idgraph);";
    std::vector<vector<pair<string, string>>> v = this->sqlite->runSelect(sqlStatement);
    for (std::vector<vector<pair<string, string>>>::iterator i = v.begin(); i != v.end(); ++i) {
        for (std::vector<pair<string, string>>::iterator j = i->begin(); j != i->end(); ++j) {
            graphIDs += j->second + ", ";
        }
    }
    graphIDs = graphIDs.substr(0, graphIDs.size() - 2);
    string sqlStatement2 =
        "UPDATE graph SET graph_status_idgraph_status = ("
        "CASE WHEN idgraph IN (" +
        graphIDs + ") THEN '" + to_string(Conts::GRAPH_STATUS::OPERATIONAL) + "' ELSE '" +
        to_string(Conts::GRAPH_STATUS::NONOPERATIONAL) + "' END )";
    this->sqlite->runUpdate(sqlStatement2);
}

std::map<string, JasmineGraphServer::workerPartitions> JasmineGraphServer::getGraphPartitionedHosts(string graphID) {
    vector<pair<string, string>> hostHasPartition;
    auto *refToSqlite = new SQLiteDBInterface();
    refToSqlite->init();
    vector<vector<pair<string, string>>> hostPartitionResults = refToSqlite->runSelect(
        "SELECT name, partition_idpartition FROM worker_has_partition INNER JOIN worker ON "
        "worker_idworker = idworker WHERE partition_graph_idgraph = '" +
        graphID + "'");
    refToSqlite->finalize();
    delete refToSqlite;
    for (auto i = hostPartitionResults.begin(); i != hostPartitionResults.end(); ++i) {
        int count = 0;
        string hostname;
        string partitionID;
        for (std::vector<pair<string, string>>::iterator j = (i->begin()); j != i->end(); ++j) {
            if (count == 0) {
                hostname = j->second;
            } else {
                partitionID = j->second;
                hostHasPartition.push_back(pair<string, string>(hostname, partitionID));
            }
            count++;
        }
    }

    map<string, vector<string>> hostPartitions;
    for (auto j = hostHasPartition.begin(); j != hostHasPartition.end(); ++j) {
        string hostname = j->first;
        auto it = hostPartitions.find(hostname);
        if (it != hostPartitions.end()) {
            it->second.push_back(j->second);
        } else {
            vector<string> vec;
            vec.push_back(j->second);
            hostPartitions[hostname] = vec;
        }
    }

    map<string, JasmineGraphServer::workerPartitions> graphPartitionedHosts;
    for (map<string, vector<string>>::iterator it = hostPartitions.begin(); it != hostPartitions.end(); ++it) {
        graphPartitionedHosts[it->first] = {getPortByHost(it->first), getDataPortByHost(it->first),
                                            hostPartitions[it->first]};
    }

    return graphPartitionedHosts;
}

bool JasmineGraphServer::hasEnding(std::string const &fullString, std::string const &ending) {
    if (fullString.length() >= ending.length()) {
        return (0 == fullString.compare(fullString.length() - ending.length(), ending.length(), ending));
    } else {
        return false;
    }
}

std::vector<std::string> JasmineGraphServer::getWorkerVector(std::string workerList) {
    std::vector<std::string> workerVector = Utils::split(workerList, ',');
    return workerVector;
}

void JasmineGraphServer::backupPerformanceDB() {
    std::string performanceDBPath = Utils::getJasmineGraphProperty("org.jasminegraph.performance.db.location");

    uint64_t currentTimestamp =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count();

    std::string backupCommand = "cp " + performanceDBPath + " " + performanceDBPath + "-" + to_string(currentTimestamp);

    if (system(backupCommand.c_str())) {
        server_logger.error("Error executing command: " + backupCommand);
    }
}

void JasmineGraphServer::clearPerformanceDB() {
    this->performanceSqlite->runUpdate("delete from host_performance_data");
    this->performanceSqlite->runUpdate("delete from place_performance_data");
    this->performanceSqlite->runUpdate("delete from place");
    this->performanceSqlite->runUpdate("delete from host");
}

void JasmineGraphServer::addInstanceDetailsToPerformanceDB(std::string host, std::vector<int> portVector,
                                                           std::string isMaster) {
    std::vector<int>::iterator it;
    std::string hostString;
    std::string hostId;
    std::string user;
    std::string ipAddress;
    int count = 0;

    if (host.find('@') != std::string::npos) {
        vector<string> splitted = Utils::split(host, '@');
        ipAddress = splitted[1];
        user = splitted[0];
    } else {
        ipAddress = host;
    }

    std::string searchHost = "select idhost,ip from host where ip='" + ipAddress + "'";
    vector<vector<pair<string, string>>> selectResult = this->performanceSqlite->runSelect(searchHost);

    if (selectResult.size() > 0) {
        hostId = selectResult[0][0].second;
    } else {
        std::string hostInsertQuery =
            "insert into host (name, ip, is_public, total_cpu_cores, total_memory) values ('" + host + "','" +
            ipAddress + "','false','','')";
        int insertedHostId = this->performanceSqlite->runInsert(hostInsertQuery);
        hostId = to_string(insertedHostId);
    }

    std::string insertPlaceQuery =
        "insert into place (host_idhost,server_port,ip,user,is_master,is_host_reporter) values ";

    for (it = portVector.begin(); it < portVector.end(); it++) {
        std::string isHostReporter = "false";
        int port = (*it);
        std::string searchPlaceQuery = "select idplace from place where ip='" + ipAddress + "' and host_idhost='" +
                                       hostId + "' and server_port='" + to_string(port) + "';";
        vector<vector<pair<string, string>>> placeSearchResult = this->performanceSqlite->runSelect(searchPlaceQuery);

        if (placeSearchResult.size() > 0) {
            continue;
        }

        if (count == 0) {
            std::string searchReporterQuery =
                "select idplace from place where ip='" + ipAddress + "' and is_host_reporter='true'";
            vector<vector<pair<string, string>>> searchResult = this->performanceSqlite->runSelect(searchReporterQuery);
            if (searchResult.size() == 0) {
                isHostReporter = "true";
            }
        }
        hostString += "('" + hostId + "','" + to_string(port) + "','" + ipAddress + "','" + user + "','" + isMaster +
                      "','" + isHostReporter + "'),";
        count++;
    }

    hostString = hostString.substr(0, hostString.length() - 1);

    if (hostString.length() == 0) {
        return;
    }

    insertPlaceQuery = insertPlaceQuery + hostString;

    this->performanceSqlite->runInsert(insertPlaceQuery);
}

static void degreeDistributionCommon(std::string graphID, std::string command) {
    std::map<std::string, JasmineGraphServer::workerPartitions> graphPartitionedHosts =
        JasmineGraphServer::getGraphPartitionedHosts(graphID);
    std::string workerList;
    std::map<std::string, JasmineGraphServer::workerPartitions>::iterator workerit;
    for (workerit = graphPartitionedHosts.begin(); workerit != graphPartitionedHosts.end(); workerit++) {
        JasmineGraphServer::workerPartitions workerPartition = workerit->second;
        string host = workerit->first;
        int port = workerPartition.port;

        for (std::vector<std::string>::iterator partitionit = workerPartition.partitionID.begin();
             partitionit != workerPartition.partitionID.end(); partitionit++) {
            std::string partition = *partitionit;
            workerList.append(host + ":" + std::to_string(port) + ":" + partition + ",");
        }
    }

    workerList.pop_back();

    int sockfd;
    char data[FED_DATA_LENGTH + 1];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    for (workerit = graphPartitionedHosts.begin(); workerit != graphPartitionedHosts.end(); workerit++) {
        JasmineGraphServer::workerPartitions workerPartition = workerit->second;
        string host = workerit->first;
        int port = workerPartition.port;

        sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd < 0) {
            server_logger.error("Cannot create socket");
            continue;
        }

        server = gethostbyname(host.c_str());
        if (server == NULL) {
            server_logger.error("ERROR, no host named " + host);
            continue;
        }

        for (std::vector<std::string>::iterator partitionit = workerPartition.partitionID.begin();
             partitionit != workerPartition.partitionID.end(); partitionit++) {
            std::string partition = *partitionit;
            bzero((char *)&serv_addr, sizeof(serv_addr));
            serv_addr.sin_family = AF_INET;
            bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
            serv_addr.sin_port = htons(port);
            if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
                continue;
            }

            if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH, command,
                                           JasmineGraphInstanceProtocol::OK)) {
                Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
                close(sockfd);
                continue;
            }

            if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH, graphID,
                                           JasmineGraphInstanceProtocol::OK)) {
                Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
                close(sockfd);
                continue;
            }

            if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH, partition,
                                           JasmineGraphInstanceProtocol::OK)) {
                Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
                close(sockfd);
                continue;
            }

            if (!Utils::send_str_wrapper(sockfd, workerList)) {
                Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
                close(sockfd);
                continue;
            }
            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
            close(sockfd);
        }
    }
}

void JasmineGraphServer::inDegreeDistribution(std::string graphID) {
    degreeDistributionCommon(graphID, JasmineGraphInstanceProtocol::IN_DEGREE_DISTRIBUTION);
}

void JasmineGraphServer::outDegreeDistribution(std::string graphID) {
    degreeDistributionCommon(graphID, JasmineGraphInstanceProtocol::OUT_DEGREE_DISTRIBUTION);
}

long JasmineGraphServer::getGraphVertexCount(std::string graphID) {
    auto *refToSqlite = new SQLiteDBInterface();
    refToSqlite->init();
    vector<vector<pair<string, string>>> output =
        refToSqlite->runSelect("SELECT vertexcount FROM graph WHERE idgraph = '" + graphID + "'");
    refToSqlite->finalize();
    delete refToSqlite;

    long vertexCount = std::stoi(output[0][0].second);
    return vertexCount;
}

void JasmineGraphServer::duplicateCentralStore(std::string graphID) {
    std::map<std::string, JasmineGraphServer::workerPartitions> graphPartitionedHosts =
        JasmineGraphServer::getGraphPartitionedHosts(graphID);
    std::string workerList;
    std::map<std::string, JasmineGraphServer::workerPartitions>::iterator workerit;
    for (workerit = graphPartitionedHosts.begin(); workerit != graphPartitionedHosts.end(); workerit++) {
        JasmineGraphServer::workerPartitions workerPartition = workerit->second;
        string host = workerit->first;
        int port = workerPartition.port;
        int dport = workerPartition.dataPort;

        for (std::vector<std::string>::iterator partitionit = workerPartition.partitionID.begin();
             partitionit != workerPartition.partitionID.end(); partitionit++) {
            std::string partition = *partitionit;
            workerList.append(host + ":" + std::to_string(port) + ":" + partition + ":" + to_string(dport) + ",");
        }
    }
    workerList.pop_back();

    int sockfd;
    char data[FED_DATA_LENGTH + 1];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    for (workerit = graphPartitionedHosts.begin(); workerit != graphPartitionedHosts.end(); workerit++) {
        JasmineGraphServer::workerPartitions workerPartition = workerit->second;
        string host = workerit->first;
        int port = workerPartition.port;

        if (host.find('@') != std::string::npos) {
            host = Utils::split(host, '@')[1];
        }

        sockfd = socket(AF_INET, SOCK_STREAM, 0);

        if (sockfd < 0) {
            server_logger.error("Cannot create socket");
            continue;
        }
        server = gethostbyname(host.c_str());
        if (server == NULL) {
            server_logger.error("ERROR, no host named " + host);
            continue;
        }

        for (std::vector<std::string>::iterator partitionit = workerPartition.partitionID.begin();
             partitionit != workerPartition.partitionID.end(); partitionit++) {
            std::string partition = *partitionit;
            bzero((char *)&serv_addr, sizeof(serv_addr));
            serv_addr.sin_family = AF_INET;
            bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
            serv_addr.sin_port = htons(port);
            if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
                continue;
            }

            if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH,
                                           JasmineGraphInstanceProtocol::DP_CENTRALSTORE,
                                           JasmineGraphInstanceProtocol::OK)) {
                Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
                close(sockfd);
                continue;
            }

            if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH, graphID,
                                           JasmineGraphInstanceProtocol::OK)) {
                Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
                close(sockfd);
                continue;
            }

            if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH, partition,
                                           JasmineGraphInstanceProtocol::OK)) {
                Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
                close(sockfd);
                continue;
            }

            if (!Utils::send_str_wrapper(sockfd, workerList)) {
                close(sockfd);
                continue;
            }
            server_logger.info("Sent: " + workerList);
            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
            close(sockfd);
        }
    }
}

void JasmineGraphServer::initiateFiles(std::string graphID, std::string trainingArgs) {
    int count = 0;
    map<string, map<int, int>> scheduleForAllHosts = JasmineGraphTrainingSchedular::schedulePartitionTraining(graphID);
    std::map<std::string, JasmineGraphServer::workerPartitions> graphPartitionedHosts =
        this->getGraphPartitionedHosts(graphID);
    int partition_count = 0;
    std::map<std::string, JasmineGraphServer::workerPartitions>::iterator mapIterator;
    for (mapIterator = graphPartitionedHosts.begin(); mapIterator != graphPartitionedHosts.end(); mapIterator++) {
        JasmineGraphServer::workerPartitions workerPartition = mapIterator->second;
        partition_count += (int)(workerPartition.partitionID.size());
    }

    std::thread workerThreads[partition_count + 1];

    string prefix = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.trainedmodelfolder");
    string attr_prefix = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    std::map<std::string, JasmineGraphServer::workerPartitions>::iterator j;
    for (j = graphPartitionedHosts.begin(); j != graphPartitionedHosts.end(); j++) {
        JasmineGraphServer::workerPartitions workerPartition = j->second;
        std::vector<std::string>::iterator k;
        map<int, int> scheduleOfHost = scheduleForAllHosts[j->first];
        for (k = workerPartition.partitionID.begin(); k != workerPartition.partitionID.end(); k++) {
            int iterationOfPart = scheduleOfHost[stoi(*k)];
            workerThreads[count++] = std::thread(initiateTrain, j->first, workerPartition.port, trainingArgs + " " + *k,
                                                 iterationOfPart, this->masterHost);
        }
    }

    for (int threadCount = 0; threadCount < count; threadCount++) {
        workerThreads[threadCount].join();
        server_logger.info("Thread : " + to_string(threadCount) + " joined");
    }
}

void JasmineGraphServer::initiateCommunication(std::string graphID, std::string trainingArgs, SQLiteDBInterface *sqlite,
                                               std::string masterIP) {
    int fl_clients = stoi(Utils::getJasmineGraphProperty("org.jasminegraph.fl_clients"));
    int threadLimit = fl_clients + 1;
    std::thread *workerThreads = new std::thread[threadLimit];

    Utils::worker workerInstance;
    vector<Utils::worker> workerVector = Utils::getWorkerList(sqlite);

    int threadID = 0;

    for (int i = 0; i < workerVector.size(); i++) {
        workerInstance = workerVector[i];

        int serverPort = stoi(workerInstance.port);

        if (i == 0) {
            workerThreads[threadID] =
                std::thread(initiateServer, workerInstance.hostname, serverPort, trainingArgs, fl_clients, masterIP);
            threadID++;
        }

        workerThreads[threadID] = std::thread(initiateClient, workerInstance.hostname, serverPort,
                                              trainingArgs + " " + to_string(i), fl_clients, masterIP);
        threadID++;
    }

    for (int threadCount = 0; threadCount < threadLimit; threadCount++) {
        workerThreads[threadCount].join();
    }
    server_logger.info("Federated learning commands sent");
    delete[] workerThreads;
}

void JasmineGraphServer::initiateOrgCommunication(std::string graphID, std::string trainingArgs,
                                                  SQLiteDBInterface *sqlite, std::string masterIP) {
    int fl_clients = stoi(Utils::getJasmineGraphProperty("org.jasminegraph.fl_clients"));
    int orgs_count = stoi(Utils::getJasmineGraphProperty("org.jasminegraph.fl.num.orgs"));
    std::string flagPath = Utils::getJasmineGraphProperty("org.jasminegraph.fl.flag.file");
    int threadLimit = fl_clients + 1;
    int org_thread_count = 0;

    Utils::editFlagOne(flagPath);
    Utils::worker workerInstance;
    vector<Utils::worker> workerVector = Utils::getWorkerList(sqlite);

    if (Utils::getJasmineGraphProperty("org.jasminegraph.fl.aggregator") == "true") {
        initiateAggregator("localhost", stoi(workerVector[0].port), trainingArgs, fl_clients, masterIP);
    }

    std::ifstream file(Utils::getJasmineGraphProperty("org.jasminegraph.fl.organization.file"));

    if (file.good()) {
        if (file.is_open()) {
            std::string line;
            std::thread *trainThreads = new std::thread[orgs_count];
            while (std::getline(file, line)) {
                line = Utils::trim_copy(line);
                std::vector<std::string> strArr = Utils::split(line.c_str(), '|');
                std::string host = strArr[0].c_str();
                int port = stoi(strArr[1].c_str());
                std::string trainingArgs = "--graph_id " + strArr[2];

                if (strArr.size() == 3 && org_thread_count <= orgs_count) {
                    trainThreads[org_thread_count] = std::thread(sendTrainCommand, host, port, trainingArgs);
                    org_thread_count++;
                }
            }
            file.close();
            server_logger.info("Organizational details loading successful");
        }
    } else {
        server_logger.error("Error loading organization details");
    }

    std::thread *workerThreads = new std::thread[threadLimit];
    std::thread *communicationThread = new std::thread[1];
    communicationThread[0] = std::thread(receiveGlobalWeights, "localhost", 5000, "0", 1, "1");
    server_logger.info("Communication Thread Initiated");
    int threadID = 0;

    for (int i = 0; i < workerVector.size(); i++) {
        workerInstance = workerVector[i];
        int serverPort = stoi(workerInstance.port);

        if (i == 0) {
            workerThreads[threadID] =
                std::thread(initiateOrgServer, workerInstance.hostname, serverPort, trainingArgs, fl_clients, masterIP);
            threadID++;
        }

        workerThreads[threadID] = std::thread(initiateClient, workerInstance.hostname, serverPort,
                                              trainingArgs + " " + to_string(i), fl_clients, masterIP);
        threadID++;
    }

    communicationThread[0].join();

    for (int threadCount = 0; threadCount < threadLimit; threadCount++) {
        workerThreads[threadCount].join();
    }

    server_logger.info("Federated learning commands sent");
    delete[] workerThreads;
    delete[] communicationThread;
}

void JasmineGraphServer::initiateMerge(std::string graphID, std::string trainingArgs, SQLiteDBInterface *sqlite) {
    map<string, map<int, int>> scheduleForAllHosts = JasmineGraphTrainingSchedular::schedulePartitionTraining(graphID);
    std::map<std::string, JasmineGraphServer::workerPartitions> graphPartitionedHosts =
        this->getGraphPartitionedHosts(graphID);
    int partition_count = 0;
    std::map<std::string, JasmineGraphServer::workerPartitions>::iterator mapIterator;
    for (mapIterator = graphPartitionedHosts.begin(); mapIterator != graphPartitionedHosts.end(); mapIterator++) {
        JasmineGraphServer::workerPartitions workerPartition = mapIterator->second;
        partition_count += (int)(workerPartition.partitionID.size());
    }

    std::thread *workerThreads = new std::thread[partition_count];

    int fl_clients = stoi(Utils::getJasmineGraphProperty("org.jasminegraph.fl_clients"));
    std::map<std::string, JasmineGraphServer::workerPartitions>::iterator j;
    int count = 0;
    for (j = graphPartitionedHosts.begin(); j != graphPartitionedHosts.end(); j++) {
        JasmineGraphServer::workerPartitions workerPartition = j->second;
        std::vector<std::string>::iterator k;
        map<int, int> scheduleOfHost = scheduleForAllHosts[j->first];
        for (k = workerPartition.partitionID.begin(); k != workerPartition.partitionID.end(); k++) {
            int iterationOfPart = scheduleOfHost[stoi(*k)];
            workerThreads[count] = std::thread(mergeFiles, j->first, workerPartition.port, trainingArgs + " " + *k,
                                               fl_clients, *k, this->masterHost);
            count++;
        }
    }

    for (int threadCount = 0; threadCount < count; threadCount++) {
        workerThreads[threadCount].join();
    }
    server_logger.info("Merge Commands Sent");
    delete[] workerThreads;
}

static bool initiateCommon(std::string host, int port, std::string trainingArgs, int iteration, std::string masterIP,
                           std::string initType) {
    bool result = true;
    int sockfd;
    char data[FED_DATA_LENGTH + 1];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        server_logger.error("Cannot create socket");
        return false;
    }

    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        server_logger.error("ERROR, can not find the host");
        return false;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        return false;
    }

    if (!Utils::performHandshake(sockfd, data, FED_DATA_LENGTH, masterIP)) {
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return false;
    }

    if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH, initType, JasmineGraphInstanceProtocol::OK)) {
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return false;
    }

    if (!Utils::send_str_wrapper(sockfd, trainingArgs)) {
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return false;
    }
    server_logger.info("Sent: " + trainingArgs);

    Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
    close(sockfd);
    return true;
}

static bool initiateTrain(std::string host, int port, std::string trainingArgs, int iteration, std::string masterIP) {
    return initiateCommon(host, port, trainingArgs, iteration, masterIP,
                          JasmineGraphInstanceProtocol::INITIATE_FED_PREDICT);
}

static bool initiatePredict(std::string host, int port, std::string trainingArgs, int iteration, std::string masterIP) {
    return initiateCommon(host, port, trainingArgs, iteration, masterIP, JasmineGraphInstanceProtocol::INITIATE_FILES);
}

static bool initiateServer(std::string host, int port, std::string trainingArgs, int iteration, std::string masterIP) {
    return initiateCommon(host, port, trainingArgs, iteration, masterIP, JasmineGraphInstanceProtocol::INITIATE_SERVER);
}

// todo Remove partCount from parameters as the partition id is being parsed within the training args
static bool initiateClient(std::string host, int port, std::string trainingArgs, int iteration, std::string masterIP) {
    return initiateCommon(host, port, trainingArgs, iteration, masterIP, JasmineGraphInstanceProtocol::INITIATE_CLIENT);
}

static bool initiateAggregator(std::string host, int port, std::string trainingArgs, int iteration,
                               std::string masterIP) {
    return initiateCommon(host, port, trainingArgs, iteration, masterIP, JasmineGraphInstanceProtocol::INITIATE_AGG);
}

static bool initiateOrgServer(std::string host, int port, std::string trainingArgs, int iteration,
                              std::string masterIP) {
    return initiateCommon(host, port, trainingArgs, iteration, masterIP,
                          JasmineGraphInstanceProtocol::INITIATE_ORG_SERVER);
}

bool JasmineGraphServer::receiveGlobalWeights(std::string host, int port, std::string trainingArgs, int iteration,
                                              std::string partCount) {
    int HEADER_LENGTH = 10;
    bool result = true;
    int sockfd;

    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    std::string weights_file = Utils::getJasmineGraphProperty("org.jasminegraph.fl.weights.file");
    std::string flagPath = Utils::getJasmineGraphProperty("org.jasminegraph.fl.flag.file");

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        server_logger.error("Cannot create socket");
        return false;
    }
    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }
    server = gethostbyname(host.c_str());
    if (server == NULL) {
        server_logger.error("ERROR, can not find the host");
        return false;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr));

    bool isTrue = false;
    int count = 0;

    while (true) {
        char data[WEIGHTS_DATA_LENGTH];
        read(sockfd, data, WEIGHTS_DATA_LENGTH);
        std::string length = "";

        for (int i = 0; i < HEADER_LENGTH; i++) {
            if (data[i]) {
                length += data[i];
            }
        }
        server_logger.info("message length : " + length);
        char content[stoi(length)];
        int full_size = stoi(length) + HEADER_LENGTH;

        // FIXME: content has a capacity of (full_size - HEADER_LENGTH)
        // but in this loop, index i will increase upto full_size
        // which leads to a buffer overflow
        for (int i = 0; i < full_size; i++) {
            content[i] = data[i];
        }

        ofstream stream;
        std::ofstream file(weights_file, std::ios::out);
        file.write(content, full_size);
        Utils::editFlagZero(flagPath);
        server_logger.info("Done writting to the weight file");
        std::string path = weights_file + to_string(count) + ".txt";
        count++;
        std::ofstream file1(path, std::ios::out | std::ios::out);
        file1.write(content, full_size);

        while (true) {
            sleep(DELAY);
            if (Utils::checkFlag(flagPath) == "1") {
                ifstream infile{weights_file};
                server_logger.info("Weight file opened");
                string file_contents{istreambuf_iterator<char>(infile), istreambuf_iterator<char>()};
                server_logger.info("Reading weights");
                write(sockfd, file_contents.c_str(), file_contents.size());
                server_logger.info("Writing to the socket");
                break;

            } else if (Utils::checkFlag(flagPath) == "STOP") {
                isTrue = true;
                break;
            }
        }
        server_logger.info("Round completed");
        sleep(DELAY);

        if (isTrue == true) {
            break;
        }
    }
    Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
    close(sockfd);
    return true;
}

bool JasmineGraphServer::mergeFiles(std::string host, int port, std::string trainingArgs, int iteration,
                                    string partCount, std::string masterIP) {
    bool result = true;
    int sockfd;
    char data[FED_DATA_LENGTH + 1];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        server_logger.error("Cannot create socket");
        return false;
    }

    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        server_logger.error("ERROR, can not find the host");
        return false;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        return false;
    }

    if (!Utils::performHandshake(sockfd, data, FED_DATA_LENGTH, masterIP)) {
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return false;
    }

    if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH, JasmineGraphInstanceProtocol::MERGE_FILES,
                                   JasmineGraphInstanceProtocol::OK)) {
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return false;
    }

    if (!Utils::send_str_wrapper(sockfd, trainingArgs)) {
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return false;
    }
    server_logger.info("Sent: " + trainingArgs);

    Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
    close(sockfd);
    return true;
}

bool JasmineGraphServer::sendTrainCommand(std::string host, int port, std::string trainingArgs) {
    bool result = true;
    int sockfd;
    char data[FED_DATA_LENGTH + 1];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        server_logger.error("Cannot create socket");
        return false;
    }

    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        server_logger.error("ERROR, can not find the host");
        return false;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        return false;
    }

    if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH, JasmineGraphInstanceProtocol::INITIATE_TRAIN,
                                   JasmineGraphInstanceProtocol::OK)) {
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return false;
    }

    if (!Utils::send_str_wrapper(sockfd, trainingArgs)) {
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return false;
    }
    server_logger.info("Sent: " + trainingArgs);

    Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
    close(sockfd);
    return true;
}

void JasmineGraphServer::egoNet(std::string graphID) {
    std::map<std::string, JasmineGraphServer::workerPartitions> graphPartitionedHosts =
        JasmineGraphServer::getGraphPartitionedHosts(graphID);
    std::string workerList;

    for (auto workerit = graphPartitionedHosts.begin(); workerit != graphPartitionedHosts.end(); workerit++) {
        JasmineGraphServer::workerPartitions workerPartition = workerit->second;
        string host = workerit->first;
        int port = workerPartition.port;

        for (std::vector<std::string>::iterator partitionit = workerPartition.partitionID.begin();
             partitionit != workerPartition.partitionID.end(); partitionit++) {
            std::string partition = *partitionit;
            workerList.append(host + ":" + std::to_string(port) + ":" + partition + ",");
        }
    }

    workerList.pop_back();

    int sockfd;
    char data[FED_DATA_LENGTH + 1];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    for (auto workerit = graphPartitionedHosts.begin(); workerit != graphPartitionedHosts.end(); workerit++) {
        JasmineGraphServer::workerPartitions workerPartition = workerit->second;
        string host = workerit->first;
        int port = workerPartition.port;
        sockfd = socket(AF_INET, SOCK_STREAM, 0);

        if (sockfd < 0) {
            server_logger.error("Cannot create socket");
            return;
        }
        server = gethostbyname(host.c_str());
        if (server == NULL) {
            server_logger.error("ERROR, no host named " + host);
            return;
        }

        for (std::vector<std::string>::iterator partitionit = workerPartition.partitionID.begin();
             partitionit != workerPartition.partitionID.end(); partitionit++) {
            std::string partition = *partitionit;
            bzero((char *)&serv_addr, sizeof(serv_addr));
            serv_addr.sin_family = AF_INET;
            bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
            serv_addr.sin_port = htons(port);
            if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
                return;
            }

            if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH, JasmineGraphInstanceProtocol::EGONET,
                                           JasmineGraphInstanceProtocol::OK)) {
                Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
                close(sockfd);
                return;
            }

            if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH, graphID,
                                           JasmineGraphInstanceProtocol::OK)) {
                Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
                close(sockfd);
                return;
            }

            if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH, partition,
                                           JasmineGraphInstanceProtocol::OK)) {
                Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
                close(sockfd);
                return;
            }

            if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH, workerList,
                                           JasmineGraphInstanceProtocol::OK)) {
                Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
                close(sockfd);
                return;
            }

            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
            close(sockfd);
        }
    }
}
