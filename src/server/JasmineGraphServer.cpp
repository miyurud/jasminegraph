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

#include "../ml/trainer/JasmineGraphTrainingSchedular.h"
#include "../partitioner/local/MetisPartitioner.h"
#include "../util/Utils.h"
#include "../util/logger/Logger.h"
#include "JasmineGraphInstance.h"
#include "JasmineGraphInstanceProtocol.h"

Logger server_logger;

static void copyArtifactsToWorkers(const std::string &workerPath, const std::string &artifactLocation,
                                   const std::string &remoteWorker);
static void createLogFilePath(const std::string &workerHost, const std::string &workerPath);
static void deleteWorkerPath(const std::string &workerHost, const std::string &workerPath);
static void assignPartitionToWorker(std::string fileName, int graphId, std::string workerHost, int workerPort,
                                    int workerDataPort);
static void updateMetaDB(int graphID, std::string uploadEndTime);
static bool batchUploadCommon(std::string host, int port, int dataPort, int graphID, std::string filePath,
                              std::string masterIP, std::string uploadType);
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
static bool initiateCommon(std::string host, int port, int dataPort, std::string trainingArgs, int iteration,
                           std::string partCount, std::string masterIP, std::string initType);
static bool initiateTrain(std::string host, int port, int dataPort, std::string trainingArgs, int iteration,
                          std::string partCount, std::string masterIP);
static bool initiatePredict(std::string host, int port, int dataPort, std::string trainingArgs, int iteration,
                            std::string partCount, std::string masterIP);
static bool initiateServer(std::string host, int port, int dataPort, std::string trainingArgs, int iteration,
                           std::string partCount, std::string masterIP);
static bool initiateClient(std::string host, int port, int dataPort, std::string trainingArgs, int iteration,
                           std::string partCount, std::string masterIP);
static bool initiateAggregator(std::string host, int port, int dataPort, std::string trainingArgs, int iteration,
                               std::string partCount, std::string masterIP);
static bool initiateOrgServer(std::string host, int port, int dataPort, std::string trainingArgs, int iteration,
                              std::string partCount, std::string masterIP);
static void degreeDistributionCommon(std::string graphID, std::string command);

static map<string, string> hostIDMap;
static std::vector<JasmineGraphServer::workers> hostWorkerMap;
static map<string, pair<int, int>> hostPortMap;
std::map<int, int> aggregateWeightMap;

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

JasmineGraphServer *JasmineGraphServer::getInstance() {
    static JasmineGraphServer *instance = nullptr;
    // TODO(thevindu-w): synchronize
    if (instance == nullptr) {
        instance = new JasmineGraphServer();
    }
    return instance;
}

JasmineGraphServer::~JasmineGraphServer() {
    server_logger.info("Freeing up server resources.");
    this->sqlite->finalize();
    this->performanceSqlite->finalize();
    delete this->sqlite;
    delete this->performanceSqlite;
    delete this->jobScheduler;
}

int JasmineGraphServer::run(std::string profile, std::string masterIp, int numberofWorkers, std::string workerIps,
                            std::string enableNmon) {
    server_logger.info("Running the server...");
    std::vector<int> masterPortVector;
    if (masterIp.empty()) {
        this->masterHost = Utils::getJasmineGraphProperty("org.jasminegraph.server.host");
    } else {
        this->masterHost = masterIp;
    }
    server_logger.info("masterHost = " + this->masterHost);
    this->profile = profile;
    this->numberOfWorkers = numberofWorkers;
    this->workerHosts = workerIps;
    this->enableNmon = enableNmon;
    init();
    masterPortVector.push_back(Conts::JASMINEGRAPH_FRONTEND_PORT);
    updateOperationalGraphList();

    if (profile == Conts::PROFILE_K8S) {
        k8sWorkerController = new K8sWorkerController(masterIp, numberofWorkers, sqlite);
        std::string selectQuery = "select ip,server_port,server_data_port from worker";
        std::vector<vector<pair<string, string>>> output = this->sqlite->runSelect(selectQuery);
        for (std::vector<vector<pair<string, string>>>::iterator i = output.begin(); i != output.end(); ++i) {
            std::vector<pair<string, string>>::iterator j = (i->begin());
            std::string ip = j->second;
            ++j;
            std::string port = j->second;
            ++j;
            std::string dataPort = j->second;
            hostWorkerMap.push_back({ip, atoi(port.c_str()), atoi(dataPort.c_str())});
        }

    } else {
        start_workers();
        addInstanceDetailsToPerformanceDB(masterHost, masterPortVector, "true");
    }

    std::thread *myThreads = new std::thread[1];
    myThreads[0] = std::thread(StatisticCollector::logLoadAverage, "Load Average");
    sleep(2);
    waitForAcknowledgement(numberofWorkers);
    resolveOperationalGraphs();
    initiateAggregateMap();
    return 0;
}

bool JasmineGraphServer::isRunning() { return true; }

void JasmineGraphServer::init() {
    pthread_t frontendthread;
    pthread_t backendthread;
    pthread_create(&frontendthread, NULL, runfrontend, this);
    pthread_create(&backendthread, NULL, runbackend, this);
}

void JasmineGraphServer::start_workers() {
    int hostListModeNWorkers = 0;
    int numberOfWorkersPerHost;
    std::vector<std::string> hostsList;
    std::string nWorkers;
    if (profile == Conts::PROFILE_NATIVE) {
        hostsList = Utils::getHostListFromProperties();
        if ((this->numberOfWorkers) == -1) {
            nWorkers = Utils::getJasmineGraphProperty("org.jasminegraph.server.nworkers");
        }
        enableNmon = Utils::getJasmineGraphProperty("org.jasminegraph.server.enable.nmon");
    } else if (profile == Conts::PROFILE_DOCKER) {
        hostsList = getWorkerVector(workerHosts);
    }

    if (hostsList.size() == 0) {
        server_logger.error("At least one host needs to be specified");
        exit(-1);
    }

    this->sqlite->runUpdate("DELETE FROM host");

    std::vector<std::string>::iterator it;
    it = hostsList.begin();
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
    it = hostsList.begin();

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
            hostWorkerMap.push_back({*it, workerPort, workerDataPort});
            // FIXME: When there are more than 1 worker in the same host, one workers ports will replace the entries of
            // other workers port entries in hostPortMap
            hostPortMap.insert((pair<string, pair<int, int>>(*it, make_pair(workerPort, workerDataPort))));
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
            hostWorkerMap.push_back({*it, workerPort, workerDataPort});
            hostPortMap.insert(((pair<string, pair<int, int>>(*it, make_pair(workerPort, workerDataPort)))));
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
                                       profile, masterHost, enableNmon);
        count++;
    }

    for (int threadCount = 0; threadCount < hostListSize; threadCount++) {
        myThreads[threadCount].join();
    }
    delete[] myThreads;
    hostIDMap = getLiveHostIDList();
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
                                            string host, string profile, string masterHost, string enableNmon) {
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
    server_logger.info("###MASTER#### Starting remote workers for profile " + profile);
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

    if (profile == Conts::PROFILE_NATIVE) {
        copyArtifactsToWorkers(workerPath, artifactPath, host);
        for (int i = 0; i < workerPortsVector.size(); i++) {
            if (host.find("localhost") != std::string::npos) {
                serverStartScript = executableFile + " native 2 " + host + " " + masterHost + " " +
                                    std::to_string(workerPortsVector.at(i)) + " " +
                                    std::to_string(workerDataPortsVector.at(i)) + " " + enableNmon;
            } else {
                serverStartScript = "ssh -p 22 " + host + " " + executableFile + " native 2 " + host + " " +
                                    masterHost + " " + std::to_string(workerPortsVector.at(i)) + " " +
                                    std::to_string(workerDataPortsVector.at(i)) + " " + enableNmon;
            }
            const char *commandStr = serverStartScript.c_str();
            pid_t child = fork();
            if (child == 0) {
                execl("/bin/sh", "sh", "-c", commandStr, nullptr);
                _exit(1);
            }
        }
    } else if (profile == Conts::PROFILE_DOCKER) {
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
                        "/home/ubuntu/software/jasminegraph/logs" + " -p " + std::to_string(workerPortsVector.at(i)) +
                        ":" + std::to_string(workerPortsVector.at(i)) + " -p " +
                        std::to_string(workerDataPortsVector.at(i)) + ":" +
                        std::to_string(workerDataPortsVector.at(i)) + " -e WORKER_ID=" + to_string(i) +
                        " jasminegraph:latest --MODE 2 --HOST_NAME " + host + " --MASTERIP " + masterHost +
                        " --SERVER_PORT " + std::to_string(workerPortsVector.at(i)) + " --SERVER_DATA_PORT " +
                        std::to_string(workerDataPortsVector.at(i)) + " --ENABLE_NMON " + enableNmon;
                }
            } else {
                if (is_testing) {
                    serverStartScript =
                        "docker -H ssh://" + host + " run -p " + std::to_string(workerPortsVector.at(i)) + ":" +
                        std::to_string(workerPortsVector.at(i)) + " -p " + std::to_string(workerDataPortsVector.at(i)) +
                        ":" + std::to_string(workerDataPortsVector.at(i)) + " -e WORKER_ID=" + to_string(i) +
                        " jasminegraph:test --MODE 2 --HOST_NAME " + host + " --MASTERIP " + masterHost +
                        " --SERVER_PORT " + std::to_string(workerPortsVector.at(i)) + " --SERVER_DATA_PORT " +
                        std::to_string(workerDataPortsVector.at(i)) + " --ENABLE_NMON " + enableNmon + " >" +
                        worker_logdir + "/worker.log 2>&1";
                } else {
                    serverStartScript =
                        "docker -H ssh://" + host + " run -v " + instanceDataFolder + ":" + instanceDataFolder +
                        " -v " + aggregateDataFolder + ":" + aggregateDataFolder + " -v " + nmonFileLocation + ":" +
                        nmonFileLocation + " -v " + instanceDataFolder + "/" + to_string(i) + "/logs" + ":" +
                        "/home/ubuntu/software/jasminegraph/logs" + " -p " + std::to_string(workerPortsVector.at(i)) +
                        ":" + std::to_string(workerPortsVector.at(i)) + " -p " +
                        std::to_string(workerDataPortsVector.at(i)) + ":" +
                        std::to_string(workerDataPortsVector.at(i)) + " -e WORKER_ID=" + to_string(i) +
                        " jasminegraph:latest --MODE 2 --HOST_NAME " + host + " --MASTERIP " + masterHost +
                        " --SERVER_PORT " + std::to_string(workerPortsVector.at(i)) + " --SERVER_DATA_PORT " +
                        std::to_string(workerDataPortsVector.at(i)) + " --ENABLE_NMON " + enableNmon;
                }
            }
            server_logger.info(serverStartScript);
            const char *serverStartCmd = serverStartScript.c_str();
            pid_t child = fork();
            if (child == 0) {
                execl("/bin/sh", "sh", "-c", serverStartCmd, nullptr);
                _exit(1);
            }
        }
    }
}

bool JasmineGraphServer::spawnNewWorker(string host, string port, string dataPort, string profile, string masterHost,
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

    JasmineGraphServer::startRemoteWorkers(workerPortsVector, workerDataPortsVector, host, profile, masterHost,
                                           enableNmon);
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

    for (std::vector<vector<pair<string, string>>>::iterator i = output.begin(); i != output.end(); ++i) {
        int workerID = -1;
        string graphID;
        string partitionID;
        std::vector<pair<string, string>>::iterator j = (i->begin());
        graphID = j->second;
        ++j;
        partitionID = j->second;
        ++j;
        workerID = std::stoi(j->second);
        std::vector<string> partitionList = partitionMap[workerID];
        partitionList.push_back(graphID + "_" + partitionID);
        partitionMap[workerID] = partitionList;
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
                std::vector<string> partitionList = partitionAggregatedMap[workerID];
                string data = ss.str();
                std::stringstream().swap(ss);
                counter = 0;
                data = data.substr(0, data.find_last_of(","));
                partitionList.push_back(data);
                partitionAggregatedMap[workerID] = partitionList;
            }
            ss << x->c_str() << ",";
            counter++;
        }

        std::vector<string> partitionList = partitionAggregatedMap[workerID];
        string data = ss.str();
        std::stringstream().swap(ss);
        counter = 0;
        data = data.substr(0, data.find_last_of(","));
        partitionList.push_back(data);
        partitionAggregatedMap[workerID] = partitionList;
    }

    sqlStatement = "SELECT idworker,ip,server_port FROM worker";
    output = this->sqlite->runSelect(sqlStatement);

    std::set<int> graphIDsFromWorkersSet;
    for (std::vector<vector<pair<string, string>>>::iterator i = output.begin(); i != output.end(); ++i) {
        int workerID = -1;
        string host;
        int workerPort = -1;
        string partitionID;
        std::vector<pair<string, string>>::iterator j = (i->begin());
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
            close(sockfd);
            return;
        }

        if (!Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::INITIATE_FRAGMENT_RESOLUTION)) {
            close(sockfd);
            continue;
        }
        server_logger.info("Sent: " + JasmineGraphInstanceProtocol::INITIATE_FRAGMENT_RESOLUTION);

        string response = Utils::read_str_trim_wrapper(sockfd, data, INSTANCE_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
            server_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::OK +
                                " ; Received: " + response);
            close(sockfd);
            continue;
        }
        server_logger.info("Received: " + response);

        std::vector<string> partitionList = partitionAggregatedMap[workerID];

        bool success = true;
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
            close(sockfd);
            continue;
        }
        if (!Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::FRAGMENT_RESOLUTION_DONE)) {
            close(sockfd);
            continue;
        }
        server_logger.info("Sent: " + JasmineGraphInstanceProtocol::FRAGMENT_RESOLUTION_DONE);

        response = Utils::read_str_trim_wrapper(sockfd, data, INSTANCE_DATA_LENGTH);
        if (response.compare("") != 0) {
            std::vector<string> listOfOitems = Utils::split(response, ',');
            for (std::vector<string>::iterator it = listOfOitems.begin(); it != listOfOitems.end(); it++) {
                graphIDsFromWorkersSet.insert(atoi(it->c_str()));
            }
        }
        close(sockfd);
    }

    sqlStatement = "SELECT idgraph FROM graph";
    std::vector<vector<pair<string, string>>> output2 = this->sqlite->runSelect(sqlStatement);
    std::set<int> graphIDsFromMetDBSet;
    for (std::vector<vector<pair<string, string>>>::iterator i = output2.begin(); i != output2.end(); ++i) {
        std::vector<pair<string, string>>::iterator j = (i->begin());
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
    std::thread *deleteThreads = new std::thread[hostPortMap.size()];
    // Iterate through all hosts
    for (std::map<string, pair<int, int>>::iterator it = hostPortMap.begin(); it != hostPortMap.end(); it++) {
        // Fetch hostname and port
        string hostname = it->first;
        int port = (it->second).first;
        // Initialize threads for host
        // Each thread runs the service to remove the given graph ID fragments in their datafolders
        deleteThreads[count++] =
            std::thread(removeFragmentThroughService, hostname, port, to_string(graphID), this->masterHost);
        sleep(1);
        server_logger.info("Deleted graph fragments of graph ID " + to_string(graphID));
    }

    for (int threadCount = 0; threadCount < count; threadCount++) {
        if (deleteThreads[threadCount].joinable()) {
            deleteThreads[threadCount].join();
        }
    }
}

void JasmineGraphServer::shutdown_workers() {
    server_logger.info("Shutting down workers");
    std::vector<workers, std::allocator<workers>>::iterator mapIterator;
    for (mapIterator = hostWorkerMap.begin(); mapIterator < hostWorkerMap.end(); mapIterator++) {
        workers worker = *mapIterator;
        bool result = true;
        server_logger.info("Host:" + worker.hostname + " Port:" + to_string(worker.port) +
                           " DPort:" + to_string(worker.dataPort));
        int sockfd;
        char data[FED_DATA_LENGTH + 1];
        bool loop = false;
        socklen_t len;
        struct sockaddr_in serv_addr;
        struct hostent *server;

        sockfd = socket(AF_INET, SOCK_STREAM, 0);

        if (sockfd < 0) {
            server_logger.error("Cannot create socket");
            return;
        }

        std::string host = worker.hostname;
        int port = worker.port;

        if (worker.hostname.find('@') != std::string::npos) {
            host = Utils::split(host, '@')[1];
        }

        server = gethostbyname(host.c_str());
        if (server == NULL) {
            server_logger.error("ERROR, no host named " + host);
            return;
        }

        bzero((char *)&serv_addr, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
        serv_addr.sin_port = htons(port);
        if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) != 0) {
            return;
        }

        if (!Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::SHUTDOWN)) {
            return;
        }
        server_logger.info("Sent: " + JasmineGraphInstanceProtocol::SHUTDOWN);

        string response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
        server_logger.info("Response: " + response);
    }
}

void JasmineGraphServer::uploadGraphLocally(int graphID, const string graphType,
                                            vector<std::map<int, string>> fullFileList, std::string masterIP) {
    server_logger.info("Uploading the graph locally..");
    std::map<int, string> partitionFileList = fullFileList[0];
    std::map<int, string> centralStoreFileList = fullFileList[1];
    std::map<int, string> centralStoreDuplFileList = fullFileList[2];
    std::map<int, string> compositeCentralStoreFileList = fullFileList[5];
    std::map<int, string> attributeFileList;
    std::map<int, string> centralStoreAttributeFileList;
    if (masterHost.empty()) {
        masterHost = Utils::getJasmineGraphProperty("org.jasminegraph.server.host");
    }
    int total_threads = partitionFileList.size() + centralStoreFileList.size() + centralStoreDuplFileList.size() +
                        compositeCentralStoreFileList.size();
    if (graphType == Conts::GRAPH_WITH_ATTRIBUTES) {
        attributeFileList = fullFileList[3];
        total_threads += attributeFileList.size();
        centralStoreAttributeFileList = fullFileList[4];
        total_threads += centralStoreAttributeFileList.size();
    }
    int count = 0;
    int file_count = 0;
    std::thread *workerThreads = new std::thread[total_threads];
    while (count < total_threads) {
        std::vector<workers, std::allocator<workers>>::iterator mapIterator;
        for (mapIterator = hostWorkerMap.begin(); mapIterator < hostWorkerMap.end(); mapIterator++) {
            workers worker = *mapIterator;
            if (count == total_threads) {
                break;
            }
            std::string partitionFileName = partitionFileList[file_count];
            workerThreads[count] = std::thread(batchUploadFile, worker.hostname, worker.port, worker.dataPort, graphID,
                                               partitionFileName, masterHost);
            count++;
            copyCentralStoreToAggregateLocation(centralStoreFileList[file_count]);
            workerThreads[count] = std::thread(batchUploadCentralStore, worker.hostname, worker.port, worker.dataPort,
                                               graphID, centralStoreFileList[file_count], masterHost);
            count++;

            if (compositeCentralStoreFileList.find(file_count) != compositeCentralStoreFileList.end()) {
                copyCentralStoreToAggregateLocation(compositeCentralStoreFileList[file_count]);
                workerThreads[count] =
                    std::thread(batchUploadCompositeCentralstoreFile, worker.hostname, worker.port, worker.dataPort,
                                graphID, compositeCentralStoreFileList[file_count], masterHost);
                count++;
            }

            workerThreads[count] = std::thread(batchUploadCentralStore, worker.hostname, worker.port, worker.dataPort,
                                               graphID, centralStoreDuplFileList[file_count], masterHost);
            count++;
            if (graphType == Conts::GRAPH_WITH_ATTRIBUTES) {
                workerThreads[count] = std::thread(batchUploadAttributeFile, worker.hostname, worker.port,
                                                   worker.dataPort, graphID, attributeFileList[file_count], masterHost);
                count++;
                workerThreads[count] =
                    std::thread(batchUploadCentralAttributeFile, worker.hostname, worker.port, worker.dataPort, graphID,
                                centralStoreAttributeFileList[file_count], masterHost);
                count++;
            }
            assignPartitionToWorker(partitionFileName, graphID, worker.hostname, worker.port, worker.dataPort);
            file_count++;
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

static void assignPartitionToWorker(std::string fileName, int graphId, std::string workerHost, int workerPort,
                                    int workerDataPort) {
    auto *refToSqlite = new SQLiteDBInterface();
    refToSqlite->init();
    size_t lastindex = fileName.find_last_of(".");
    string rawname = fileName.substr(0, lastindex);
    string partitionID = rawname.substr(rawname.find_last_of("_") + 1);

    if (workerHost.find('@') != std::string::npos) {
        workerHost = Utils::split(workerHost, '@')[1];
    }

    std::string workerSearchQuery = "select idworker from worker where ip='" + workerHost + "' and server_port='" +
                                    std::to_string(workerPort) + "' and server_data_port='" +
                                    std::to_string(workerDataPort) + "'";

    std::vector<vector<pair<string, string>>> results = refToSqlite->runSelect(workerSearchQuery);

    std::string workerID = results[0][0].second;

    std::string partitionToWorkerQuery =
        "insert into worker_has_partition (partition_idpartition, partition_graph_idgraph, worker_idworker) values "
        "('" +
        partitionID + "','" + std::to_string(graphId) + "','" + workerID + "')";

    refToSqlite->runInsert(partitionToWorkerQuery);
    refToSqlite->finalize();
    delete refToSqlite;
}

static bool batchUploadCommon(std::string host, int port, int dataPort, int graphID, std::string filePath,
                              std::string masterIP, std::string uploadType) {
    server_logger.info("Host:" + host + " Port:" + to_string(port) + " DPort:" + to_string(dataPort));
    bool result = true;
    int sockfd;
    char data[FED_DATA_LENGTH + 1];
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
        close(sockfd);
        return false;
    }

    if (!Utils::send_str_wrapper(sockfd, uploadType)) {
        close(sockfd);
        return false;
    }
    server_logger.info("Sent: " + uploadType);

    string response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        server_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::OK +
                            " ; Received: " + response);
        close(sockfd);
        return false;
    }
    server_logger.info("Received: " + response);

    if (!Utils::send_str_wrapper(sockfd, std::to_string(graphID))) {
        close(sockfd);
        return false;
    }
    server_logger.info("Sent: " + std::to_string(graphID));

    std::string fileName = Utils::getFileName(filePath);
    int fileSize = Utils::getFileSize(filePath);
    std::string fileLength = to_string(fileSize);

    response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_NAME) != 0) {
        server_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::SEND_FILE_NAME +
                            " ; Received: " + response);
        close(sockfd);
        return false;
    }
    server_logger.info("Received: " + response);

    if (!Utils::send_str_wrapper(sockfd, fileName)) {
        close(sockfd);
        return false;
    }
    server_logger.info("Sent: " + fileName);

    response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_LEN) != 0) {
        server_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::SEND_FILE_LEN +
                            " ; Received: " + response);
        close(sockfd);
        return false;
    }
    server_logger.info("Received: " + response);

    if (!Utils::send_str_wrapper(sockfd, fileLength)) {
        close(sockfd);
        return false;
    }
    server_logger.info("Sent: " + fileLength);

    response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_CONT) != 0) {
        server_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::SEND_FILE_CONT +
                            " ; Received: " + response);
        close(sockfd);
        return false;
    }
    server_logger.info("Received: " + response);

    server_logger.info("Going to send file through service");
    JasmineGraphServer::sendFileThroughService(host, dataPort, fileName, filePath, masterIP);

    int count = 0;
    while (true) {
        if (!Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::FILE_RECV_CHK)) {
            close(sockfd);
            return false;
        }
        server_logger.info("Sent: " + JasmineGraphInstanceProtocol::FILE_RECV_CHK);

        server_logger.info("Checking if file is received");
        response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::FILE_RECV_WAIT) == 0) {
            server_logger.info("Received: " + JasmineGraphInstanceProtocol::FILE_RECV_WAIT);
            server_logger.info("Checking file status : " + to_string(count));
            count++;
            sleep(1);
            continue;
        } else if (response.compare(JasmineGraphInstanceProtocol::FILE_ACK) == 0) {
            server_logger.info("Received: " + JasmineGraphInstanceProtocol::FILE_ACK);
            server_logger.info("File transfer completed for file : " + filePath);
            break;
        }
    }
    // Next we wait till the batch upload completes
    while (true) {
        if (!Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK)) {
            close(sockfd);
            return false;
        }
        server_logger.info("Sent: " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK);

        response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT) == 0) {
            server_logger.info("Received: " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT);
            sleep(1);
            continue;
        } else if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK) == 0) {
            server_logger.info("Received: " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK);
            server_logger.info("Batch upload completed");
            break;
        }
    }
    close(sockfd);
    return true;
}

static bool batchUploadFile(std::string host, int port, int dataPort, int graphID, std::string filePath,
                            std::string masterIP) {
    return batchUploadCommon(host, port, dataPort, graphID, filePath, masterIP,
                             JasmineGraphInstanceProtocol::BATCH_UPLOAD);
}

static bool batchUploadCentralStore(std::string host, int port, int dataPort, int graphID, std::string filePath,
                                    std::string masterIP) {
    return batchUploadCommon(host, port, dataPort, graphID, filePath, masterIP,
                             JasmineGraphInstanceProtocol::BATCH_UPLOAD_CENTRAL);
}

void JasmineGraphServer::copyCentralStoreToAggregateLocation(std::string filePath) {
    std::string result = "SUCCESS";
    std::string aggregatorDirPath = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.aggregatefolder");

    if (access(aggregatorDirPath.c_str(), F_OK)) {
        std::string createDirCommand = "mkdir -p " + aggregatorDirPath;
        if (system(createDirCommand.c_str())) {
            server_logger.error("Creating directory " + aggregatorDirPath + " failed");
        }
    }

    std::string copyCommand = "cp " + filePath + " " + aggregatorDirPath;
    if (system(copyCommand.c_str())) {
        server_logger.error("Copying " + filePath + " into " + aggregatorDirPath + " failed");
    }
}

static bool batchUploadAttributeFile(std::string host, int port, int dataPort, int graphID, std::string filePath,
                                     std::string masterIP) {
    return batchUploadCommon(host, port, dataPort, graphID, filePath, masterIP,
                             JasmineGraphInstanceProtocol::UPLOAD_RDF_ATTRIBUTES);
}

static bool batchUploadCentralAttributeFile(std::string host, int port, int dataPort, int graphID, std::string filePath,
                                            std::string masterIP) {
    return batchUploadCommon(host, port, dataPort, graphID, filePath, masterIP,
                             JasmineGraphInstanceProtocol::UPLOAD_RDF_ATTRIBUTES_CENTRAL);
}

static bool batchUploadCompositeCentralstoreFile(std::string host, int port, int dataPort, int graphID,
                                                 std::string filePath, std::string masterIP) {
    return batchUploadCommon(host, port, dataPort, graphID, filePath, masterIP,
                             JasmineGraphInstanceProtocol::BATCH_UPLOAD_COMPOSITE_CENTRAL);
}

bool JasmineGraphServer::sendFileThroughService(std::string host, int dataPort, std::string fileName,
                                                std::string filePath, std::string masterIP) {
    int sockfd;
    char data[FED_DATA_LENGTH + 1];
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        server_logger.error("Cannot create socket");
        return false;
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        server_logger.error("ERROR, no host named " + host);
        return false;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(dataPort);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        return false;
    }

    if (!Utils::send_str_wrapper(sockfd, fileName)) {
        close(sockfd);
        return false;
    }
    server_logger.info("Sent: " + fileName);

    string response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE) != 0) {
        server_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::SEND_FILE +
                            " ; Received: " + response);
        close(sockfd);
        return false;
    }
    server_logger.info("Received: " + response);

    server_logger.info("Sending file " + filePath + " through port " + std::to_string(dataPort));
    FILE *fp = fopen(filePath.c_str(), "r");
    if (fp == NULL) {
        close(sockfd);
        return false;
    }

    while (true) {
        unsigned char buff[1024] = {0};
        int nread = fread(buff, 1, 1024, fp);

        /* If read was success, send data. */
        if (nread > 0) {
            write(sockfd, buff, nread);
        }

        if (nread < 1024) {
            if (feof(fp)) printf("End of file\n");
            if (ferror(fp)) printf("Error reading\n");
            break;
        }
    }

    fclose(fp);
    close(sockfd);
    return true;
}

static void copyArtifactsToWorkers(const std::string &workerPath, const std::string &artifactLocation,
                                   const std::string &remoteWorker) {
    if (artifactLocation.empty() || artifactLocation.find_first_not_of(' ') == artifactLocation.npos) {
        server_logger.error("Received `" + artifactLocation + "` for `artifactLocation` value!");
        throw std::invalid_argument("Received empty string for `artifactLocation` value!");
    }

    std::string pathCheckCommand = "test -e " + workerPath;
    if (remoteWorker.find("localhost") == std::string::npos) {
        pathCheckCommand = "ssh -p 22 " + remoteWorker + " " + pathCheckCommand;
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
    std::string pathDeletionCommand = "rm -rf " + workerPath;
    if (workerHost.find("localhost") == std::string::npos) {
        pathDeletionCommand = "ssh -p 22 " + workerHost + " " + pathDeletionCommand;
    }

    if (system(pathDeletionCommand.c_str())) {
        server_logger.error("Error executing command: " + pathDeletionCommand);
    }
}

static void createLogFilePath(const std::string &workerHost, const std::string &workerPath) {
    std::string pathCreationCommand = "mkdir -p " + workerPath + "/logs";
    if (workerHost.find("localhost") == std::string::npos) {
        pathCreationCommand = "ssh -p 22 " + workerHost + " " + pathCreationCommand;
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

map<string, string> JasmineGraphServer::getLiveHostIDList() {
    server_logger.info("###MASTER### Loading Live Host ID List");
    map<string, string> hostIDMap;
    std::vector<vector<pair<string, string>>> v =
        this->sqlite->runSelect("SELECT host_idhost,user,ip,server_port FROM worker;");
    string id = v[0][0].second;
    for (int i = 0; i < v.size(); i++) {
        string id = v[i][0].second;
        string user = v[i][1].second;
        string ip = v[i][2].second;
        string serverPort = v[i][3].second;

        string host = "";

        if (user == "") {
            host = ip + ":" + serverPort;
        } else {
            host = user + "@" + ip + ":" + serverPort;
        }

        hostIDMap.insert(make_pair(host, id));
    }

    return hostIDMap;
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
        deleteThreads[count] = std::thread(removePartitionThroughService, j->first, hostPortMap[j->first].first,
                                           graphID, j->second, masterIP);
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
        close(sockfd);
        return false;
    }

    if (!Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::DELETE_GRAPH_FRAGMENT)) {
        close(sockfd);
        return false;
    }
    server_logger.info("Sent: " + JasmineGraphInstanceProtocol::DELETE_GRAPH_FRAGMENT);

    string response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        server_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::OK +
                            " ; Received: " + response);
        close(sockfd);
        return false;
    }
    server_logger.info("Received: " + response);

    if (!Utils::send_str_wrapper(sockfd, graphID)) {
        close(sockfd);
        return false;
    }
    server_logger.info("Sent: " + graphID);

    response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        server_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::OK +
                            " ; Received: " + response);
        close(sockfd);
        return false;
    }
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
        close(sockfd);
        return false;
    }

    if (!Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::DELETE_GRAPH)) {
        close(sockfd);
        return false;
    }
    server_logger.info("Sent: " + JasmineGraphInstanceProtocol::DELETE_GRAPH);

    string response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        server_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::OK +
                            " ; Received: " + response);
        close(sockfd);
        return false;
    }
    server_logger.info("Received: " + response);

    if (!Utils::send_str_wrapper(sockfd, graphID)) {
        close(sockfd);
        return false;
    }
    server_logger.info("Sent: " + graphID);

    response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::SEND_PARTITION_ID) != 0) {
        server_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::SEND_PARTITION_ID +
                            " ; Received: " + response);
        close(sockfd);
        return false;
    }
    server_logger.info("Received: " + response);

    if (!Utils::send_str_wrapper(sockfd, partitionID)) {
        close(sockfd);
        return false;
    }
    server_logger.info("Sent: " + partitionID);

    response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        server_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::OK +
                            " ; Received: " + response);
        close(sockfd);
        return false;
    }
    server_logger.info("Received: " + response);
    close(sockfd);
    return true;
}

std::vector<JasmineGraphServer::workers> JasmineGraphServer::getHostWorkerMap() { return hostWorkerMap; }

void JasmineGraphServer::updateOperationalGraphList() {
    string hosts = "";
    string graphIDs = "";
    std::vector<std::string> hostsList;

    if (profile == Conts::PROFILE_NATIVE) {
        hostsList = Utils::getHostListFromProperties();
    } else if (profile == "docker") {
        hostsList = getWorkerVector(workerHosts);
    }
    vector<string>::iterator it;
    for (it = hostsList.begin(); it < hostsList.end(); it++) {
        string host = *it;
        hosts += ("'" + host + "', ");
    }
    hosts = hosts.substr(0, hosts.size() - 2);
    string sqlStatement =
        ("SELECT b.partition_graph_idgraph FROM worker_has_partition AS b "
         "JOIN worker WHERE worker.idworker = b.worker_idworker AND worker.name IN "
         "(" +
         hosts +
         ") GROUP BY b.partition_graph_idgraph HAVING COUNT(b.partition_idpartition)= "
         "(SELECT COUNT(a.idpartition) FROM partition AS a "
         "WHERE a.graph_idgraph = b.partition_graph_idgraph);");
    std::vector<vector<pair<string, string>>> v = this->sqlite->runSelect(sqlStatement);
    for (std::vector<vector<pair<string, string>>>::iterator i = v.begin(); i != v.end(); ++i) {
        for (std::vector<pair<string, string>>::iterator j = (i->begin()); j != i->end(); ++j) {
            graphIDs += (j->second + ", ");
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

std::map<std::string, JasmineGraphServer::workerPartition> JasmineGraphServer::getWorkerPartitions(string graphID) {
    vector<pair<string, string>> hostHasPartition;
    auto *refToSqlite = new SQLiteDBInterface();
    refToSqlite->init();
    map<std::string, JasmineGraphServer::workerPartition> graphPartitionedHosts;
    vector<vector<pair<string, string>>> hostPartitionResults = refToSqlite->runSelect(
        "SELECT name, worker_idworker, server_port, server_data_port, partition_idpartition FROM worker_has_partition "
        "INNER JOIN worker ON worker_"
        "idworker = idworker WHERE partition_graph_idgraph = '" +
        graphID + "'");

    refToSqlite->finalize();
    delete refToSqlite;

    for (std::vector<vector<pair<string, string>>>::iterator i = hostPartitionResults.begin();
         i != hostPartitionResults.end(); ++i) {
        std::vector<pair<string, string>> rowData = *i;

        string name = rowData.at(0).second;
        string workerID = rowData.at(1).second;
        int serverPort = std::stoi(rowData.at(2).second);
        int serverDataPort = std::stoi(rowData.at(3).second);
        string partitionId = rowData.at(4).second;

        server_logger.info("name : " + name + " workerID : " + workerID + " sport : " + std::to_string(serverPort) +
                           " sdport : " + std::to_string(serverDataPort) + " partitionId : " + partitionId);
        graphPartitionedHosts.insert((pair<string, JasmineGraphServer::workerPartition>(
            workerID, {name, serverPort, serverDataPort, partitionId})));
    }

    return graphPartitionedHosts;
}

std::map<string, JasmineGraphServer::workerPartitions> JasmineGraphServer::getGraphPartitionedHosts(string graphID) {
    vector<pair<string, string>> hostHasPartition;
    auto *refToSqlite = new SQLiteDBInterface();
    refToSqlite->init();
    vector<vector<pair<string, string>>> hostPartitionResults = refToSqlite->runSelect(
        "SELECT name, partition_idpartition FROM worker_has_partition INNER JOIN worker ON worker_"
        "idworker = idworker WHERE partition_graph_idgraph = '" +
        graphID + "'");
    refToSqlite->finalize();
    delete refToSqlite;
    for (vector<vector<pair<string, string>>>::iterator i = hostPartitionResults.begin();
         i != hostPartitionResults.end(); ++i) {
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
    for (std::vector<pair<string, string>>::iterator j = (hostHasPartition.begin()); j != hostHasPartition.end(); ++j) {
        string hostname = j->first;
        if (hostPartitions.count(hostname) > 0) {
            hostPartitions[hostname].push_back(j->second);
        } else {
            vector<string> vec;
            vec.push_back(j->second);
            hostPartitions.insert((pair<string, vector<string>>(hostname, vec)));
        }
    }

    map<string, JasmineGraphServer::workerPartitions> graphPartitionedHosts;
    for (map<string, vector<string>>::iterator it = (hostPartitions.begin()); it != hostPartitions.end(); ++it) {
        graphPartitionedHosts.insert((pair<string, JasmineGraphServer::workerPartitions>(
            it->first, {hostPortMap[it->first].first, hostPortMap[it->first].second, hostPartitions[it->first]})));
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
    std::map<std::string, JasmineGraphServer::workerPartition> graphPartitionedHosts =
        JasmineGraphServer::getWorkerPartitions(graphID);
    int partition_count = 0;
    string partition;
    string host;
    int port;
    std::string workerList;
    std::map<std::string, JasmineGraphServer::workerPartition>::iterator workerit;
    for (workerit = graphPartitionedHosts.begin(); workerit != graphPartitionedHosts.end(); workerit++) {
        JasmineGraphServer::workerPartition workerPartition = workerit->second;
        partition = workerPartition.partitionID;
        host = workerPartition.hostname;
        port = workerPartition.port;

        if (host.find('@') != std::string::npos) {
            host = Utils::split(host, '@')[1];
        }

        workerList.append(host + ":" + std::to_string(port) + ":" + partition + ",");
    }

    workerList.pop_back();

    for (workerit = graphPartitionedHosts.begin(); workerit != graphPartitionedHosts.end(); workerit++) {
        JasmineGraphServer::workerPartition workerPartition = workerit->second;
        partition = workerPartition.partitionID;
        host = workerPartition.hostname;
        port = workerPartition.port;

        if (host.find('@') != std::string::npos) {
            host = Utils::split(host, '@')[1];
        }

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

        server = gethostbyname(host.c_str());
        if (server == NULL) {
            server_logger.error("ERROR, no host named " + host);
            continue;
        }

        bzero((char *)&serv_addr, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
        serv_addr.sin_port = htons(port);
        if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
            continue;
        }

        if (!Utils::send_str_wrapper(sockfd, command)) {
            close(sockfd);
            continue;
        }
        server_logger.info("Sent: " + command);

        string response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
            server_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::OK +
                                " ; Received: " + response);
            close(sockfd);
            continue;
        }
        server_logger.info("Received: " + response);

        if (!Utils::send_str_wrapper(sockfd, graphID)) {
            close(sockfd);
            continue;
        }
        server_logger.info("Sent: " + graphID);

        response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
            server_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::OK +
                                " ; Received: " + response);
            close(sockfd);
            continue;
        }
        server_logger.info("Received: " + response);

        if (!Utils::send_str_wrapper(sockfd, partition)) {
            close(sockfd);
            continue;
        }
        server_logger.info("Sent: " + partition);

        response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
            server_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::OK +
                                " ; Received: " + response);
            close(sockfd);
            continue;
        }
        server_logger.info("Received: " + response);

        if (!Utils::send_str_wrapper(sockfd, workerList)) {
            close(sockfd);
            continue;
        }
        server_logger.info("Sent: " + workerList);
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
    std::map<std::string, JasmineGraphServer::workerPartition> graphPartitionedHosts =
        JasmineGraphServer::getWorkerPartitions(graphID);
    int partition_count = 0;
    string partition;
    string host;
    int port;
    int dport;
    std::string workerList;
    std::map<std::string, JasmineGraphServer::workerPartition>::iterator workerit;
    for (workerit = graphPartitionedHosts.begin(); workerit != graphPartitionedHosts.end(); workerit++) {
        JasmineGraphServer::workerPartition workerPartition = workerit->second;
        partition = workerPartition.partitionID;
        host = workerPartition.hostname;
        port = workerPartition.port;
        dport = workerPartition.dataPort;

        if (host.find('@') != std::string::npos) {
            host = Utils::split(host, '@')[1];
        }

        workerList.append(host + ":" + std::to_string(port) + ":" + partition + ":" + to_string(dport) + ",");
    }
    workerList.pop_back();

    for (workerit = graphPartitionedHosts.begin(); workerit != graphPartitionedHosts.end(); workerit++) {
        JasmineGraphServer::workerPartition workerPartition = workerit->second;
        partition = workerPartition.partitionID;
        host = workerPartition.hostname;
        port = workerPartition.port;

        if (host.find('@') != std::string::npos) {
            host = Utils::split(host, '@')[1];
        }

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
        server = gethostbyname(host.c_str());
        if (server == NULL) {
            server_logger.error("ERROR, no host named " + host);
            continue;
        }

        bzero((char *)&serv_addr, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
        serv_addr.sin_port = htons(port);
        if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
            continue;
        }

        if (!Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::DP_CENTRALSTORE)) {
            close(sockfd);
            continue;
        }
        server_logger.info("Sent: " + JasmineGraphInstanceProtocol::DP_CENTRALSTORE);

        string response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
            server_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::OK +
                                " ; Received: " + response);
            close(sockfd);
            continue;
        }
        server_logger.info("Received: " + response);

        if (!Utils::send_str_wrapper(sockfd, graphID)) {
            close(sockfd);
            continue;
        }
        server_logger.info("Sent: " + graphID);

        response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
            server_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::OK +
                                " ; Received: " + response);
            close(sockfd);
            continue;
        }
        server_logger.info("Received: " + response);

        if (!Utils::send_str_wrapper(sockfd, partition)) {
            close(sockfd);
            continue;
        }
        server_logger.info("Sent: " + partition);

        response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
            server_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::OK +
                                " ; Received: " + response);
            close(sockfd);
            continue;
        }
        server_logger.info("Received: " + response);

        if (!Utils::send_str_wrapper(sockfd, workerList)) {
            close(sockfd);
            continue;
        }
        server_logger.info("Sent: " + workerList);
    }
}

void JasmineGraphServer::initiateFiles(std::string graphID, std::string trainingArgs) {
    int count = 0;
    map<string, map<int, int>> scheduleForAllHosts = JasmineGraphTrainingSchedular::schedulePartitionTraining(graphID);
    std::map<std::string, JasmineGraphServer::workerPartition> graphPartitionedHosts =
        this->getWorkerPartitions(graphID);
    int partition_count = 0;
    std::map<std::string, JasmineGraphServer::workerPartition>::iterator mapIterator;
    for (mapIterator = graphPartitionedHosts.begin(); mapIterator != graphPartitionedHosts.end(); mapIterator++) {
        JasmineGraphServer::workerPartition workerPartition = mapIterator->second;
        std::vector<std::string> partitions;
        partitions.push_back(workerPartition.partitionID);
        std::vector<std::string>::iterator it;
        for (it = partitions.begin(); it < partitions.end(); it++) {
            partition_count++;
        }
    }

    std::thread workerThreads[partition_count + 1];

    string prefix = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.trainedmodelfolder");
    string attr_prefix = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    trainingArgs = trainingArgs;
    std::map<std::string, JasmineGraphServer::workerPartition>::iterator j;
    for (j = graphPartitionedHosts.begin(); j != graphPartitionedHosts.end(); j++) {
        JasmineGraphServer::workerPartition workerPartition = j->second;
        std::vector<std::string> partitions;
        partitions.push_back(workerPartition.partitionID);
        string partitionCount = std::to_string(partitions.size());
        std::vector<std::string>::iterator k;
        map<int, int> scheduleOfHost = scheduleForAllHosts[j->first];
        for (k = partitions.begin(); k != partitions.end(); k++) {
            int iterationOfPart = scheduleOfHost[stoi(*k)];
            workerThreads[count] =
                std::thread(initiateTrain, workerPartition.hostname, workerPartition.port, workerPartition.dataPort,
                            trainingArgs + " " + *k, iterationOfPart, partitionCount, this->masterHost);
            count++;
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

    trainingArgs = trainingArgs;
    int threadID = 0;

    for (int i = 0; i < workerVector.size(); i++) {
        workerInstance = workerVector[i];

        int serverPort = stoi(workerInstance.port);
        int serverDataPort = stoi(workerInstance.dataPort);

        if (i == 0) {
            workerThreads[threadID] = std::thread(initiateServer, workerInstance.hostname, serverPort, serverDataPort,
                                                  trainingArgs, fl_clients, to_string(i), masterIP);
            threadID++;
        }

        workerThreads[threadID] = std::thread(initiateClient, workerInstance.hostname, serverPort, serverDataPort,
                                              trainingArgs + " " + to_string(i), fl_clients, to_string(i), masterIP);
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
        initiateAggregator("localhost", stoi(workerVector[0].port), stoi(workerVector[0].dataPort), trainingArgs,
                           fl_clients, "1", masterIP);
    }

    std::ifstream file(Utils::getJasmineGraphProperty("org.jasminegraph.fl.organization.file"));

    if (file.good()) {
        if (file.is_open()) {
            std::string line;
            std::thread *trainThreads = new std::thread[orgs_count];
            while (std::getline(file, line)) {
                line = Utils::trim_copy(line, " \f\n\r\t\v");
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
    trainingArgs = trainingArgs;
    int threadID = 0;

    for (int i = 0; i < workerVector.size(); i++) {
        workerInstance = workerVector[i];
        int serverPort = stoi(workerInstance.port);
        int serverDataPort = stoi(workerInstance.dataPort);

        if (i == 0) {
            workerThreads[threadID] = std::thread(initiateOrgServer, workerInstance.hostname, serverPort,
                                                  serverDataPort, trainingArgs, fl_clients, to_string(i), masterIP);
            threadID++;
        }

        workerThreads[threadID] = std::thread(initiateClient, workerInstance.hostname, serverPort, serverDataPort,
                                              trainingArgs + " " + to_string(i), fl_clients, to_string(i), masterIP);
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
    int count = 0;
    map<string, map<int, int>> scheduleForAllHosts = JasmineGraphTrainingSchedular::schedulePartitionTraining(graphID);
    std::map<std::string, JasmineGraphServer::workerPartition> graphPartitionedHosts =
        this->getWorkerPartitions(graphID);
    int partition_count = 0;
    std::map<std::string, JasmineGraphServer::workerPartition>::iterator mapIterator;
    for (mapIterator = graphPartitionedHosts.begin(); mapIterator != graphPartitionedHosts.end(); mapIterator++) {
        JasmineGraphServer::workerPartition workerPartition = mapIterator->second;
        std::vector<std::string> partitions;
        partitions.push_back(workerPartition.partitionID);
        std::vector<std::string>::iterator it;
        for (it = partitions.begin(); it < partitions.end(); it++) {
            partition_count++;
        }
    }

    std::thread *workerThreads = new std::thread[partition_count + 1];

    trainingArgs = trainingArgs;
    int fl_clients = stoi(Utils::getJasmineGraphProperty("org.jasminegraph.fl_clients"));
    std::map<std::string, JasmineGraphServer::workerPartition>::iterator j;
    for (j = graphPartitionedHosts.begin(); j != graphPartitionedHosts.end(); j++) {
        JasmineGraphServer::workerPartition workerPartition = j->second;
        std::vector<std::string> partitions;
        partitions.push_back(workerPartition.partitionID);
        string partitionCount = std::to_string(partitions.size());
        std::vector<std::string>::iterator k;
        map<int, int> scheduleOfHost = scheduleForAllHosts[j->first];
        for (k = partitions.begin(); k != partitions.end(); k++) {
            int iterationOfPart = scheduleOfHost[stoi(*k)];
            workerThreads[count] =
                std::thread(mergeFiles, workerPartition.hostname, workerPartition.port, workerPartition.dataPort,
                            trainingArgs + " " + *k, fl_clients, *k, this->masterHost);
            count++;
        }
    }

    for (int threadCount = 0; threadCount < count; threadCount++) {
        workerThreads[threadCount].join();
    }
    server_logger.info("Merge Commands Sent");
    delete[] workerThreads;
}

static bool initiateCommon(std::string host, int port, int dataPort, std::string trainingArgs, int iteration,
                           std::string partCount, std::string masterIP, std::string initType) {
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
        close(sockfd);
        return false;
    }

    if (!Utils::send_str_wrapper(sockfd, initType)) {
        close(sockfd);
        return false;
    }
    server_logger.info("Sent: " + initType);

    string response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        server_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::OK +
                            " ; Received: " + response);
        close(sockfd);
        return false;
    }
    server_logger.info("Received: " + response);

    if (!Utils::send_str_wrapper(sockfd, trainingArgs)) {
        close(sockfd);
        return false;
    }
    server_logger.info("Sent: " + trainingArgs);

    close(sockfd);
    return true;
}

static bool initiateTrain(std::string host, int port, int dataPort, std::string trainingArgs, int iteration,
                          std::string partCount, std::string masterIP) {
    return initiateCommon(host, port, dataPort, trainingArgs, iteration, partCount, masterIP,
                          JasmineGraphInstanceProtocol::INITIATE_FED_PREDICT);
}

static bool initiatePredict(std::string host, int port, int dataPort, std::string trainingArgs, int iteration,
                            std::string partCount, std::string masterIP) {
    return initiateCommon(host, port, dataPort, trainingArgs, iteration, partCount, masterIP,
                          JasmineGraphInstanceProtocol::INITIATE_FILES);
}

static bool initiateServer(std::string host, int port, int dataPort, std::string trainingArgs, int iteration,
                           std::string partCount, std::string masterIP) {
    return initiateCommon(host, port, dataPort, trainingArgs, iteration, partCount, masterIP,
                          JasmineGraphInstanceProtocol::INITIATE_SERVER);
}

// todo Remove partCount from parameters as the partition id is being parsed within the training args
static bool initiateClient(std::string host, int port, int dataPort, std::string trainingArgs, int iteration,
                           std::string partCount, std::string masterIP) {
    return initiateCommon(host, port, dataPort, trainingArgs, iteration, partCount, masterIP,
                          JasmineGraphInstanceProtocol::INITIATE_CLIENT);
}

static bool initiateAggregator(std::string host, int port, int dataPort, std::string trainingArgs, int iteration,
                               std::string partCount, std::string masterIP) {
    return initiateCommon(host, port, dataPort, trainingArgs, iteration, partCount, masterIP,
                          JasmineGraphInstanceProtocol::INITIATE_AGG);
}

static bool initiateOrgServer(std::string host, int port, int dataPort, std::string trainingArgs, int iteration,
                              std::string partCount, std::string masterIP) {
    return initiateCommon(host, port, dataPort, trainingArgs, iteration, partCount, masterIP,
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

    while (true) {
        if (!(Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)) {
            break;
        }
    }

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
    close(sockfd);
    return true;
}

bool JasmineGraphServer::mergeFiles(std::string host, int port, int dataPort, std::string trainingArgs, int iteration,
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
        close(sockfd);
        return false;
    }

    if (!Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::MERGE_FILES)) {
        close(sockfd);
        return false;
    }
    server_logger.info("Sent: " + JasmineGraphInstanceProtocol::MERGE_FILES);

    string response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        server_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::OK +
                            " ; Received: " + response);
        close(sockfd);
        return false;
    }
    server_logger.info("Received: " + response);

    if (!Utils::send_str_wrapper(sockfd, trainingArgs)) {
        close(sockfd);
        return false;
    }
    server_logger.info("Sent: " + trainingArgs);

    close(sockfd);
    return true;
}

bool JasmineGraphServer::sendTrainCommand(std::string host, int port, std::string trainingArgs) {
    bool result = true;
    int sockfd;
    char data[FED_DATA_LENGTH];
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

    if (!Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::INITIATE_TRAIN)) {
        close(sockfd);
        return false;
    }
    server_logger.info("Sent: " + JasmineGraphInstanceProtocol::INITIATE_TRAIN);

    string response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        server_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::OK +
                            " ; Received: " + response);
        close(sockfd);
        return false;
    }
    server_logger.info("Received: " + response);

    if (!Utils::send_str_wrapper(sockfd, trainingArgs)) {
        close(sockfd);
        return false;
    }
    server_logger.info("Sent: " + trainingArgs);

    close(sockfd);
    return true;
}

void JasmineGraphServer::egoNet(std::string graphID) {
    std::map<std::string, JasmineGraphServer::workerPartition> graphPartitionedHosts =
        JasmineGraphServer::getWorkerPartitions(graphID);
    int partition_count = 0;
    string partition;
    string host;
    int port;
    int dataPort;
    std::string workerList;

    std::map<std::string, JasmineGraphServer::workerPartition>::iterator workerit;
    for (workerit = graphPartitionedHosts.begin(); workerit != graphPartitionedHosts.end(); workerit++) {
        JasmineGraphServer::workerPartition workerPartition = workerit->second;
        partition = workerPartition.partitionID;
        host = workerPartition.hostname;
        port = workerPartition.port;
        dataPort = workerPartition.dataPort;

        if (host.find('@') != std::string::npos) {
            host = Utils::split(host, '@')[1];
        }

        workerList.append(host + ":" + std::to_string(port) + ":" + partition + ",");
    }

    workerList.pop_back();

    int sockfd;
    char data[301];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

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

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        return;
    }

    if (!Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::EGONET)) {
        close(sockfd);
        return;
    }
    server_logger.info("Sent: " + JasmineGraphInstanceProtocol::EGONET);

    string response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        server_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::OK +
                            " ; Received: " + response);
        close(sockfd);
        return;
    }
    server_logger.info("Received: " + response);

    if (!Utils::send_str_wrapper(sockfd, graphID)) {
        close(sockfd);
        return;
    }
    server_logger.info("Sent: " + graphID);

    response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        server_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::OK +
                            " ; Received: " + response);
        close(sockfd);
        return;
    }
    server_logger.info("Received: " + response);

    if (!Utils::send_str_wrapper(sockfd, partition)) {
        close(sockfd);
        return;
    }
    server_logger.info("Sent: " + partition);

    response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        server_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::OK +
                            " ; Received: " + response);
        close(sockfd);
        return;
    }
    server_logger.info("Received: " + response);

    if (!Utils::send_str_wrapper(sockfd, workerList)) {
        close(sockfd);
        return;
    }
    server_logger.info("Sent: " + workerList);

    response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        server_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::OK +
                            " ; Received: " + response);
        close(sockfd);
        return;
    }
    server_logger.info("Received: " + response);

    close(sockfd);
}

void JasmineGraphServer::pageRank(std::string graphID, double alpha, int iterations) {
    std::map<std::string, JasmineGraphServer::workerPartition> graphPartitionedHosts =
        JasmineGraphServer::getWorkerPartitions(graphID);
    int partition_count = 0;
    string partition;
    string host;
    int port;
    int dataPort;
    std::string workerList;

    std::map<std::string, JasmineGraphServer::workerPartition>::iterator workerIter;
    for (workerIter = graphPartitionedHosts.begin(); workerIter != graphPartitionedHosts.end(); workerIter++) {
        JasmineGraphServer::workerPartition workerPartition = workerIter->second;
        partition = workerPartition.partitionID;
        host = workerPartition.hostname;
        port = workerPartition.port;
        dataPort = workerPartition.dataPort;

        if (host.find('@') != std::string::npos) {
            host = Utils::split(host, '@')[1];
        }

        workerList.append(host + ":" + std::to_string(port) + ":" + partition + ",");
    }

    workerList.pop_back();
    server_logger.info("Worker list " + workerList);

    for (workerIter = graphPartitionedHosts.begin(); workerIter != graphPartitionedHosts.end(); workerIter++) {
        JasmineGraphServer::workerPartition workerPartition = workerIter->second;
        partition = workerPartition.partitionID;
        host = workerPartition.hostname;
        port = workerPartition.port;
        dataPort = workerPartition.dataPort;

        if (host.find('@') != std::string::npos) {
            host = Utils::split(host, '@')[1];
        }

        int sockfd;
        char data[301];
        bool loop = false;
        socklen_t len;
        struct sockaddr_in serv_addr;
        struct hostent *server;

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

        bzero((char *)&serv_addr, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
        serv_addr.sin_port = htons(port);
        if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
            continue;
        }

        if (!Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::PAGE_RANK)) {
            close(sockfd);
            continue;
        }
        server_logger.info("Sent: " + JasmineGraphInstanceProtocol::PAGE_RANK);

        string response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
            server_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::OK +
                                " ; Received: " + response);
            close(sockfd);
            continue;
        }

        if (!Utils::send_str_wrapper(sockfd, graphID)) {
            close(sockfd);
            continue;
        }
        server_logger.info("Sent: " + graphID);

        response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
            server_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::OK +
                                " ; Received: " + response);
            close(sockfd);
            continue;
        }

        if (!Utils::send_str_wrapper(sockfd, partition)) {
            close(sockfd);
            continue;
        }
        server_logger.info("Sent: " + partition);

        response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
            server_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::OK +
                                " ; Received: " + response);
            close(sockfd);
            continue;
        }

        if (!Utils::send_str_wrapper(sockfd, workerList)) {
            close(sockfd);
            continue;
        }
        server_logger.info("Sent: " + workerList);

        response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
            server_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::OK +
                                " ; Received: " + response);
            close(sockfd);
            continue;
        }

        long graphVertexCount = JasmineGraphServer::getGraphVertexCount(graphID);
        if (!Utils::send_str_wrapper(sockfd, to_string(graphVertexCount))) {
            close(sockfd);
            continue;
        }
        server_logger.info("Sent: " + to_string(graphVertexCount));

        response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
            server_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::OK +
                                " ; Received: " + response);
            close(sockfd);
            continue;
        }

        if (!Utils::send_str_wrapper(sockfd, to_string(alpha))) {
            close(sockfd);
            continue;
        }
        server_logger.info("Sent: " + to_string(alpha));

        response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
            server_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::OK +
                                " ; Received: " + response);
            close(sockfd);
            continue;
        }

        if (!Utils::send_str_wrapper(sockfd, to_string(iterations))) {
            close(sockfd);
            continue;
        }
        server_logger.info("Sent: " + to_string(iterations));

        response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
            server_logger.error("Incorrect response. Expected: " + JasmineGraphInstanceProtocol::OK +
                                " ; Received: " + response);
            close(sockfd);
            continue;
        }
        close(sockfd);
    }
}
