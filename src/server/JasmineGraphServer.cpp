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
static void updateMetaDB(int graphID, std::string uploadEndTime);

static map<string, string> hostIDMap;
static std::vector<JasmineGraphServer::workers> hostWorkerMap;
static map<string, pair<int, int>> hostPortMap;
std::map<int, int> aggregateWeightMap;

void *runfrontend(void *dummyPt) {
    JasmineGraphServer *refToServer = (JasmineGraphServer *)dummyPt;
    refToServer->frontend = new JasmineGraphFrontEnd(*(refToServer->sqlite), *(refToServer->performanceSqlite),
                                                     refToServer->masterHost, refToServer->jobScheduler);
    refToServer->frontend->run();
    return NULL;
}

void *runbackend(void *dummyPt) {
    JasmineGraphServer *refToServer = (JasmineGraphServer *)dummyPt;
    refToServer->backend = new JasmineGraphBackend(*(refToServer->sqlite), refToServer->numberOfWorkers);
    refToServer->backend->run();
    return NULL;
}

JasmineGraphServer *JasmineGraphServer::getInstance() {
    static JasmineGraphServer *instance = nullptr;
    // TODO(thevindu-w): synchronize
    if (instance == nullptr) {
        instance = new JasmineGraphServer();
    }
    return instance;
}

JasmineGraphServer::JasmineGraphServer() {}

JasmineGraphServer::~JasmineGraphServer() {
    puts("Freeing up server resources.");
    sqlite->finalize();
}

int JasmineGraphServer::run(std::string profile, std::string masterIp, int numberofWorkers, std::string workerIps,
                            std::string enableNmon) {
    server_logger.log("Running the server...", "info");
    std::vector<int> masterPortVector;

    this->sqlite = new SQLiteDBInterface();
    if (this->sqlite->init()) {
        server_logger.error("Error initializing SQLiteDBInterface");
        delete this->sqlite;
        return 1;
    }
    this->performanceSqlite = new PerformanceSQLiteDBInterface();
    if (this->performanceSqlite->init()) {
        server_logger.error("Error initializing PerformanceSQLiteDBInterface");
        delete this->performanceSqlite;
        delete this->sqlite;
        return 1;
    }
    this->jobScheduler = *new JobScheduler(*(this->sqlite), *(this->performanceSqlite));
    this->jobScheduler.init();
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
    addInstanceDetailsToPerformanceDB(masterHost, masterPortVector, "true");
    updateOperationalGraphList();
    start_workers();
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
    if (profile == "native") {
        hostsList = Utils::getHostListFromProperties();
        if ((this->numberOfWorkers) == -1) {
            nWorkers = Utils::getJasmineGraphProperty("org.jasminegraph.server.nworkers");
        }
        enableNmon = Utils::getJasmineGraphProperty("org.jasminegraph.server.enable.nmon");
    } else if (profile == "docker") {
        hostsList = getWorkerVector(workerHosts);
    }

    if (hostsList.size() == 0) {
        server_logger.log("At least one host needs to be specified", "error");
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
        server_logger.log("Number of Workers is not specified", "error");
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
        std::string hostID = Utils::getHostID(ip, *(this->sqlite));
        std::vector<int> portVector = workerPortsMap[hostName];
        std::vector<int> dataPortVector = workerDataPortsMap[hostName];

        while (portCount < numberOfWorkersPerHost) {
            portVector.push_back(workerPort);
            dataPortVector.push_back(workerDataPort);
            hostWorkerMap.push_back({*it, workerPort, workerDataPort});
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

    Utils::assignPartitionsToWorkers(numberOfWorkers, *(this->sqlite));

    int hostListSize = hostsList.size();
    std::vector<std::string>::iterator hostListIterator;
    hostListIterator = hostsList.begin();

    std::thread *myThreads = new std::thread[hostListSize];
    int count = 0;
    server_logger.log("Starting threads for workers", "info");
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
        std::cout << "############JOINED###########" << std::endl;
    }
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
    server_logger.log("###MASTER#### Starting remote workers for profile " + profile, "info");
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

    if (profile == "native") {
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
    } else if (profile == "docker") {
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
            server_logger.log(serverStartScript, "info");
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
    SQLiteDBInterface refToSqlite = *new SQLiteDBInterface();
    refToSqlite.init();
    string selectHostSQL = "SELECT idhost from host where name='" + host + "'";
    string selectWorkerSQL =
        "SELECT idworker from worker where server_port = '" + port + "' or server_data_port = '" + dataPort + "'";
    std::vector<vector<pair<string, string>>> checkWorkerOutput = refToSqlite.runSelect(selectWorkerSQL);

    if (checkWorkerOutput.size() > 0) {
        return false;
    }

    std::vector<vector<pair<string, string>>> selectHostOutput = refToSqlite.runSelect(selectHostSQL);
    string idHost = "";

    if (selectHostOutput.size() > 0) {
        idHost = selectHostOutput[0][0].second;
    } else {
        string maxHostIDSQL = "select max(idhost) from host";
        std::vector<vector<pair<string, string>>> selectMaxHostOutput = refToSqlite.runSelect(maxHostIDSQL);
        idHost = selectMaxHostOutput[0][0].second;

        int hostId = atoi(idHost.c_str());
        hostId++;
        std::string hostInsertString = "INSERT INTO host (idhost,name,ip,is_public) VALUES ('" +
                                       std::to_string(hostId) + "','" + host + "','" + host + "','false')";

        refToSqlite.runInsert(hostInsertString);

        idHost = to_string(hostId);
    }

    string maxWorkerIDSQL = "select max(idworker) from worker";
    std::vector<vector<pair<string, string>>> selectMaxWorkerIdOutput = refToSqlite.runSelect(maxWorkerIDSQL);
    string maxIdWorker = selectMaxWorkerIdOutput[0][0].second;
    int maxWorkerId = atoi(maxIdWorker.c_str());
    maxWorkerId++;
    string workerInsertSqlStatement =
        "INSERT INTO worker (idworker,host_idhost,name,ip,user,is_public,server_port,server_data_port) VALUES ('" +
        to_string(maxWorkerId) + "','" + idHost + "','" + host + "','" + host + "','','false','" + port + "','" +
        dataPort + "')";

    refToSqlite.runInsert(workerInsertSqlStatement);

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
            std::cerr << "Cannot create socket" << std::endl;
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
            std::cerr << "ERROR connecting" << std::endl;
            // TODO::exit
            continue;
        }

        bzero(data, FED_DATA_LENGTH + 1);
        int result_wr = write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(),
                              JasmineGraphInstanceProtocol::HANDSHAKE.size());

        if (result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
        bzero(data, FED_DATA_LENGTH + 1);
        read(sockfd, data, FED_DATA_LENGTH);
        string response = (data);

        response = Utils::trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
            string server_host = Utils::getJasmineGraphProperty("org.jasminegraph.server.host");
            result_wr = write(sockfd, server_host.c_str(), server_host.size());

            if (result_wr < 0) {
                server_logger.log("Error writing to socket", "error");
            }

            server_logger.log("Sent : " + server_host, "info");

            result_wr = write(sockfd, JasmineGraphInstanceProtocol::INITIATE_FRAGMENT_RESOLUTION.c_str(),
                              JasmineGraphInstanceProtocol::INITIATE_FRAGMENT_RESOLUTION.size());

            if (result_wr < 0) {
                server_logger.log("Error writing to socket", "error");
            }

            server_logger.log("Sent : " + JasmineGraphInstanceProtocol::INITIATE_FRAGMENT_RESOLUTION, "info");
            bzero(data, FED_DATA_LENGTH + 1);
            read(sockfd, data, FED_DATA_LENGTH);
            response = (data);
            response = Utils::trim_copy(response, " \f\n\r\t\v");
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");

            if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
                std::vector<string> partitionList = partitionAggregatedMap[workerID];

                for (std::vector<string>::iterator x = partitionList.begin(); x != partitionList.end(); ++x) {
                    string partitionsList = x->c_str();
                    result_wr = write(sockfd, partitionsList.c_str(), partitionsList.size());

                    if (result_wr < 0) {
                        server_logger.log("Error writing to socket", "error");
                    }

                    server_logger.log("Sent : " + partitionsList, "info");
                    bzero(data, FED_DATA_LENGTH + 1);
                    read(sockfd, data, FED_DATA_LENGTH);
                    response = (data);
                    response = Utils::trim_copy(response, " \f\n\r\t\v");
                    server_logger.log("Received : " + response, "info");

                    if (response.compare(JasmineGraphInstanceProtocol::FRAGMENT_RESOLUTION_CHK) == 0) {
                        continue;
                    } else {
                        server_logger.log("Error in fragment resolution process. Received : " + response, "error");
                    }
                }

                if (response.compare(JasmineGraphInstanceProtocol::FRAGMENT_RESOLUTION_CHK) == 0) {
                    result_wr = write(sockfd, JasmineGraphInstanceProtocol::FRAGMENT_RESOLUTION_DONE.c_str(),
                                      JasmineGraphInstanceProtocol::FRAGMENT_RESOLUTION_DONE.size());

                    if (result_wr < 0) {
                        server_logger.log("Error writing to socket", "error");
                    }

                    server_logger.log("Sent : " + JasmineGraphInstanceProtocol::FRAGMENT_RESOLUTION_DONE, "info");

                    bzero(data, FED_DATA_LENGTH + 1);
                    read(sockfd, data, FED_DATA_LENGTH);
                    response = (data);
                    response = Utils::trim_copy(response, " \f\n\r\t\v");
                    server_logger.log("Received : " + response, "info");

                    if (response.compare("") != 0) {
                        std::vector<string> listOfOitems = Utils::split(response, ',');
                        for (std::vector<string>::iterator it = listOfOitems.begin(); it != listOfOitems.end(); it++) {
                            graphIDsFromWorkersSet.insert(atoi(it->c_str()));
                        }
                    }
                }
            }
        }
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
            std::cout << "could not find " << *itr << " from metadb" << std::endl;
            deleteNonOperationalGraphFragment(*itr);
        }
    }
}

/** Method used in master node to commence deletion of a graph fragment
 *
 * @param graphID ID of graph fragments to be deleted
 */
void JasmineGraphServer::deleteNonOperationalGraphFragment(int graphID) {
    std::cout << "Deleting the graph fragments" << std::endl;
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
        server_logger.log("Deleted graph fragments of graph ID " + to_string(graphID), "info");
    }

    for (int threadCount = 0; threadCount < count; threadCount++) {
        if (deleteThreads[threadCount].joinable()) {
            deleteThreads[threadCount].join();
        }
        std::cout << "Thread [A]: " << threadCount << " joined" << std::endl;
    }
}

void JasmineGraphServer::shutdown_workers() {
    std::cout << "Shutting the workers down" << std::endl;
    std::vector<workers, std::allocator<workers>>::iterator mapIterator;
    for (mapIterator = hostWorkerMap.begin(); mapIterator < hostWorkerMap.end(); mapIterator++) {
        workers worker = *mapIterator;
        bool result = true;
        std::cout << pthread_self() << " host : " << worker.hostname << " port : " << worker.port
                  << " DPort : " << worker.dataPort << std::endl;
        int sockfd;
        char data[FED_DATA_LENGTH + 1];
        bool loop = false;
        socklen_t len;
        struct sockaddr_in serv_addr;
        struct hostent *server;

        sockfd = socket(AF_INET, SOCK_STREAM, 0);

        if (sockfd < 0) {
            std::cerr << "Cannot create socket" << std::endl;
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
            server_logger.error("ERROR connecting");
            // TODO::exit
            return;
        }

        bzero(data, FED_DATA_LENGTH + 1);
        write(sockfd, JasmineGraphInstanceProtocol::SHUTDOWN.c_str(), JasmineGraphInstanceProtocol::SHUTDOWN.size());
        server_logger.log("Sent : " + JasmineGraphInstanceProtocol::SHUTDOWN, "info");
        bzero(data, FED_DATA_LENGTH + 1);
        read(sockfd, data, FED_DATA_LENGTH);
        string response = (data);

        response = Utils::trim_copy(response, " \f\n\r\t\v");
        server_logger.log("Response : " + response, "info");
    }
}

void JasmineGraphServer::uploadGraphLocally(int graphID, const string graphType,
                                            vector<std::map<int, string>> fullFileList, std::string masterIP) {
    server_logger.log("Uploading the graph locally..", "info");
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
            sleep(1);
            copyCentralStoreToAggregateLocation(centralStoreFileList[file_count]);
            workerThreads[count] = std::thread(batchUploadCentralStore, worker.hostname, worker.port, worker.dataPort,
                                               graphID, centralStoreFileList[file_count], masterHost);
            count++;
            sleep(1);

            if (compositeCentralStoreFileList.find(file_count) != compositeCentralStoreFileList.end()) {
                copyCentralStoreToAggregateLocation(compositeCentralStoreFileList[file_count]);
                workerThreads[count] =
                    std::thread(batchUploadCompositeCentralstoreFile, worker.hostname, worker.port, worker.dataPort,
                                graphID, compositeCentralStoreFileList[file_count], masterHost);
                count++;
                sleep(1);
            }

            workerThreads[count] = std::thread(batchUploadCentralStore, worker.hostname, worker.port, worker.dataPort,
                                               graphID, centralStoreDuplFileList[file_count], masterHost);
            count++;
            sleep(1);
            if (graphType == Conts::GRAPH_WITH_ATTRIBUTES) {
                workerThreads[count] = std::thread(batchUploadAttributeFile, worker.hostname, worker.port,
                                                   worker.dataPort, graphID, attributeFileList[file_count], masterHost);
                count++;
                sleep(1);
                workerThreads[count] =
                    std::thread(batchUploadCentralAttributeFile, worker.hostname, worker.port, worker.dataPort, graphID,
                                centralStoreAttributeFileList[file_count], masterHost);
                count++;
                sleep(1);
            }
            assignPartitionToWorker(partitionFileName, graphID, worker.hostname, worker.port, worker.dataPort);
            file_count++;
        }
    }

    std::cout << "Total number of threads to join : " << count << std::endl;
    for (int threadCount = 0; threadCount < count; threadCount++) {
        workerThreads[threadCount].join();
        std::cout << "Thread [B]: " << threadCount << " joined" << std::endl;
    }

    std::time_t time = chrono::system_clock::to_time_t(chrono::system_clock::now());
    string uploadEndTime = ctime(&time);

    // The following function updates the 'worker_has_partition' table and 'graph' table only
    updateMetaDB(graphID, uploadEndTime);
    server_logger.info("Upload Graph Locally done");
}

void JasmineGraphServer::assignPartitionToWorker(std::string fileName, int graphId, std::string workerHost,
                                                 int workerPort, int workerDataPort) {
    SQLiteDBInterface refToSqlite = *new SQLiteDBInterface();
    refToSqlite.init();
    size_t lastindex = fileName.find_last_of(".");
    string rawname = fileName.substr(0, lastindex);
    string partitionID = rawname.substr(rawname.find_last_of("_") + 1);

    if (workerHost.find('@') != std::string::npos) {
        workerHost = Utils::split(workerHost, '@')[1];
    }

    std::string workerSearchQuery = "select idworker from worker where ip='" + workerHost + "' and server_port='" +
                                    std::to_string(workerPort) + "' and server_data_port='" +
                                    std::to_string(workerDataPort) + "'";

    std::vector<vector<pair<string, string>>> results = refToSqlite.runSelect(workerSearchQuery);

    std::string workerID = results[0][0].second;

    std::string partitionToWorkerQuery =
        "insert into worker_has_partition (partition_idpartition, partition_graph_idgraph, worker_idworker) values "
        "('" +
        partitionID + "','" + std::to_string(graphId) + "','" + workerID + "')";

    refToSqlite.runInsert(partitionToWorkerQuery);
}

bool JasmineGraphServer::batchUploadFile(std::string host, int port, int dataPort, int graphID, std::string filePath,
                                         std::string masterIP) {
    bool result = true;
    std::cout << pthread_self() << " host : " << host << " port : " << port << " DPort : " << dataPort << std::endl;
    int sockfd;
    char data[FED_DATA_LENGTH + 1];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot create socket" << std::endl;
        return 0;
    }

    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        server_logger.error("ERROR, no host named " + host);
        return 0;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
        // TODO::exit
        return 0;
    }

    bzero(data, FED_DATA_LENGTH + 1);
    int result_wr =
        write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if (result_wr < 0) {
        server_logger.log("Error writing to socket", "error");
    }

    server_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, FED_DATA_LENGTH + 1);
    read(sockfd, data, FED_DATA_LENGTH);
    string response = (data);

    response = Utils::trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        server_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if (result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + masterIP, "info");
        bzero(data, FED_DATA_LENGTH + 1);
        read(sockfd, data, FED_DATA_LENGTH);
        response = (data);

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            server_logger.log("Received : " + response, "error");
        }

        result_wr = write(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD.c_str(),
                          JasmineGraphInstanceProtocol::BATCH_UPLOAD.size());

        if (result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD, "info");
        bzero(data, FED_DATA_LENGTH + 1);
        read(sockfd, data, FED_DATA_LENGTH);
        response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            // std::cout << graphID << std::endl;
            result_wr = write(sockfd, std::to_string(graphID).c_str(), std::to_string(graphID).size());

            if (result_wr < 0) {
                server_logger.log("Error writing to socket", "error");
            }

            server_logger.log("Sent : Graph ID " + std::to_string(graphID), "info");
            std::string fileName = Utils::getFileName(filePath);
            int fileSize = Utils::getFileSize(filePath);
            std::string fileLength = to_string(fileSize);

            bzero(data, FED_DATA_LENGTH + 1);
            read(sockfd, data, FED_DATA_LENGTH);
            response = (data);
            response = Utils::trim_copy(response, " \f\n\r\t\v");

            if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_NAME) == 0) {
                server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");
                result_wr = write(sockfd, fileName.c_str(), fileName.size());

                if (result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : File name " + fileName, "info");
                bzero(data, FED_DATA_LENGTH + 1);
                read(sockfd, data, FED_DATA_LENGTH);
                response = (data);

                if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_LEN) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
                    result_wr = write(sockfd, fileLength.c_str(), fileLength.size());

                    if (result_wr < 0) {
                        server_logger.log("Error writing to socket", "error");
                    }

                    server_logger.log("Sent : File length in bytes " + fileLength, "info");
                    bzero(data, FED_DATA_LENGTH + 1);
                    read(sockfd, data, FED_DATA_LENGTH);
                    response = (data);

                    if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_CONT) == 0) {
                        server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT, "info");
                        server_logger.log("Going to send file through service", "info");
                        sendFileThroughService(host, dataPort, fileName, filePath, masterIP);
                    }
                }
            }
            int count = 0;

            while (true) {
                result_wr = write(sockfd, JasmineGraphInstanceProtocol::FILE_RECV_CHK.c_str(),
                                  JasmineGraphInstanceProtocol::FILE_RECV_CHK.size());

                if (result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : " + JasmineGraphInstanceProtocol::FILE_RECV_CHK, "info");
                server_logger.log("Checking if file is received", "info");
                bzero(data, FED_DATA_LENGTH + 1);
                read(sockfd, data, FED_DATA_LENGTH);
                response = (data);
                if (response.compare(JasmineGraphInstanceProtocol::FILE_RECV_WAIT) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_RECV_WAIT, "info");
                    server_logger.log("Checking file status : " + to_string(count), "info");
                    count++;
                    sleep(1);
                    continue;
                } else if (response.compare(JasmineGraphInstanceProtocol::FILE_ACK) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_ACK, "info");
                    server_logger.log("File transfer completed for file : " + filePath, "info");
                    break;
                }
            };
            // Next we wait till the batch upload completes
            while (true) {
                result_wr = write(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.c_str(),
                                  JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.size());

                if (result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                bzero(data, FED_DATA_LENGTH + 1);
                read(sockfd, data, FED_DATA_LENGTH);
                response = (data);

                if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT, "info");
                    sleep(1);
                    continue;
                } else if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK, "info");
                    server_logger.log("Batch upload completed", "info");
                    break;
                }
            }
        }
    } else {
        server_logger.log("There was an error in the upload process and the response is :: " + response, "error");
    }

    close(sockfd);
    return 0;
}

bool JasmineGraphServer::batchUploadCentralStore(std::string host, int port, int dataPort, int graphID,
                                                 std::string filePath, std::string masterIP) {
    int sockfd;
    char data[FED_DATA_LENGTH + 1];
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot create socket" << std::endl;
        return 0;
    }

    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        server_logger.error("ERROR, no host named " + host);
        return 0;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
        return 0;
    }

    bzero(data, FED_DATA_LENGTH + 1);
    int result_wr =
        write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if (result_wr < 0) {
        server_logger.log("Error writing to socket", "error");
    }

    server_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, FED_DATA_LENGTH + 1);
    read(sockfd, data, FED_DATA_LENGTH);
    string response = (data);

    response = Utils::trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        server_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if (result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + masterIP, "info");
        bzero(data, FED_DATA_LENGTH + 1);
        read(sockfd, data, FED_DATA_LENGTH);
        response = (data);

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            server_logger.log("Received : " + response, "error");
        }

        result_wr = write(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_CENTRAL.c_str(),
                          JasmineGraphInstanceProtocol::BATCH_UPLOAD_CENTRAL.size());

        if (result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CENTRAL, "info");
        bzero(data, FED_DATA_LENGTH + 1);
        read(sockfd, data, FED_DATA_LENGTH);
        response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");

            result_wr = write(sockfd, std::to_string(graphID).c_str(), std::to_string(graphID).size());

            if (result_wr < 0) {
                server_logger.log("Error writing to socket", "error");
            }

            server_logger.log("Sent : Graph ID " + std::to_string(graphID), "info");
            std::string fileName = Utils::getFileName(filePath);
            int fileSize = Utils::getFileSize(filePath);
            std::string fileLength = to_string(fileSize);

            bzero(data, FED_DATA_LENGTH + 1);
            read(sockfd, data, FED_DATA_LENGTH);
            response = (data);
            response = Utils::trim_copy(response, " \f\n\r\t\v");

            if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_NAME) == 0) {
                server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");

                result_wr = write(sockfd, fileName.c_str(), fileName.size());

                if (result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : File name " + fileName, "info");
                bzero(data, FED_DATA_LENGTH + 1);
                read(sockfd, data, FED_DATA_LENGTH);
                response = (data);

                if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_LEN) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
                    result_wr = write(sockfd, fileLength.c_str(), fileLength.size());

                    if (result_wr < 0) {
                        server_logger.log("Error writing to socket", "error");
                    }

                    server_logger.log("Sent : File length in bytes " + fileLength, "info");
                    bzero(data, FED_DATA_LENGTH + 1);
                    read(sockfd, data, FED_DATA_LENGTH);
                    response = (data);

                    if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_CONT) == 0) {
                        server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT, "info");
                        server_logger.log("Going to send file through service", "info");
                        sendFileThroughService(host, dataPort, fileName, filePath, masterIP);
                    }
                }
            }
            int count = 0;

            while (true) {
                result_wr = write(sockfd, JasmineGraphInstanceProtocol::FILE_RECV_CHK.c_str(),
                                  JasmineGraphInstanceProtocol::FILE_RECV_CHK.size());

                if (result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : " + JasmineGraphInstanceProtocol::FILE_RECV_CHK, "info");
                server_logger.log("Checking if file is received", "info");
                bzero(data, FED_DATA_LENGTH + 1);
                read(sockfd, data, FED_DATA_LENGTH);
                response = (data);

                if (response.compare(JasmineGraphInstanceProtocol::FILE_RECV_WAIT) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_RECV_WAIT, "info");
                    server_logger.log("Checking file status : " + to_string(count), "info");
                    count++;
                    sleep(1);
                    continue;
                } else if (response.compare(JasmineGraphInstanceProtocol::FILE_ACK) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_ACK, "info");
                    server_logger.log("File transfer completed for file : " + filePath, "info");
                    break;
                }
            }

            // Next we wait till the batch upload completes
            while (true) {
                result_wr = write(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.c_str(),
                                  JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.size());

                if (result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                bzero(data, FED_DATA_LENGTH + 1);
                read(sockfd, data, FED_DATA_LENGTH);
                response = (data);

                if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT, "info");
                    sleep(1);
                    continue;
                } else if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK, "info");
                    server_logger.log("CentralStore partition file upload completed", "info");
                    break;
                }
            }
        }
    } else {
        server_logger.log("There was an error in the upload process and the response is :: " + response, "error");
    }

    close(sockfd);
    return 0;
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

bool JasmineGraphServer::batchUploadAttributeFile(std::string host, int port, int dataPort, int graphID,
                                                  std::string filePath, std::string masterIP) {
    int sockfd;
    char data[FED_DATA_LENGTH + 1];
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot create socket" << std::endl;
        return 0;
    }

    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        server_logger.error("ERROR, no host named " + host);
        return 0;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
        return 0;
    }

    bzero(data, FED_DATA_LENGTH + 1);
    int result_wr =
        write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if (result_wr < 0) {
        server_logger.log("Error writing to socket", "error");
    }

    server_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, FED_DATA_LENGTH + 1);
    read(sockfd, data, FED_DATA_LENGTH);
    string response = (data);

    response = Utils::trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        server_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if (result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + masterIP, "info");
        bzero(data, FED_DATA_LENGTH + 1);
        read(sockfd, data, FED_DATA_LENGTH);
        response = (data);

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            server_logger.log("Received : " + response, "error");
        }

        result_wr = write(sockfd, JasmineGraphInstanceProtocol::UPLOAD_RDF_ATTRIBUTES.c_str(),
                          JasmineGraphInstanceProtocol::UPLOAD_RDF_ATTRIBUTES.size());

        if (result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + JasmineGraphInstanceProtocol::UPLOAD_RDF_ATTRIBUTES, "info");
        bzero(data, FED_DATA_LENGTH + 1);
        read(sockfd, data, FED_DATA_LENGTH);
        response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");
        // std::cout << response << std::endl;
        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            // std::cout << graphID << std::endl;
            result_wr = write(sockfd, std::to_string(graphID).c_str(), std::to_string(graphID).size());

            if (result_wr < 0) {
                server_logger.log("Error writing to socket", "error");
            }

            server_logger.log("Sent : Graph ID " + std::to_string(graphID), "info");
            std::string fileName = Utils::getFileName(filePath);
            int fileSize = Utils::getFileSize(filePath);
            std::string fileLength = to_string(fileSize);

            bzero(data, FED_DATA_LENGTH + 1);
            read(sockfd, data, FED_DATA_LENGTH);
            response = (data);
            response = Utils::trim_copy(response, " \f\n\r\t\v");

            if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_NAME) == 0) {
                server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");
                // std::cout << fileName << std::endl;
                result_wr = write(sockfd, fileName.c_str(), fileName.size());

                if (result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : File name " + fileName, "info");
                bzero(data, FED_DATA_LENGTH + 1);
                read(sockfd, data, FED_DATA_LENGTH);
                response = (data);

                if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_LEN) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
                    result_wr = write(sockfd, fileLength.c_str(), fileLength.size());

                    if (result_wr < 0) {
                        server_logger.log("Error writing to socket", "error");
                    }

                    server_logger.log("Sent : File length in bytes " + fileLength, "info");
                    bzero(data, FED_DATA_LENGTH + 1);
                    read(sockfd, data, FED_DATA_LENGTH);
                    response = (data);

                    if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_CONT) == 0) {
                        server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT, "info");
                        server_logger.log("Going to send file through service", "info");
                        sendFileThroughService(host, dataPort, fileName, filePath, masterIP);
                    }
                }
            }
            int count = 0;

            while (true) {
                result_wr = write(sockfd, JasmineGraphInstanceProtocol::FILE_RECV_CHK.c_str(),
                                  JasmineGraphInstanceProtocol::FILE_RECV_CHK.size());

                if (result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : " + JasmineGraphInstanceProtocol::FILE_RECV_CHK, "info");
                server_logger.log("Checking if file is received", "info");
                bzero(data, FED_DATA_LENGTH + 1);
                read(sockfd, data, FED_DATA_LENGTH);
                response = (data);

                if (response.compare(JasmineGraphInstanceProtocol::FILE_RECV_WAIT) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_RECV_WAIT, "info");
                    server_logger.log("Checking file status : " + to_string(count), "info");
                    count++;
                    sleep(1);
                    continue;
                } else if (response.compare(JasmineGraphInstanceProtocol::FILE_ACK) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_ACK, "info");
                    server_logger.log("File transfer completed for file : " + filePath, "info");
                    break;
                }
            }

            // Next we wait till the batch upload completes
            while (true) {
                result_wr = write(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.c_str(),
                                  JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.size());

                if (result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                bzero(data, FED_DATA_LENGTH + 1);
                read(sockfd, data, FED_DATA_LENGTH);
                response = (data);

                if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT, "info");
                    sleep(1);
                    continue;
                } else if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK, "info");
                    server_logger.log("Attribute file upload completed", "info");
                    break;
                }
            }
        }
    } else {
        server_logger.log("There was an error in the upload process and the response is :: " + response, "error");
    }

    close(sockfd);
    return 0;
}

bool JasmineGraphServer::batchUploadCentralAttributeFile(std::string host, int port, int dataPort, int graphID,
                                                         std::string filePath, std::string masterIP) {
    int sockfd;
    char data[FED_DATA_LENGTH + 1];
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot create socket" << std::endl;
        return 0;
    }

    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        server_logger.error("ERROR, no host named " + host);
        return 0;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
        return 0;
    }

    bzero(data, FED_DATA_LENGTH + 1);
    int result_wr =
        write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if (result_wr < 0) {
        server_logger.log("Error writing to socket", "error");
    }

    server_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, FED_DATA_LENGTH + 1);
    read(sockfd, data, FED_DATA_LENGTH);
    string response = (data);

    response = Utils::trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        server_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if (result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + masterIP, "info");
        bzero(data, FED_DATA_LENGTH + 1);
        read(sockfd, data, FED_DATA_LENGTH);
        response = (data);

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            server_logger.log("Received : " + response, "error");
        }

        result_wr = write(sockfd, JasmineGraphInstanceProtocol::UPLOAD_RDF_ATTRIBUTES_CENTRAL.c_str(),
                          JasmineGraphInstanceProtocol::UPLOAD_RDF_ATTRIBUTES_CENTRAL.size());

        if (result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + JasmineGraphInstanceProtocol::UPLOAD_RDF_ATTRIBUTES_CENTRAL, "info");
        bzero(data, FED_DATA_LENGTH + 1);
        read(sockfd, data, FED_DATA_LENGTH);
        response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");
        // std::cout << response << std::endl;
        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            // std::cout << graphID << std::endl;
            result_wr = write(sockfd, std::to_string(graphID).c_str(), std::to_string(graphID).size());

            if (result_wr < 0) {
                server_logger.log("Error writing to socket", "error");
            }

            server_logger.log("Sent : Graph ID " + std::to_string(graphID), "info");
            std::string fileName = Utils::getFileName(filePath);
            int fileSize = Utils::getFileSize(filePath);
            std::string fileLength = to_string(fileSize);

            bzero(data, FED_DATA_LENGTH + 1);
            read(sockfd, data, FED_DATA_LENGTH);
            response = (data);
            response = Utils::trim_copy(response, " \f\n\r\t\v");

            if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_NAME) == 0) {
                server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");
                // std::cout << fileName << std::endl;
                result_wr = write(sockfd, fileName.c_str(), fileName.size());

                if (result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : File name " + fileName, "info");
                bzero(data, FED_DATA_LENGTH + 1);
                read(sockfd, data, FED_DATA_LENGTH);
                response = (data);

                if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_LEN) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
                    result_wr = write(sockfd, fileLength.c_str(), fileLength.size());

                    if (result_wr < 0) {
                        server_logger.log("Error writing to socket", "error");
                    }

                    server_logger.log("Sent : File length in bytes " + fileLength, "info");
                    bzero(data, FED_DATA_LENGTH + 1);
                    read(sockfd, data, FED_DATA_LENGTH);
                    response = (data);

                    if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_CONT) == 0) {
                        server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT, "info");
                        server_logger.log("Going to send file through service", "info");
                        sendFileThroughService(host, dataPort, fileName, filePath, masterIP);
                    }
                }
            }
            int count = 0;

            while (true) {
                result_wr = write(sockfd, JasmineGraphInstanceProtocol::FILE_RECV_CHK.c_str(),
                                  JasmineGraphInstanceProtocol::FILE_RECV_CHK.size());

                if (result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : " + JasmineGraphInstanceProtocol::FILE_RECV_CHK, "info");
                server_logger.log("Checking if file is received", "info");
                bzero(data, FED_DATA_LENGTH + 1);
                read(sockfd, data, FED_DATA_LENGTH);
                response = (data);

                if (response.compare(JasmineGraphInstanceProtocol::FILE_RECV_WAIT) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_RECV_WAIT, "info");
                    server_logger.log("Checking file status : " + to_string(count), "info");
                    count++;
                    sleep(1);
                    continue;
                } else if (response.compare(JasmineGraphInstanceProtocol::FILE_ACK) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_ACK, "info");
                    server_logger.log("File transfer completed for file :" + filePath, "info");
                    break;
                }
            }

            // Next we wait till the batch upload completes
            while (true) {
                result_wr = write(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.c_str(),
                                  JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.size());

                if (result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                bzero(data, FED_DATA_LENGTH + 1);
                read(sockfd, data, FED_DATA_LENGTH);
                response = (data);

                if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT, "info");
                    sleep(1);
                    continue;
                } else if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK, "info");
                    server_logger.log("Attribute file upload completed", "info");
                    break;
                }
            }
        }
    } else {
        server_logger.log("There was an error in the upload process and the response is :: " + response, "error");
    }

    close(sockfd);
    return 0;
}

bool JasmineGraphServer::batchUploadCompositeCentralstoreFile(std::string host, int port, int dataPort, int graphID,
                                                              std::string filePath, std::string masterIP) {
    int sockfd;
    char data[FED_DATA_LENGTH + 1];
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot create socket" << std::endl;
        return 0;
    }

    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        server_logger.error("ERROR, no host named " + host);
        return 0;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
        return 0;
    }

    bzero(data, FED_DATA_LENGTH + 1);
    int result_wr =
        write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if (result_wr < 0) {
        server_logger.log("Error writing to socket", "error");
    }

    server_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, FED_DATA_LENGTH + 1);
    read(sockfd, data, FED_DATA_LENGTH);
    string response = (data);

    response = Utils::trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        server_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if (result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + masterIP, "info");
        bzero(data, FED_DATA_LENGTH + 1);
        read(sockfd, data, FED_DATA_LENGTH);
        response = (data);

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            server_logger.log("Received : " + response, "error");
        }

        result_wr = write(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_COMPOSITE_CENTRAL.c_str(),
                          JasmineGraphInstanceProtocol::BATCH_UPLOAD_COMPOSITE_CENTRAL.size());

        if (result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_COMPOSITE_CENTRAL, "info");
        bzero(data, FED_DATA_LENGTH + 1);
        read(sockfd, data, FED_DATA_LENGTH);
        response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");

            result_wr = write(sockfd, std::to_string(graphID).c_str(), std::to_string(graphID).size());

            if (result_wr < 0) {
                server_logger.log("Error writing to socket", "error");
            }

            server_logger.log("Sent : Graph ID " + std::to_string(graphID), "info");
            std::string fileName = Utils::getFileName(filePath);
            int fileSize = Utils::getFileSize(filePath);
            std::string fileLength = to_string(fileSize);

            bzero(data, FED_DATA_LENGTH + 1);
            read(sockfd, data, FED_DATA_LENGTH);
            response = (data);
            response = Utils::trim_copy(response, " \f\n\r\t\v");

            if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_NAME) == 0) {
                server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");

                result_wr = write(sockfd, fileName.c_str(), fileName.size());

                if (result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : File name " + fileName, "info");
                bzero(data, FED_DATA_LENGTH + 1);
                read(sockfd, data, FED_DATA_LENGTH);
                response = (data);

                if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_LEN) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
                    result_wr = write(sockfd, fileLength.c_str(), fileLength.size());

                    if (result_wr < 0) {
                        server_logger.log("Error writing to socket", "error");
                    }

                    server_logger.log("Sent : File length in bytes " + fileLength, "info");
                    bzero(data, FED_DATA_LENGTH + 1);
                    read(sockfd, data, FED_DATA_LENGTH);
                    response = (data);

                    if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_CONT) == 0) {
                        server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT, "info");
                        server_logger.log("Going to send file through service", "info");
                        sendFileThroughService(host, dataPort, fileName, filePath, masterIP);
                    }
                }
            }
            int count = 0;

            while (true) {
                result_wr = write(sockfd, JasmineGraphInstanceProtocol::FILE_RECV_CHK.c_str(),
                                  JasmineGraphInstanceProtocol::FILE_RECV_CHK.size());

                if (result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : " + JasmineGraphInstanceProtocol::FILE_RECV_CHK, "info");
                server_logger.log("Checking if file is received", "info");
                bzero(data, FED_DATA_LENGTH + 1);
                read(sockfd, data, FED_DATA_LENGTH);
                response = (data);

                if (response.compare(JasmineGraphInstanceProtocol::FILE_RECV_WAIT) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_RECV_WAIT, "info");
                    server_logger.log("Checking file status : " + to_string(count), "info");
                    count++;
                    sleep(1);
                    continue;
                } else if (response.compare(JasmineGraphInstanceProtocol::FILE_ACK) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_ACK, "info");
                    server_logger.log("File transfer completed for file : " + filePath, "info");
                    break;
                }
            }

            // Next we wait till the batch upload completes
            while (true) {
                result_wr = write(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.c_str(),
                                  JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.size());

                if (result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                bzero(data, FED_DATA_LENGTH + 1);
                read(sockfd, data, FED_DATA_LENGTH);
                response = (data);

                if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT, "info");
                    sleep(1);
                    continue;
                } else if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK, "info");
                    server_logger.log("Composite CentralStore partition file upload completed", "info");
                    break;
                }
            }
        }
    } else {
        server_logger.log("There was an error in the upload process and the response is :: " + response, "error");
    }

    close(sockfd);
    return 0;
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
        std::cerr << "Cannot create socket" << std::endl;
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
        std::cerr << "ERROR connecting to port " << dataPort << std::endl;
        return false;
    }

    int result_wr = write(sockfd, fileName.c_str(), fileName.size());

    if (result_wr < 0) {
        server_logger.log("Error writing to socket", "error");
    }

    bzero(data, FED_DATA_LENGTH + 1);
    read(sockfd, data, FED_DATA_LENGTH);
    string response = (data);
    response = Utils::trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE) == 0) {
        std::cout << "Sending file " << filePath << " through port " << dataPort << std::endl;

        FILE *fp = fopen(filePath.c_str(), "r");
        if (fp == NULL) {
            // printf("Error opening file\n");
            close(sockfd);
            return false;
        }

        for (;;) {
            unsigned char buff[1024] = {0};
            int nread = fread(buff, 1, 1024, fp);
            // printf("Bytes read %d \n", nread);

            /* If read was success, send data. */
            if (nread > 0) {
                // printf("Sending \n");
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
    return false;
}

static void copyArtifactsToWorkers(const std::string &workerPath, const std::string &artifactLocation,
                                   const std::string &remoteWorker) {
    if (artifactLocation.empty() || artifactLocation.find_first_not_of(' ') == artifactLocation.npos) {
        server_logger.log("Received `" + artifactLocation + "` for `artifactLocation` value!", "error");
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

        if (!Utils::hostExists(name, ip_address, std::to_string(workerPort), *(this->sqlite))) {
            string hostID = Utils::getHostID(name, *(this->sqlite));
            sqlStatement =
                ("INSERT INTO worker (host_idhost,name,ip,user,is_public,server_port,server_data_port) VALUES (\"" +
                 hostID + "\", \"" + name + "\", \"" + ip_address + "\",\"" + user + "\", \"\",\"" +
                 std::to_string(workerPort) + "\", \"" + std::to_string(workerDataPort) + "\")");
            this->sqlite->runInsert(sqlStatement);
        }
    }
}

map<string, string> JasmineGraphServer::getLiveHostIDList() {
    server_logger.log("###MASTER### Loading Live Host ID List", "info");
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
    std::cout << "Deleting the graph partitions.." << std::endl;
    int count = 0;
    std::thread *deleteThreads = new std::thread[hostHasPartition.size()];
    for (std::vector<pair<string, string>>::iterator j = (hostHasPartition.begin()); j != hostHasPartition.end(); ++j) {
        deleteThreads[count] = std::thread(removePartitionThroughService, j->first, hostPortMap[j->first].first,
                                           graphID, j->second, masterIP);
        count++;
        sleep(1);
    }

    for (int threadCount = 0; threadCount < count; threadCount++) {
        deleteThreads[threadCount].join();
        std::cout << "Thread [A]: " << threadCount << " joined" << std::endl;
    }
}

/** Used to delete graph fragments of a given graph ID for a particular host running at a particular port
 *
 *  @param host Hostname of worker
 *  @param port Port host is running on
 *  @param graphID ID of graph fragments to be deleted
 *  @param masterIP IP of master node
 */
int JasmineGraphServer::removeFragmentThroughService(string host, int port, string graphID, string masterIP) {
    std::cout << pthread_self() << " host : " << host << " port : " << port << std::endl;
    int sockfd;
    char data[FED_DATA_LENGTH + 1];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot create socket" << std::endl;
        return 0;
    }

    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        server_logger.error("ERROR, no host named " + host);
        return 0;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
        // TODO::exit
        return 0;
    }

    bzero(data, FED_DATA_LENGTH + 1);
    int result_wr =
        write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if (result_wr < 0) {
        server_logger.log("Error writing to socket", "error");
    }

    server_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, FED_DATA_LENGTH + 1);
    read(sockfd, data, FED_DATA_LENGTH);
    string response = (data);

    response = Utils::trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        server_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if (result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + masterIP, "info");
        bzero(data, FED_DATA_LENGTH + 1);
        read(sockfd, data, FED_DATA_LENGTH);
        response = (data);

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            server_logger.log("Received : " + response, "error");
        }

        result_wr = write(sockfd, JasmineGraphInstanceProtocol::DELETE_GRAPH_FRAGMENT.c_str(),
                          JasmineGraphInstanceProtocol::DELETE_GRAPH_FRAGMENT.size());

        if (result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + JasmineGraphInstanceProtocol::DELETE_GRAPH_FRAGMENT, "info");
        bzero(data, FED_DATA_LENGTH + 1);
        read(sockfd, data, FED_DATA_LENGTH);
        response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");
        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, graphID.c_str(), graphID.size());

            if (result_wr < 0) {
                server_logger.log("Error writing to socket", "error");
            }

            server_logger.log("Sent : Graph ID " + graphID, "info");

            bzero(data, FED_DATA_LENGTH + 1);
            read(sockfd, data, FED_DATA_LENGTH);
            response = (data);
            response = Utils::trim_copy(response, " \f\n\r\t\v");
            server_logger.log("Received last response : " + response, "info");
            return 1;

        } else {
            close(sockfd);
            return 0;
        }
    }
    close(sockfd);
    return 0;
}

int JasmineGraphServer::removePartitionThroughService(string host, int port, string graphID, string partitionID,
                                                      string masterIP) {
    std::cout << pthread_self() << " host : " << host << " port : " << port << std::endl;
    int sockfd;
    char data[FED_DATA_LENGTH + 1];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot create socket" << std::endl;
        return 0;
    }

    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        server_logger.error("ERROR, no host named " + host);
        return 0;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
        // TODO::exit
        return 0;
    }

    bzero(data, FED_DATA_LENGTH + 1);
    int result_wr =
        write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if (result_wr < 0) {
        server_logger.log("Error writing to socket", "error");
    }

    server_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, FED_DATA_LENGTH + 1);
    read(sockfd, data, FED_DATA_LENGTH);
    string response = (data);

    response = Utils::trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        server_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if (result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + masterIP, "info");
        bzero(data, FED_DATA_LENGTH + 1);
        read(sockfd, data, FED_DATA_LENGTH);
        response = (data);

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            server_logger.log("Received : " + response, "error");
        }

        result_wr = write(sockfd, JasmineGraphInstanceProtocol::DELETE_GRAPH.c_str(),
                          JasmineGraphInstanceProtocol::DELETE_GRAPH.size());

        if (result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + JasmineGraphInstanceProtocol::DELETE_GRAPH, "info");
        bzero(data, FED_DATA_LENGTH + 1);
        read(sockfd, data, FED_DATA_LENGTH);
        response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, graphID.c_str(), graphID.size());

            if (result_wr < 0) {
                server_logger.log("Error writing to socket", "error");
            }

            server_logger.log("Sent : Graph ID " + graphID, "info");

            bzero(data, FED_DATA_LENGTH + 1);
            read(sockfd, data, FED_DATA_LENGTH);
            response = (data);
            response = Utils::trim_copy(response, " \f\n\r\t\v");

            if (response.compare(JasmineGraphInstanceProtocol::SEND_PARTITION_ID) == 0) {
                server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_PARTITION_ID, "info");
                result_wr = write(sockfd, partitionID.c_str(), partitionID.size());

                if (result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : Partition ID " + partitionID, "info");

                bzero(data, FED_DATA_LENGTH + 1);
                read(sockfd, data, FED_DATA_LENGTH);
                response = (data);
                response = Utils::trim_copy(response, " \f\n\r\t\v");
                server_logger.log("Received last response : " + response, "info");
                return 1;
            } else {
                close(sockfd);
                return 0;
            }
        } else {
            close(sockfd);
            return 0;
        }
    }
    close(sockfd);
    return 0;
}

std::vector<JasmineGraphServer::workers> JasmineGraphServer::getHostWorkerMap() { return hostWorkerMap; }

void JasmineGraphServer::updateOperationalGraphList() {
    string hosts = "";
    string graphIDs = "";
    std::vector<std::string> hostsList;

    if (profile == "native") {
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
    SQLiteDBInterface refToSqlite = *new SQLiteDBInterface();
    refToSqlite.init();
    map<std::string, JasmineGraphServer::workerPartition> graphPartitionedHosts;
    vector<vector<pair<string, string>>> hostPartitionResults = refToSqlite.runSelect(
        "SELECT name, worker_idworker, server_port, server_data_port, partition_idpartition FROM worker_has_partition "
        "INNER JOIN worker ON worker_"
        "idworker = idworker WHERE partition_graph_idgraph = '" +
        graphID + "'");

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
    SQLiteDBInterface refToSqlite = *new SQLiteDBInterface();
    refToSqlite.init();
    vector<vector<pair<string, string>>> hostPartitionResults = refToSqlite.runSelect(
        "SELECT name, partition_idpartition FROM worker_has_partition INNER JOIN worker ON worker_"
        "idworker = idworker WHERE partition_graph_idgraph = '" +
        graphID + "'");
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
        cout << "HOST ID : " << j->first << " Partition ID : " << j->second << endl;
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

void JasmineGraphServer::inDegreeDistribution(std::string graphID) {
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
            std::cout << "Cannot create socket" << std::endl;
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
            std::cout << "ERROR connecting" << std::endl;
            // TODO::exit
            continue;
        }

        bzero(data, FED_DATA_LENGTH + 1);
        int result_wr = write(sockfd, JasmineGraphInstanceProtocol::IN_DEGREE_DISTRIBUTION.c_str(),
                              JasmineGraphInstanceProtocol::IN_DEGREE_DISTRIBUTION.size());
        if (result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + JasmineGraphInstanceProtocol::IN_DEGREE_DISTRIBUTION, "info");
        bzero(data, FED_DATA_LENGTH + 1);
        read(sockfd, data, FED_DATA_LENGTH);
        string response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");

            result_wr = write(sockfd, graphID.c_str(), graphID.size());

            if (result_wr < 0) {
                server_logger.log("Error writing to socket", "error");
            }
            server_logger.log("Sent : Graph ID " + graphID, "info");

            bzero(data, FED_DATA_LENGTH + 1);
            read(sockfd, data, FED_DATA_LENGTH);
            string response = (data);
            response = Utils::trim_copy(response, " \f\n\r\t\v");

            if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
                server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
                int partitionID = stoi(partition);
                result_wr = write(sockfd, std::to_string(partitionID).c_str(), std::to_string(partitionID).size());

                if (result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : Partition ID " + std::to_string(partitionID), "info");

                bzero(data, FED_DATA_LENGTH + 1);
                read(sockfd, data, FED_DATA_LENGTH);
                string response = (data);
                response = Utils::trim_copy(response, " \f\n\r\t\v");

                if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
                    result_wr = write(sockfd, workerList.c_str(), workerList.size());

                    if (result_wr < 0) {
                        server_logger.log("Error writing to socket", "error");
                    }

                    server_logger.log("Sent : Host List ", "info");
                }
            }
        }
    }
}

void JasmineGraphServer::outDegreeDistribution(std::string graphID) {
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
            std::cout << "Cannot create socket" << std::endl;
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
            std::cout << "ERROR connecting" << std::endl;
            // TODO::exit
            continue;
        }

        bzero(data, FED_DATA_LENGTH + 1);
        int result_wr = write(sockfd, JasmineGraphInstanceProtocol::OUT_DEGREE_DISTRIBUTION.c_str(),
                              JasmineGraphInstanceProtocol::OUT_DEGREE_DISTRIBUTION.size());
        if (result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + JasmineGraphInstanceProtocol::OUT_DEGREE_DISTRIBUTION, "info");
        bzero(data, FED_DATA_LENGTH + 1);
        read(sockfd, data, FED_DATA_LENGTH);
        string response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");

            result_wr = write(sockfd, graphID.c_str(), graphID.size());

            if (result_wr < 0) {
                server_logger.log("Error writing to socket", "error");
            }
            server_logger.log("Sent : Graph ID " + graphID, "info");

            bzero(data, FED_DATA_LENGTH + 1);
            read(sockfd, data, FED_DATA_LENGTH);
            string response = (data);
            response = Utils::trim_copy(response, " \f\n\r\t\v");

            if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
                server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
                int partitionID = stoi(partition);
                result_wr = write(sockfd, std::to_string(partitionID).c_str(), std::to_string(partitionID).size());

                if (result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }
                server_logger.log("Sent : Partition ID " + std::to_string(partitionID), "info");

                bzero(data, FED_DATA_LENGTH + 1);
                read(sockfd, data, FED_DATA_LENGTH);
                string response = (data);
                response = Utils::trim_copy(response, " \f\n\r\t\v");

                if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
                    result_wr = write(sockfd, workerList.c_str(), workerList.size());

                    if (result_wr < 0) {
                        server_logger.log("Error writing to socket", "error");
                    }
                    server_logger.log("Sent : Host List ", "info");
                }
            }
        }
    }
}

long JasmineGraphServer::getGraphVertexCount(std::string graphID) {
    SQLiteDBInterface refToSqlite = *new SQLiteDBInterface();
    refToSqlite.init();
    vector<vector<pair<string, string>>> output =
        refToSqlite.runSelect("SELECT vertexcount FROM graph WHERE idgraph = '" + graphID + "'");

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
            std::cout << "Cannot create socket" << std::endl;
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
            std::cout << "ERROR connecting" << std::endl;
            // TODO::exit
            continue;
        }

        bzero(data, FED_DATA_LENGTH + 1);
        int result_wr = write(sockfd, JasmineGraphInstanceProtocol::DP_CENTRALSTORE.c_str(),
                              JasmineGraphInstanceProtocol::DP_CENTRALSTORE.size());
        if (result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + JasmineGraphInstanceProtocol::DP_CENTRALSTORE, "info");
        bzero(data, FED_DATA_LENGTH + 1);
        read(sockfd, data, FED_DATA_LENGTH);
        string response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
        } else {
            server_logger.log("Error reading from socket", "error");
        }

        result_wr = write(sockfd, graphID.c_str(), graphID.size());

        if (result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }
        server_logger.log("Sent : Graph ID " + graphID, "info");

        bzero(data, FED_DATA_LENGTH + 1);
        read(sockfd, data, FED_DATA_LENGTH);
        response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
        } else {
            server_logger.log("Error reading from socket", "error");
        }

        int partitionID = stoi(partition);

        result_wr = write(sockfd, std::to_string(partitionID).c_str(), std::to_string(partitionID).size());

        if (result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : Partition ID " + std::to_string(partitionID), "info");

        bzero(data, FED_DATA_LENGTH + 1);
        read(sockfd, data, FED_DATA_LENGTH);
        response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
        } else {
            server_logger.log("Error reading from socket", "error");
        }

        result_wr = write(sockfd, workerList.c_str(), workerList.size());

        if (result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : Host List ", "info");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
        } else {
            server_logger.log("Error reading from socket", "error");
        }
    }
}

void JasmineGraphServer::initiateFiles(std::string graphID, std::string trainingArgs) {
    int count = 0;
    JasmineGraphTrainingSchedular *schedular = new JasmineGraphTrainingSchedular();
    map<string, map<int, int>> scheduleForAllHosts = schedular->schedulePartitionTraining(graphID);
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
                            trainingArgs + " " + *k, iterationOfPart, partitionCount);
            count++;
            sleep(3);
        }
    }

    for (int threadCount = 0; threadCount < count; threadCount++) {
        workerThreads[threadCount].join();
        server_logger.log("Thread : " + to_string(threadCount) + " joined", "info");
    }
}

void JasmineGraphServer::initiateCommunication(std::string graphID, std::string trainingArgs,
                                               SQLiteDBInterface sqlite) {
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
                                                  trainingArgs, fl_clients, to_string(i));
            threadID++;
        }

        workerThreads[threadID] = std::thread(initiateClient, workerInstance.hostname, serverPort, serverDataPort,
                                              trainingArgs + " " + to_string(i), fl_clients, to_string(i));
        threadID++;
    }

    workerThreads[0].join();

    for (int threadCount = 1; threadCount < threadLimit; threadCount++) {
        workerThreads[threadCount].join();
    }

    server_logger.log("Federated learning commands sent", "info");
}

void JasmineGraphServer::initiateOrgCommunication(std::string graphID, std::string trainingArgs,
                                                  SQLiteDBInterface sqlite) {
    int fl_clients = stoi(Utils::getJasmineGraphProperty("org.jasminegraph.fl_clients"));
    int orgs_count = stoi(Utils::getJasmineGraphProperty("org.jasminegraph.fl.num.orgs"));
    std::string flagPath = Utils::getJasmineGraphProperty("org.jasminegraph.fl.flag.file");
    int threadLimit = fl_clients + 1;
    int org_thread_count = 0;
    std::thread *workerThreads = new std::thread[threadLimit];

    Utils::editFlagOne(flagPath);
    Utils::worker workerInstance;
    vector<Utils::worker> workerVector = Utils::getWorkerList(sqlite);

    std::thread *trainThreads = new std::thread[orgs_count];

    if (Utils::getJasmineGraphProperty("org.jasminegraph.fl.aggregator") == "true") {
        std::thread *orgAggThread = new std::thread[1];
        orgAggThread[0] = std::thread(initiateAggregator, "localhost", stoi(workerVector[0].port),
                                      stoi(workerVector[0].dataPort), trainingArgs, fl_clients, "1");
        orgAggThread[0].join();
    }

    std::ifstream file(Utils::getJasmineGraphProperty("org.jasminegraph.fl.organization.file"));

    if (file.good()) {
        if (file.is_open()) {
            std::string line;
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
            server_logger.log("Organizational details loading successful", "info");
        }
    } else {
        server_logger.log("Error loading organization details", "info");
    }

    std::thread *communicationThread = new std::thread[1];
    communicationThread[0] = std::thread(receiveGlobalWeights, "localhost", 5000, "0", 1, "1");
    server_logger.log("Communication Thread Initiated", "info");
    trainingArgs = trainingArgs;
    int threadID = 0;

    for (int i = 0; i < workerVector.size(); i++) {
        workerInstance = workerVector[i];
        int serverPort = stoi(workerInstance.port);
        int serverDataPort = stoi(workerInstance.dataPort);

        if (i == 0) {
            workerThreads[threadID] = std::thread(initiateOrgServer, workerInstance.hostname, serverPort,
                                                  serverDataPort, trainingArgs, fl_clients, to_string(i));
            threadID++;
        }

        workerThreads[threadID] = std::thread(initiateClient, workerInstance.hostname, serverPort, serverDataPort,
                                              trainingArgs + " " + to_string(i), fl_clients, to_string(i));
        threadID++;
    }

    workerThreads[0].join();
    communicationThread[0].join();

    for (int threadCount = 1; threadCount < threadLimit; threadCount++) {
        workerThreads[threadCount].join();
    }

    server_logger.log("Federated learning commands sent", "info");
}

void JasmineGraphServer::initiateMerge(std::string graphID, std::string trainingArgs, SQLiteDBInterface sqlite) {
    int count = 0;
    JasmineGraphTrainingSchedular *schedular = new JasmineGraphTrainingSchedular();
    map<string, map<int, int>> scheduleForAllHosts = schedular->schedulePartitionTraining(graphID);
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
            workerThreads[count] = std::thread(mergeFiles, workerPartition.hostname, workerPartition.port,
                                               workerPartition.dataPort, trainingArgs + " " + *k, fl_clients, *k);
            count++;
        }
    }

    for (int threadCount = 0; threadCount < count; threadCount++) {
        workerThreads[threadCount].join();
    }

    server_logger.log("Merge Commands Sent", "info");
}

bool JasmineGraphServer::initiateTrain(std::string host, int port, int dataPort, std::string trainingArgs,
                                       int iteration, string partCount) {
    bool result = true;
    int sockfd;
    char data[FED_DATA_LENGTH + 1];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        server_logger.log("Cannot create socket", "error");
        return 0;
    }

    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        server_logger.log("ERROR, can not find the host", "error");
        return 0;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        server_logger.log("ERROR connecting", "error");
        // TODO::exit
        return 0;
    }

    bzero(data, FED_DATA_LENGTH);
    int result_wr =
        write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());
    if (result_wr < 0) {
        server_logger.log("Error writing to socket", "error");
    }

    server_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, FED_DATA_LENGTH);
    read(sockfd, data, FED_DATA_LENGTH);
    string response = (data);

    response = Utils::trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        server_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        string server_host = Utils::getJasmineGraphProperty("org.jasminegraph.server.host");
        result_wr = write(sockfd, server_host.c_str(), server_host.size());
        server_logger.log("Sent : " + server_host, "info");
        if (result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        bzero(data, FED_DATA_LENGTH + 1);
        read(sockfd, data, FED_DATA_LENGTH);
        response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            server_logger.log("Received : " + response, "error");
        }

        write(sockfd, JasmineGraphInstanceProtocol::INITIATE_FED_PREDICT.c_str(),
              JasmineGraphInstanceProtocol::INITIATE_FED_PREDICT.size());
        server_logger.log("Sent : " + JasmineGraphInstanceProtocol::INITIATE_FED_PREDICT, "info");
        bzero(data, FED_DATA_LENGTH);
        read(sockfd, data, FED_DATA_LENGTH);
        response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");
        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            write(sockfd, (trainingArgs).c_str(), (trainingArgs).size());
            server_logger.log("Sent : args " + trainingArgs, "info");
            bzero(data, FED_DATA_LENGTH);
            return 0;
        }
    } else {
        server_logger.log("There was an error in the invoking training process and the response is :: " + response,
                          "error");
    }

    close(sockfd);
    return 0;
}

bool JasmineGraphServer::initiatePredict(std::string host, int port, int dataPort, std::string trainingArgs,
                                         int iteration, string partCount) {
    bool result = true;
    int sockfd;
    char data[FED_DATA_LENGTH];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        server_logger.log("Cannot create socket", "error");
        return 0;
    }

    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        server_logger.log("ERROR, can not find the host", "error");
        return 0;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        server_logger.log("ERROR connecting", "error");
        // TODO::exit
        return 0;
    }

    bzero(data, FED_DATA_LENGTH);
    write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());
    server_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, FED_DATA_LENGTH);
    read(sockfd, data, FED_DATA_LENGTH);
    string response = (data);

    response = Utils::trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        server_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        string server_host = Utils::getJasmineGraphProperty("org.jasminegraph.server.host");
        write(sockfd, server_host.c_str(), server_host.size());
        server_logger.log("Sent : " + server_host, "info");

        write(sockfd, JasmineGraphInstanceProtocol::INITIATE_FILES.c_str(),
              JasmineGraphInstanceProtocol::INITIATE_FILES.size());
        server_logger.log("Sent : " + JasmineGraphInstanceProtocol::INITIATE_FILES, "info");
        bzero(data, FED_DATA_LENGTH);
        read(sockfd, data, FED_DATA_LENGTH);
        response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");
        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            write(sockfd, (trainingArgs).c_str(), (trainingArgs).size());
            server_logger.log("Sent : args " + trainingArgs, "info");
            bzero(data, FED_DATA_LENGTH);
            return 0;
        }
    } else {
        server_logger.log("There was an error in the invoking training process and the response is :: " + response,
                          "error");
    }

    close(sockfd);
    return 0;
}

bool JasmineGraphServer::initiateServer(std::string host, int port, int dataPort, std::string trainingArgs,
                                        int iteration, string partCount) {
    bool result = true;
    int sockfd;
    char data[FED_DATA_LENGTH + 1];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        server_logger.log("Cannot create socket", "error");
        return 0;
    }

    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        server_logger.log("ERROR, can not find the host", "error");
        return 0;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        server_logger.log("ERROR connecting", "error");
        // TODO::exit
        return 0;
    }

    bzero(data, FED_DATA_LENGTH);
    write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());
    server_logger.log("Sent fed : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, FED_DATA_LENGTH);
    read(sockfd, data, FED_DATA_LENGTH);
    string response = (data);

    response = Utils::trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        server_logger.log("Received fed : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        string server_host = Utils::getJasmineGraphProperty("org.jasminegraph.server.host");
        write(sockfd, server_host.c_str(), server_host.size());
        server_logger.log("Sent fed : " + server_host, "info");
        bzero(data, FED_DATA_LENGTH + 1);
        read(sockfd, data, FED_DATA_LENGTH);
        response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        }

        write(sockfd, JasmineGraphInstanceProtocol::INITIATE_SERVER.c_str(),
              JasmineGraphInstanceProtocol::INITIATE_SERVER.size());
        server_logger.log("Sent fed : " + JasmineGraphInstanceProtocol::INITIATE_SERVER, "info");
        bzero(data, FED_DATA_LENGTH + 1);
        read(sockfd, data, FED_DATA_LENGTH);
        response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");
        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.log("Received fed : " + JasmineGraphInstanceProtocol::OK, "info");
            write(sockfd, (trainingArgs).c_str(), (trainingArgs).size());
            server_logger.log("Sent fed : training args " + trainingArgs, "info");
            bzero(data, FED_DATA_LENGTH);
            return 0;
        }
    } else {
        server_logger.log("There was an error in the invoking training process and the response is :: " + response,
                          "error");
    }

    close(sockfd);
    return 0;
}

// todo Remove partCount from parameters as the partition id is being parsed within the training args
bool JasmineGraphServer::initiateClient(std::string host, int port, int dataPort, std::string trainingArgs,
                                        int iteration, string partCount) {
    bool result = true;
    int sockfd;
    char data[FED_DATA_LENGTH + 1];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        server_logger.log("Cannot create socket", "error");
        return 0;
    }

    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        server_logger.log("ERROR, can not find the host", "error");
        return 0;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        server_logger.log("ERROR connecting", "error");
        // TODO::exit
        return 0;
    }

    bzero(data, FED_DATA_LENGTH);
    write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());
    server_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, FED_DATA_LENGTH);
    read(sockfd, data, FED_DATA_LENGTH);
    string response = (data);

    response = Utils::trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        server_logger.log("Received fed : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        string server_host = Utils::getJasmineGraphProperty("org.jasminegraph.server.host");
        write(sockfd, server_host.c_str(), server_host.size());
        server_logger.log("Sent fed : " + server_host, "info");
        bzero(data, FED_DATA_LENGTH + 1);
        read(sockfd, data, FED_DATA_LENGTH);
        response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        }

        write(sockfd, JasmineGraphInstanceProtocol::INITIATE_CLIENT.c_str(),
              JasmineGraphInstanceProtocol::INITIATE_CLIENT.size());
        server_logger.log("Sent fed : " + JasmineGraphInstanceProtocol::INITIATE_CLIENT, "info");
        bzero(data, FED_DATA_LENGTH);
        read(sockfd, data, FED_DATA_LENGTH);
        response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");
        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.log(to_string(pthread_self()) + " Received fed : " + JasmineGraphInstanceProtocol::OK,
                              "info");
            write(sockfd, (trainingArgs).c_str(), (trainingArgs).size());
            server_logger.log(to_string(pthread_self()) + " Sent fed : training args " + trainingArgs, "info");
            bzero(data, FED_DATA_LENGTH);
            return 0;
        }
    } else {
        server_logger.log("There was an error in the invoking training process and the response is :: " + response,
                          "error");
    }

    close(sockfd);
    return 0;
}

bool JasmineGraphServer::initiateAggregator(std::string host, int port, int dataPort, std::string trainingArgs,
                                            int iteration, string partCount) {
    bool result = true;
    int sockfd;
    char data[FED_DATA_LENGTH + 1];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        server_logger.log("Cannot create socket", "error");
        return 0;
    }

    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        server_logger.log("ERROR, can not find the host", "error");
        return 0;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        server_logger.log("ERROR connecting", "error");
        // TODO::exit
        return 0;
    }

    bzero(data, FED_DATA_LENGTH);
    write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());
    server_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, FED_DATA_LENGTH);
    read(sockfd, data, FED_DATA_LENGTH);
    string response = (data);

    response = Utils::trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        server_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        string server_host = Utils::getJasmineGraphProperty("org.jasminegraph.server.host");
        write(sockfd, server_host.c_str(), server_host.size());
        server_logger.log("Sent host : " + server_host, "info");

        bzero(data, FED_DATA_LENGTH + 1);
        read(sockfd, data, FED_DATA_LENGTH);
        response = (data);
        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            server_logger.log("Received : " + response, "error");
        }

        write(sockfd, JasmineGraphInstanceProtocol::INITIATE_AGG.c_str(),
              JasmineGraphInstanceProtocol::INITIATE_AGG.size());
        server_logger.log("Sent : " + JasmineGraphInstanceProtocol::INITIATE_AGG, "info");
        bzero(data, FED_DATA_LENGTH);
        read(sockfd, data, FED_DATA_LENGTH);
        response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");
        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            write(sockfd, (trainingArgs).c_str(), (trainingArgs).size());
            bzero(data, FED_DATA_LENGTH);
            return 0;
        }
    } else {
        server_logger.log("There was an error in the invoking training process and the response is :: " + response,
                          "error");
    }

    close(sockfd);
    return 0;
}

bool JasmineGraphServer::initiateOrgServer(std::string host, int port, int dataPort, std::string trainingArgs,
                                           int iteration, string partCount) {
    bool result = true;
    int sockfd;
    char data[FED_DATA_LENGTH];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        server_logger.log("Cannot create socket", "error");
        return 0;
    }

    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        server_logger.log("ERROR, can not find the host", "error");
        return 0;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        server_logger.log("Error connecting", "error");
        // TODO::exit
        return 0;
    }

    bzero(data, FED_DATA_LENGTH);
    write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());
    server_logger.log("Sent fed : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, FED_DATA_LENGTH);
    read(sockfd, data, FED_DATA_LENGTH);
    string response = (data);

    response = Utils::trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        server_logger.log("Received fed : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        string server_host = Utils::getJasmineGraphProperty("org.jasminegraph.server.host");
        write(sockfd, server_host.c_str(), server_host.size());
        server_logger.log("Sent fed : " + server_host, "info");

        write(sockfd, JasmineGraphInstanceProtocol::INITIATE_ORG_SERVER.c_str(),
              JasmineGraphInstanceProtocol::INITIATE_ORG_SERVER.size());
        server_logger.log("Sent fed : " + JasmineGraphInstanceProtocol::INITIATE_ORG_SERVER, "info");
        bzero(data, FED_DATA_LENGTH);
        read(sockfd, data, FED_DATA_LENGTH);
        response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");
        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.log("Received fed : " + JasmineGraphInstanceProtocol::OK, "info");
            write(sockfd, (trainingArgs).c_str(), (trainingArgs).size());
            server_logger.log("Sent fed : training args " + trainingArgs, "info");
            bzero(data, FED_DATA_LENGTH);
            return 0;
        }
    } else {
        server_logger.log("There was an error in the invoking training process and the response is :: " + response,
                          "error");
    }

    close(sockfd);
    return 0;
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
        server_logger.log("Cannot create socket", "error");
        return 0;
    }
    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }
    server = gethostbyname(host.c_str());
    if (server == NULL) {
        server_logger.log("ERROR, can not find the host", "error");
        return 0;
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
        server_logger.log("message length : " + length, "info");
        char content[stoi(length)];
        int full_size = stoi(length) + HEADER_LENGTH;

        for (int i = 0; i < full_size; i++) {
            content[i] = data[i];
        }

        ofstream stream;
        std::ofstream file(weights_file, std::ios::out);
        file.write(content, full_size);
        Utils::editFlagZero(flagPath);
        server_logger.log("Done writting to the weight file", "info");
        std::string path = weights_file + to_string(count) + ".txt";
        count++;
        std::ofstream file1(path, std::ios::out | std::ios::out);
        file1.write(content, full_size);

        while (true) {
            sleep(DELAY);
            if (Utils::checkFlag(flagPath) == "1") {
                ifstream infile{weights_file};
                server_logger.log("Weight file opened", "info");
                string file_contents{istreambuf_iterator<char>(infile), istreambuf_iterator<char>()};
                server_logger.log("Reading weights", "info");
                write(sockfd, file_contents.c_str(), file_contents.size());
                server_logger.log("Writing to the socket", "info");
                break;

            } else if (Utils::checkFlag(flagPath) == "STOP") {
                isTrue = true;
                break;
            }
        }
        server_logger.log("Round completed", "info");
        sleep(DELAY);

        if (isTrue == true) {
            break;
        }
    }
    close(sockfd);
    return 0;
}

bool JasmineGraphServer::mergeFiles(std::string host, int port, int dataPort, std::string trainingArgs, int iteration,
                                    string partCount) {
    bool result = true;
    int sockfd;
    char data[FED_DATA_LENGTH + 1];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        server_logger.log("Cannot create socket", "error");
        return 0;
    }

    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        server_logger.log("ERROR, can not find the host", "error");
        return 0;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        server_logger.log("ERROR connecting", "error");
        // TODO::exit
        return 0;
    }

    bzero(data, FED_DATA_LENGTH);
    write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());
    server_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, FED_DATA_LENGTH);
    read(sockfd, data, FED_DATA_LENGTH);
    string response = (data);

    response = Utils::trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        server_logger.log("Received merge : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        string server_host = Utils::getJasmineGraphProperty("org.jasminegraph.server.host");
        write(sockfd, server_host.c_str(), server_host.size());
        server_logger.log("Sent merge : " + server_host, "info");

        bzero(data, FED_DATA_LENGTH + 1);
        read(sockfd, data, FED_DATA_LENGTH);
        response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        }

        write(sockfd, JasmineGraphInstanceProtocol::MERGE_FILES.c_str(),
              JasmineGraphInstanceProtocol::MERGE_FILES.size());
        server_logger.log("Sent merge : " + JasmineGraphInstanceProtocol::MERGE_FILES, "info");
        bzero(data, FED_DATA_LENGTH);
        read(sockfd, data, FED_DATA_LENGTH);
        response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");
        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.log("Received merge : " + JasmineGraphInstanceProtocol::OK, "info");
            write(sockfd, (trainingArgs).c_str(), (trainingArgs).size());
            server_logger.log("Sent merge : training args " + trainingArgs, "info");
            bzero(data, FED_DATA_LENGTH);
            return 0;
        }
    } else {
        server_logger.log("There was an error in the invoking training process and the response is :: " + response,
                          "error");
    }

    close(sockfd);
    return 0;
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
        server_logger.log("Cannot create socket", "error");
        return 0;
    }

    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        server_logger.log("ERROR, can not find the host", "error");
        return 0;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        server_logger.log("ERROR connecting", "error");
        // TODO::exit
        return 0;
    }

    bzero(data, FED_DATA_LENGTH);
    write(sockfd, JasmineGraphInstanceProtocol::INITIATE_TRAIN.c_str(),
          JasmineGraphInstanceProtocol::INITIATE_TRAIN.size());
    bzero(data, FED_DATA_LENGTH);
    read(sockfd, data, FED_DATA_LENGTH);
    std::string command = trainingArgs;
    write(sockfd, command.c_str(), command.size());
    close(sockfd);
    return 0;
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
        std::cout << "Cannot create socket" << std::endl;
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
        std::cout << "ERROR connecting" << std::endl;
        // TODO::exit
        return;
    }

    bzero(data, 301);
    int result_wr =
        write(sockfd, JasmineGraphInstanceProtocol::EGONET.c_str(), JasmineGraphInstanceProtocol::EGONET.size());
    if (result_wr < 0) {
        server_logger.error("Error writing to socket");
    }

    server_logger.info("Sent : " + JasmineGraphInstanceProtocol::EGONET);
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);
    response = Utils::trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
        server_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
    } else {
        server_logger.error("Error reading from socket");
    }

    result_wr = write(sockfd, graphID.c_str(), graphID.size());

    if (result_wr < 0) {
        server_logger.error("Error writing to socket");
    }
    server_logger.info("Sent : Graph ID " + graphID);

    bzero(data, 301);
    read(sockfd, data, 300);
    response = (data);
    response = Utils::trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
        server_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
    } else {
        server_logger.error("Error reading from socket");
    }

    int partitionID = stoi(partition);

    result_wr = write(sockfd, std::to_string(partitionID).c_str(), std::to_string(partitionID).size());

    if (result_wr < 0) {
        server_logger.error("Error writing to socket");
    }

    server_logger.info("Sent : Partition ID " + std::to_string(partitionID));

    bzero(data, 301);
    read(sockfd, data, 300);
    response = (data);
    response = Utils::trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
        server_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
    } else {
        server_logger.error("Error reading from socket");
    }

    result_wr = write(sockfd, workerList.c_str(), workerList.size());

    if (result_wr < 0) {
        server_logger.error("Error writing to socket");
    }

    server_logger.info("Sent : Host List ");

    if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
        server_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
    } else {
        server_logger.error("Error reading from socket");
    }
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
    server_logger.error("Worker list " + workerList);

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
            std::cout << "Cannot create socket" << std::endl;
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
            std::cout << "ERROR connecting" << std::endl;
            // TODO::exit
            return;
        }

        bzero(data, 301);
        int result_wr = write(sockfd, JasmineGraphInstanceProtocol::PAGE_RANK.c_str(),
                              JasmineGraphInstanceProtocol::PAGE_RANK.size());
        if (result_wr < 0) {
            server_logger.error("Error writing to socket");
        }

        server_logger.info("Sent : " + JasmineGraphInstanceProtocol::PAGE_RANK);
        bzero(data, 301);
        read(sockfd, data, 300);
        string response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
        } else {
            server_logger.error("Error reading from socket");
        }

        result_wr = write(sockfd, graphID.c_str(), graphID.size());

        if (result_wr < 0) {
            server_logger.error("Error writing to socket");
        }
        server_logger.info("Sent : Graph ID " + graphID);

        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
        } else {
            server_logger.error("Error reading from socket");
        }

        int partitionID = stoi(partition);

        result_wr = write(sockfd, std::to_string(partitionID).c_str(), std::to_string(partitionID).size());

        if (result_wr < 0) {
            server_logger.error("Error writing to socket");
        }

        server_logger.info("Sent : Partition ID " + std::to_string(partitionID));

        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
        } else {
            server_logger.error("Error reading from socket");
        }

        result_wr = write(sockfd, workerList.c_str(), workerList.size());

        if (result_wr < 0) {
            server_logger.error("Error writing to socket");
        }

        server_logger.info("Sent : Host List ");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
        } else {
            server_logger.error("Error reading from socket");
        }

        long graphVertexCount = JasmineGraphServer::getGraphVertexCount(graphID);
        result_wr = write(sockfd, std::to_string(graphVertexCount).c_str(), std::to_string(graphVertexCount).size());

        if (result_wr < 0) {
            server_logger.error("Error writing to socket");
        }

        server_logger.info("graph vertex count: " + std::to_string(graphVertexCount));
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
        } else {
            server_logger.error("Error reading from socket");
        }

        result_wr = write(sockfd, std::to_string(alpha).c_str(), std::to_string(alpha).size());

        if (result_wr < 0) {
            server_logger.error("Error writing to socket");
        }

        server_logger.info("page rank alpha value sent : " + std::to_string(alpha));
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
        } else {
            server_logger.error("Error reading from socket");
        }

        result_wr = write(sockfd, std::to_string(iterations).c_str(), std::to_string(iterations).size());

        if (result_wr < 0) {
            server_logger.error("Error writing to socket");
        }

        server_logger.info("page rank iterations value sent : " + std::to_string(iterations));
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = Utils::trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);
        } else {
            server_logger.error("Error reading from socket");
        }
    }
}
