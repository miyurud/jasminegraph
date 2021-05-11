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

#include <iostream>
#include <map>
#include "JasmineGraphServer.h"
#include "JasmineGraphInstance.h"
#include "../util/Utils.h"
#include "../partitioner/local/MetisPartitioner.h"

#include "JasmineGraphInstanceProtocol.h"
#include "../util/logger/Logger.h"
#include "../query/algorithms/entityresolution/EntityResolver.hpp"

Logger server_logger;

static map<string, string> hostIDMap;
static std::vector<JasmineGraphServer::workers> hostWorkerMap;
static map<string, pair<int, int>> hostPortMap;

void *runfrontend(void *dummyPt) {
    JasmineGraphServer *refToServer = (JasmineGraphServer *) dummyPt;
    refToServer->frontend = new JasmineGraphFrontEnd(refToServer->sqlite, refToServer->masterHost);
    refToServer->frontend->setServer(refToServer);
    refToServer->frontend->run();
}

void *runbackend(void *dummyPt) {
    JasmineGraphServer *refToServer = (JasmineGraphServer *) dummyPt;
    refToServer->backend = new JasmineGraphBackend(refToServer->sqlite, refToServer->numberOfWorkers);
    refToServer->backend->run();
}


JasmineGraphServer::JasmineGraphServer() {

}

JasmineGraphServer::~JasmineGraphServer() {
    puts("Freeing up server resources.");
    sqlite.finalize();
}

int JasmineGraphServer::run(std::string profile, std::string masterIp, int numberofWorkers, std::string workerIps, std::string enableNmon) {
    server_logger.log("Running the server...", "info");
    Utils utils;
    std::vector<int> masterPortVector;

    this->sqlite = *new SQLiteDBInterface();
    this->sqlite.init();
    this->performanceSqlite = *new PerformanceSQLiteDBInterface();
    this->performanceSqlite.init();
    if (masterIp.empty()) {
        this->masterHost = utils.getJasmineGraphProperty("org.jasminegraph.server.host");
    } else {
        this->masterHost = masterIp;
    }
    this->profile = profile;
    this->numberOfWorkers = numberofWorkers;
    this->workerHosts = workerIps;
    this->enableNmon = enableNmon;
    init();
    masterPortVector.push_back(Conts::JASMINEGRAPH_FRONTEND_PORT);
    addInstanceDetailsToPerformanceDB(masterHost,masterPortVector,"true");
    updateOperationalGraphList();
    start_workers();
    sleep(2);
    waitForAcknowledgement(numberofWorkers);
    resolveOperationalGraphs();
    return 0;
}

bool JasmineGraphServer::isRunning() {
    return true;
}

void JasmineGraphServer::init() {
    Utils utils;

    pthread_t frontendthread;
    pthread_t backendthread;
    pthread_create(&frontendthread, NULL, runfrontend, this);
    pthread_create(&backendthread, NULL, runbackend, this);
}

void JasmineGraphServer::start_workers() {
    Utils utils;
    int hostListModeNWorkers = 0;
    int numberOfWorkersPerHost;
    std::vector<std::string> hostsList;
    std::string nWorkers;
    if (profile == "native") {
        hostsList = utils.getHostListFromProperties();
        if ((this->numberOfWorkers) == -1) {
            nWorkers = utils.getJasmineGraphProperty("org.jasminegraph.server.nworkers");
        }
        enableNmon = utils.getJasmineGraphProperty("org.jasminegraph.server.enable.nmon");
    } else if (profile == "docker") {
        hostsList = getWorkerVector(workerHosts);
    }

    if(hostsList.size() == 0) {
        server_logger.log("At least one host needs to be specified", "error");
        exit(-1);
    }

    sqlite.runUpdate("DELETE FROM host");

    std::vector<std::string>::iterator it;
    it = hostsList.begin();
    std::string hostString = "";
    std::string sqlString = "INSERT INTO host (idhost,name,ip,is_public) VALUES ";
    int counter = 0;

    for (it = hostsList.begin(); it < hostsList.end(); it++) {
        hostString = "(" + std::to_string(counter) + ", '" + (*it) + "', '" + (*it) + "', 'false'),";
        counter++;
    }

    hostString = hostString.substr(0, hostString.length() - 1);
    sqlString = sqlString + hostString;
    this->sqlite.runInsert(sqlString);

    int workerPort = Conts::JASMINEGRAPH_INSTANCE_PORT;
    int workerDataPort = Conts::JASMINEGRAPH_INSTANCE_DATA_PORT;

    if (this->numberOfWorkers == -1) {
        if (utils.is_number(nWorkers)) {
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

    std::string schedulerEnabled = utils.getJasmineGraphProperty("org.jasminegraph.scheduler.enabled");

    if (schedulerEnabled == "true") {
        backupPerformanceDB();
        clearPerformanceDB();
    }

    sqlite.runUpdate("DELETE FROM worker");

    string valuesString;
    string sqlStatement = "INSERT INTO worker (idworker,host_idhost,name,ip,user,is_public,server_port,server_data_port) VALUES ";
    int workerIDCounter = 0;
    it = hostsList.begin();

    for (it = hostsList.begin(); it < hostsList.end(); it++) {
        std::string hostName = *it;
        int portCount = 0;
        std::string hostID = Utils::getHostID(hostName, this->sqlite);
        std::vector<int> portVector = workerPortsMap[hostName];
        std::vector<int> dataPortVector = workerDataPortsMap[hostName];

        while (portCount < numberOfWorkersPerHost) {
            portVector.push_back(workerPort);
            dataPortVector.push_back(workerDataPort);
            hostWorkerMap.push_back({*it, workerPort, workerDataPort});
            hostPortMap.insert((pair<string, pair<int, int>>(*it, make_pair(workerPort, workerDataPort))));
            portCount++;
            //ToDO: Here for the moment we use host name as the IP address as the third parameter.
            //ToDO: We also keep user as empty string
            string user = "";
            string ip = hostName;
            string is_public = "false";
            valuesString += "(" + std::to_string(workerIDCounter) + ", " + hostID + ", \"" + hostName +
                    "\", \"" + ip + "\",\"" + user + "\", '" + is_public
                    + "',\""+ std::to_string(workerPort) +"\", \""+ std::to_string(workerDataPort) + "\"),";
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
            string user = "";
            string ip = hostName;
            string is_public = "false";
            valuesString += "(" + std::to_string(workerIDCounter) + ", " + hostID + ", \"" + hostName +
                            "\", \"" + ip + "\",\"" + user + "\", '" + is_public
                            + "',\""+ std::to_string(workerPort) +"\", \""+ std::to_string(workerDataPort) + "\"),";
            workerPort = workerPort + 2;
            workerDataPort = workerDataPort + 2;
            workerIDCounter++;
        }

        valuesString = valuesString.substr(0, valuesString.length() -1);
        sqlStatement = sqlStatement + valuesString;
        this->sqlite.runInsert(sqlStatement);

        workerPortsMap[hostName] = portVector;
        workerDataPortsMap[hostName] = dataPortVector;

    }

    Utils::assignPartitionsToWorkers(numberOfWorkers, this->sqlite);

    int hostListSize = hostsList.size();
    std::vector<std::string>::iterator hostListIterator;
    hostListIterator = hostsList.begin();


    std::thread *myThreads = new std::thread[hostListSize];
    int count = 0;
    server_logger.log("Starting threads for workers", "info");
    for (hostListIterator = hostsList.begin(); hostListIterator < hostsList.end(); hostListIterator++) {
        std::string host = *hostListIterator;
        addHostsToMetaDB(host, workerPortsMap[host],workerDataPortsMap[host]);
        addInstanceDetailsToPerformanceDB(host,workerPortsMap[host],"false");
        myThreads[count] = std::thread(startRemoteWorkers,workerPortsMap[host],workerDataPortsMap[host], host, profile,
                masterHost, enableNmon);
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
        sleep(2); // Sleep for two seconds
        std::string selectQuery = "select idworker from worker where status='started'";
        std::vector<vector<pair<string, string>>> output = this->sqlite.runSelect(selectQuery);
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
    Utils utils;
    std::string executableFile;
    std::string workerPath = utils.getJasmineGraphProperty("org.jasminegraph.worker.path");
    std::string artifactPath = utils.getJasmineGraphProperty("org.jasminegraph.artifact.path");
    std::string instanceDataFolder = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    std::string aggregateDataFolder = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.aggregatefolder");
    std::string nmonFileLocation = utils.getJasmineGraphProperty("org.jasminegraph.server.nmon.file.location");
    std::string jasmineGraphExecutableName = Conts::JASMINEGRAPH_EXECUTABLE;
    server_logger.log("###MASTER#### Starting remote workers for profile " + profile, "info");
    if (hasEnding(workerPath,"/")) {
        executableFile = workerPath + jasmineGraphExecutableName;
    } else {
        executableFile = workerPath + "/" + jasmineGraphExecutableName;
    }
    std::string serverStartScript;
    char buffer[128];
    std::string result = "";

    if (artifactPath.empty() || artifactPath.find_first_not_of(' ') == artifactPath.npos) {
        artifactPath = utils.getJasmineGraphHome();
    }

    if (profile == "native") {
        copyArtifactsToWorkers(workerPath, artifactPath, host);
        for (int i =0 ; i < workerPortsVector.size() ; i++) {
            if (host.find("localhost") != std::string::npos) {
                serverStartScript = executableFile + " native 2 " + host + " " + masterHost + " " +
                        std::to_string(workerPortsVector.at(i)) + " " + std::to_string(workerDataPortsVector.at(i)) + " " + enableNmon;
            } else {
                serverStartScript =
                        "ssh -p 22 " + host + " " + executableFile + " native 2 " + host + " " + masterHost + " " +
                        std::to_string(workerPortsVector.at(i)) +
                        " " + std::to_string(workerDataPortsVector.at(i)) + " " + enableNmon;
            }
            popen(serverStartScript.c_str(),"r");
        }
    } else if (profile == "docker") {
        for (int i =0 ; i < workerPortsVector.size() ; i++) {
            if (masterHost == host || host == "localhost") {
                serverStartScript = "docker run -v" + instanceDataFolder + ":" + instanceDataFolder +
                                    " -v " + aggregateDataFolder + ":" + aggregateDataFolder +
                                    " -v " + nmonFileLocation + ":" + nmonFileLocation + " -p " +
                                    std::to_string(workerPortsVector.at(i)) + ":" +
                                    std::to_string(workerPortsVector.at(i)) + " -p " +
                                    std::to_string(workerDataPortsVector.at(i)) + ":" +
                                    std::to_string(workerDataPortsVector.at(i)) + " jasminegraph:latest --MODE 2 --HOST_NAME " + host +
                                    " --MASTERIP " + masterHost + " --SERVER_PORT " +
                                    std::to_string(workerPortsVector.at(i)) + " --SERVER_DATA_PORT " +
                                    std::to_string(workerDataPortsVector.at(i)) + " --ENABLE_NMON " + enableNmon;
            } else {
                serverStartScript = "docker -H ssh://" + host + " run -v " + instanceDataFolder + ":" + instanceDataFolder +
                                    " -v " + aggregateDataFolder + ":" + aggregateDataFolder +
                                    " -v "+ nmonFileLocation + ":" + nmonFileLocation+ " -p " +
                                    std::to_string(workerPortsVector.at(i)) + ":" +
                                    std::to_string(workerPortsVector.at(i)) + " -p " +
                                    std::to_string(workerDataPortsVector.at(i)) + ":" +
                                    std::to_string(workerDataPortsVector.at(i)) + " jasminegraph:latest --MODE 2 --HOST_NAME " + host +
                                    " --MASTERIP " + masterHost + " --SERVER_PORT " +
                                    std::to_string(workerPortsVector.at(i)) + " --SERVER_DATA_PORT " +
                                    std::to_string(workerDataPortsVector.at(i)) + " --ENABLE_NMON " + enableNmon;
            }
            server_logger.log(serverStartScript, "info");
            popen(serverStartScript.c_str(),"r");
        }
    }
}

bool JasmineGraphServer::spawnNewWorker(string host, string port, string dataPort, string profile, string masterHost, string enableNmon) {
    SQLiteDBInterface refToSqlite = *new SQLiteDBInterface();
    refToSqlite.init();
    string selectHostSQL = "SELECT idhost from host where name='" + host + "'";
    string selectWorkerSQL = "SELECT idworker from worker where server_port = '" + port + "' or server_data_port = '" + dataPort + "'";
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
        std::string hostInsertString = "INSERT INTO host (idhost,name,ip,is_public) VALUES ('" + std::to_string(hostId) + "','" +
                host + "','" + host + "','false')";

        refToSqlite.runInsert(hostInsertString);

        idHost = to_string(hostId);
    }

    string maxWorkerIDSQL = "select max(idworker) from worker";
    std::vector<vector<pair<string, string>>> selectMaxWorkerIdOutput = refToSqlite.runSelect(maxWorkerIDSQL);
    string maxIdWorker = selectMaxWorkerIdOutput[0][0].second;
    int maxWorkerId = atoi(maxIdWorker.c_str());
    maxWorkerId++;
    string workerInsertSqlStatement = "INSERT INTO worker (idworker,host_idhost,name,ip,user,is_public,server_port,server_data_port) VALUES ('" +
            to_string(maxWorkerId) + "','" + idHost + "','" + host + "','" + host + "','','false','" + port + "','" + dataPort + "')";

    refToSqlite.runInsert(workerInsertSqlStatement);

    std::vector<int> workerPortsVector;
    std::vector<int> workerDataPortsVector;

    workerPortsVector.push_back(atoi(port.c_str()));
    workerDataPortsVector.push_back(atoi(dataPort.c_str()));

    startRemoteWorkers(workerPortsVector,workerDataPortsVector,host,profile,masterHost,enableNmon);

    return true;
}

void JasmineGraphServer::resolveOperationalGraphs(){
    string sqlStatement = "SELECT partition_graph_idgraph,partition_idpartition,worker_idworker FROM worker_has_partition ORDER BY worker_idworker";
    std::vector<vector<pair<string, string>>> output = sqlite.runSelect(sqlStatement);
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
    output = sqlite.runSelect(sqlStatement);

    Utils utils;
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
        char data[300];
        bool loop = false;
        socklen_t len;
        struct sockaddr_in serv_addr;
        struct hostent *server;

        sockfd = socket(AF_INET, SOCK_STREAM, 0);

        if (sockfd < 0) {
            std::cerr << "Cannot accept connection" << std::endl;
        }

        if (host.find('@') != std::string::npos) {
            host = utils.split(host, '@')[1];
        }

        server = gethostbyname(host.c_str());
        if (server == NULL) {
            std::cerr << "ERROR, no host named " << server << std::endl;
        }

        bzero((char *) &serv_addr, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        bcopy((char *) server->h_addr,
              (char *) &serv_addr.sin_addr.s_addr,
              server->h_length);
        serv_addr.sin_port = htons(workerPort);
        if (connect(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
            std::cerr << "ERROR connecting" << std::endl;
            //TODO::exit
        }

        bzero(data, 301);
        int result_wr = write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

        if(result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        string response = (data);

        response = utils.trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
            string server_host = utils.getJasmineGraphProperty("org.jasminegraph.server.host");
            result_wr = write(sockfd, server_host.c_str(), server_host.size());

            if(result_wr < 0) {
                server_logger.log("Error writing to socket", "error");
            }

            server_logger.log("Sent : " + server_host, "info");

            result_wr = write(sockfd, JasmineGraphInstanceProtocol::INITIATE_FRAGMENT_RESOLUTION.c_str(),
                  JasmineGraphInstanceProtocol::INITIATE_FRAGMENT_RESOLUTION.size());

            if(result_wr < 0) {
                server_logger.log("Error writing to socket", "error");
            }

            server_logger.log("Sent : " + JasmineGraphInstanceProtocol::INITIATE_FRAGMENT_RESOLUTION, "info");
            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");

            if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
                std::vector<string> partitionList = partitionAggregatedMap[workerID];

                for (std::vector<string>::iterator x = partitionList.begin(); x != partitionList.end(); ++x) {
                    string partitionsList = x->c_str();
                    result_wr = write(sockfd, partitionsList.c_str(), partitionsList.size());

                    if(result_wr < 0) {
                        server_logger.log("Error writing to socket", "error");
                    }

                    server_logger.log("Sent : " + partitionsList, "info");
                    bzero(data, 301);
                    read(sockfd, data, 300);
                    response = (data);
                    response = utils.trim_copy(response, " \f\n\r\t\v");
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

                    if(result_wr < 0) {
                        server_logger.log("Error writing to socket", "error");
                    }

                    server_logger.log("Sent : " + JasmineGraphInstanceProtocol::FRAGMENT_RESOLUTION_DONE, "info");

                    bzero(data, 301);
                    read(sockfd, data, 300);
                    response = (data);
                    response = utils.trim_copy(response, " \f\n\r\t\v");
                    server_logger.log("Received : " + response, "info");

                    if(response.compare("") != 0) {
                        std::vector<string> listOfOitems = utils.split(response, ',');
                        for(std::vector<string>::iterator it = listOfOitems.begin(); it != listOfOitems.end(); it++) {
                            graphIDsFromWorkersSet.insert(atoi(it->c_str()));
                        }
                    }
                }
            }
        }
    }
    sqlStatement = "SELECT idgraph FROM graph";
    std::vector<vector<pair<string, string>>> output2 = sqlite.runSelect(sqlStatement);
    std::set<int> graphIDsFromMetDBSet;
    for (std::vector<vector<pair<string, string>>>::iterator i = output2.begin(); i != output2.end(); ++i) {
        std::vector<pair<string, string>>::iterator j = (i->begin());
        graphIDsFromMetDBSet.insert(atoi(j->second.c_str()));
    }

    for(std::set<int>::iterator itr = graphIDsFromWorkersSet.begin(); itr != graphIDsFromWorkersSet.end(); itr++){
        if(graphIDsFromMetDBSet.find(*itr) == graphIDsFromMetDBSet.end()){
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
    //Define threads for each host
    std::thread *deleteThreads = new std::thread[hostPortMap.size()];
    //Iterate through all hosts
    for (std::map<string, pair<int, int>>::iterator it = hostPortMap.begin(); it != hostPortMap.end(); it++) {
        //Fetch hostname and port
        string hostname = it -> first;
        int port = (it -> second).first;
        //Initialize threads for host
        //Each thread runs the service to remove the given graph ID fragments in their datafolders
        deleteThreads[count++] = std::thread(removeFragmentThroughService, hostname, port, to_string(graphID), this -> masterHost);
        sleep(1);
        server_logger.log("Deleted graph fragments of graph ID " + to_string(graphID), "info");
    }

    for (int threadCount = 0; threadCount < count; threadCount++) {
        if(deleteThreads[threadCount].joinable()) {
            deleteThreads[threadCount].join();
        }
        std::cout << "Thread [A]: " << threadCount << " joined" << std::endl;
    }
}

int JasmineGraphServer::shutdown_workers() {
    std::cout << "Shutting the workers down" << std::endl;
    std::vector<workers, std::allocator<workers>>::iterator mapIterator;
    for (mapIterator = hostWorkerMap.begin(); mapIterator < hostWorkerMap.end(); mapIterator++) {
        workers worker = *mapIterator;
        Utils utils;
        bool result = true;
        std::cout << pthread_self() << " host : " << worker.hostname << " port : " << worker.port << " DPort : "
                  << worker.dataPort << std::endl;
        int sockfd;
        char data[300];
        bool loop = false;
        socklen_t len;
        struct sockaddr_in serv_addr;
        struct hostent *server;

        sockfd = socket(AF_INET, SOCK_STREAM, 0);

        if (sockfd < 0) {
            std::cerr << "Cannot accept connection" << std::endl;
            return 0;
        }

        std::string host = worker.hostname;
        int port = worker.port;

        if (worker.hostname.find('@') != std::string::npos) {
            host = utils.split(host, '@')[1];
        }

        server = gethostbyname(host.c_str());
        if (server == NULL) {
            std::cerr << "ERROR, no host named " << server << std::endl;
        }

        bzero((char *) &serv_addr, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        bcopy((char *) server->h_addr,
              (char *) &serv_addr.sin_addr.s_addr,
              server->h_length);
        serv_addr.sin_port = htons(port);
        if (connect(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
            std::cerr << "ERROR connecting" << std::endl;
            //TODO::exit
        }

        bzero(data, 301);
        write(sockfd, JasmineGraphInstanceProtocol::SHUTDOWN.c_str(), JasmineGraphInstanceProtocol::SHUTDOWN.size());
        server_logger.log("Sent : " + JasmineGraphInstanceProtocol::SHUTDOWN, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        string response = (data);

        response = utils.trim_copy(response, " \f\n\r\t\v");
        server_logger.log("Response : " + response, "info");
    }
}

void JasmineGraphServer::uploadGraphLocally(int graphID, const string graphType, vector<std::map<int,string>> fullFileList,
        std::string masterIP) {
    server_logger.log("Uploading the graph locally..","info");
    std::map<int, string> partitionFileList = fullFileList[0];
    std::map<int, string> centralStoreFileList = fullFileList[1];
    std::map<int, string> centralStoreDuplFileList = fullFileList[2];
    std::map<int, string> compositeCentralStoreFileList = fullFileList[5];
    std::map<int, string> attributeFileList;
    std::map<int, string> centralStoreAttributeFileList;
    Utils utils;
    if (masterHost.empty()) {
        masterHost = utils.getJasmineGraphProperty("org.jasminegraph.server.host");;
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
                workerThreads[count] = std::thread(batchUploadCompositeCentralstoreFile, worker.hostname, worker.port, worker.dataPort,
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
                workerThreads[count] = std::thread(batchUploadCentralAttributeFile, worker.hostname, worker.port,
                                                   worker.dataPort, graphID, centralStoreAttributeFileList[file_count], masterHost);
                count++;
                sleep(1);
            }
            assignPartitionToWorker(partitionFileName,graphID,worker.hostname,worker.port,worker.dataPort);
            file_count++;
        }
    }

    std::cout << "Total number of threads to join : " << count << std::endl;
    for (int threadCount = 0; threadCount < count; threadCount++) {
        if(workerThreads[threadCount].joinable()) {
            workerThreads[threadCount].join();
        }
        std::cout << "Thread [B]: " << threadCount << " joined" << std::endl;
    }

    std::time_t time = chrono::system_clock::to_time_t(chrono::system_clock::now());
    string uploadEndTime = ctime(&time);

    //The following function updates the 'worker_has_partition' table and 'graph' table only
    updateMetaDB(hostWorkerMap, partitionFileList, graphID, uploadEndTime);

}

void JasmineGraphServer::assignPartitionToWorker(std::string fileName, int graphId, std::string workerHost, int workerPort, int workerDataPort) {
    SQLiteDBInterface refToSqlite = *new SQLiteDBInterface();
    refToSqlite.init();
    size_t lastindex = fileName.find_last_of(".");
    string rawname = fileName.substr(0, lastindex);
    string partitionID = rawname.substr(rawname.find_last_of("_") + 1);

    std::string workerSearchQuery = "select idworker from worker where ip='" + workerHost + "' and server_port='" + std::to_string(workerPort)+ "' and server_data_port='" + std::to_string(workerDataPort) + "'";

    std::vector<vector<pair<string, string>>> results = refToSqlite.runSelect(workerSearchQuery);

    std::string workerID = results[0][0].second;

    std::string partitionToWorkerQuery = "insert into worker_has_partition (partition_idpartition, partition_graph_idgraph, worker_idworker) values "
                                         "('"+ partitionID + "','" +std::to_string(graphId) + "','" + workerID + "')";

    refToSqlite.runInsert(partitionToWorkerQuery);
}

bool JasmineGraphServer::batchUploadFile(std::string host, int port, int dataPort, int graphID, std::string filePath,
        std::string masterIP) {
    Utils utils;
    bool result = true;
    std::cout << pthread_self() << " host : " << host << " port : " << port << " DPort : " << dataPort << std::endl;
    int sockfd;
    char data[300];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot accept connection" << std::endl;
        return 0;
    }

    if (host.find('@') != std::string::npos) {
        host = utils.split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        std::cerr << "ERROR, no host named " << server << std::endl;
    }

    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *) server->h_addr,
          (char *) &serv_addr.sin_addr.s_addr,
          server->h_length);
    serv_addr.sin_port = htons(port);
    if (connect(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
        //TODO::exit
    }

    bzero(data, 301);
    int result_wr = write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if(result_wr < 0) {
        server_logger.log("Error writing to socket", "error");
    }

    server_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        server_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if(result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + masterIP, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            server_logger.log("Received : " + response, "error");
        }

        result_wr = write(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD.c_str(),
              JasmineGraphInstanceProtocol::BATCH_UPLOAD.size());

        if(result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            //std::cout << graphID << std::endl;
            result_wr = write(sockfd, std::to_string(graphID).c_str(), std::to_string(graphID).size());

            if(result_wr < 0) {
                server_logger.log("Error writing to socket", "error");
            }

            server_logger.log("Sent : Graph ID " + std::to_string(graphID), "info");
            std::string fileName = utils.getFileName(filePath);
            int fileSize = utils.getFileSize(filePath);
            std::string fileLength = to_string(fileSize);

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");

            if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_NAME) == 0) {
                server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");
                result_wr = write(sockfd, fileName.c_str(), fileName.size());

                if(result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : File name " + fileName, "info");
                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);
                //response = utils.trim_copy(response, " \f\n\r\t\v");

                if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_LEN) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
                    result_wr = write(sockfd, fileLength.c_str(), fileLength.size());

                    if(result_wr < 0) {
                        server_logger.log("Error writing to socket", "error");
                    }

                    server_logger.log("Sent : File length in bytes " + fileLength, "info");
                    bzero(data, 301);
                    read(sockfd, data, 300);
                    response = (data);
                    //response = utils.trim_copy(response, " \f\n\r\t\v");

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

                if(result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : " + JasmineGraphInstanceProtocol::FILE_RECV_CHK, "info");
                server_logger.log("Checking if file is received", "info");
                bzero(data, 301);
                read(sockfd, data, 300);
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
            //Next we wait till the batch upload completes
            while (true) {
                result_wr = write(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.c_str(),
                      JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.size());

                if(result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                bzero(data, 301);
                read(sockfd, data, 300);
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
    Utils utils;
    int sockfd;
    char data[300];
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot accept connection" << std::endl;
        return 0;
    }

    if (host.find('@') != std::string::npos) {
        host = utils.split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        std::cerr << "ERROR, no host named " << server << std::endl;
    }

    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *) server->h_addr,
          (char *) &serv_addr.sin_addr.s_addr,
          server->h_length);
    serv_addr.sin_port = htons(port);
    if (connect(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
    }

    bzero(data, 301);
    int result_wr = write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if(result_wr < 0) {
        server_logger.log("Error writing to socket", "error");
    }

    server_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        server_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if(result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + masterIP, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            server_logger.log("Received : " + response, "error");
        }

        result_wr = write(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_CENTRAL.c_str(),
              JasmineGraphInstanceProtocol::BATCH_UPLOAD_CENTRAL.size());

        if(result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CENTRAL, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");

            result_wr = write(sockfd, std::to_string(graphID).c_str(), std::to_string(graphID).size());

            if(result_wr < 0) {
                server_logger.log("Error writing to socket", "error");
            }

            server_logger.log("Sent : Graph ID " + std::to_string(graphID), "info");
            std::string fileName = utils.getFileName(filePath);
            int fileSize = utils.getFileSize(filePath);
            std::string fileLength = to_string(fileSize);

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");

            if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_NAME) == 0) {
                server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");

                result_wr = write(sockfd, fileName.c_str(), fileName.size());

                if(result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : File name " + fileName, "info");
                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);
                //response = utils.trim_copy(response, " \f\n\r\t\v");

                if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_LEN) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
                    result_wr = write(sockfd, fileLength.c_str(), fileLength.size());

                    if(result_wr < 0) {
                        server_logger.log("Error writing to socket", "error");
                    }

                    server_logger.log("Sent : File length in bytes " + fileLength, "info");
                    bzero(data, 301);
                    read(sockfd, data, 300);
                    response = (data);
                    //response = utils.trim_copy(response, " \f\n\r\t\v");

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

                if(result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : " + JasmineGraphInstanceProtocol::FILE_RECV_CHK, "info");
                server_logger.log("Checking if file is received", "info");
                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);
                //response = utils.trim_copy(response, " \f\n\r\t\v");

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

            //Next we wait till the batch upload completes
            while (true) {
                result_wr = write(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.c_str(),
                      JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.size());

                if(result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                bzero(data, 301);
                read(sockfd, data, 300);
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
    Utils utils;
    char buffer[128];
    std::string result = "SUCCESS";
    std::string copyCommand;
    std::string aggregatorFilePath = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.aggregatefolder");

    DIR* dir = opendir(aggregatorFilePath.c_str());

    if (dir) {
        closedir(dir);
    } else {
        std::string createDirCommand = "mkdir -p " + aggregatorFilePath;
        FILE *createDirInput = popen(createDirCommand.c_str(),"r");
        pclose(createDirInput);
    }

    copyCommand = "cp "+filePath+ " " + aggregatorFilePath;

    FILE *copyInput = popen(copyCommand.c_str(),"r");

    if (copyInput) {
        // read the input
        while (!feof(copyInput)) {
            if (fgets(buffer, 128, copyInput) != NULL) {
                result.append(buffer);
            }
        }
        if (!result.empty()) {
            std::cout<<result<< std::endl;
        }
        pclose(copyInput);
    }

    std::string fileName = utils.getFileName(filePath);

    std::string fullFileName = aggregatorFilePath + "/" + fileName;

}

bool JasmineGraphServer::batchUploadAttributeFile(std::string host, int port, int dataPort, int graphID,
                                                 std::string filePath, std::string masterIP) {
    Utils utils;
    int sockfd;
    char data[300];
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot accept connection" << std::endl;
        return 0;
    }

    if (host.find('@') != std::string::npos) {
        host = utils.split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        std::cerr << "ERROR, no host named " << server << std::endl;
    }

    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *) server->h_addr,
          (char *) &serv_addr.sin_addr.s_addr,
          server->h_length);
    serv_addr.sin_port = htons(port);
    if (connect(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
    }

    bzero(data, 301);
    int result_wr = write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if(result_wr < 0) {
        server_logger.log("Error writing to socket", "error");
    }

    server_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        server_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if(result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + masterIP, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            server_logger.log("Received : " + response, "error");
        }

        result_wr = write(sockfd, JasmineGraphInstanceProtocol::UPLOAD_RDF_ATTRIBUTES.c_str(),
              JasmineGraphInstanceProtocol::UPLOAD_RDF_ATTRIBUTES.size());

        if(result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + JasmineGraphInstanceProtocol::UPLOAD_RDF_ATTRIBUTES, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");
        //std::cout << response << std::endl;
        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            //std::cout << graphID << std::endl;
            result_wr = write(sockfd, std::to_string(graphID).c_str(), std::to_string(graphID).size());

            if(result_wr < 0) {
                server_logger.log("Error writing to socket", "error");
            }

            server_logger.log("Sent : Graph ID " + std::to_string(graphID), "info");
            std::string fileName = utils.getFileName(filePath);
            int fileSize = utils.getFileSize(filePath);
            std::string fileLength = to_string(fileSize);

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");

            if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_NAME) == 0) {
                server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");
                //std::cout << fileName << std::endl;
                result_wr = write(sockfd, fileName.c_str(), fileName.size());

                if(result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : File name " + fileName, "info");
                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);
                //response = utils.trim_copy(response, " \f\n\r\t\v");

                if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_LEN) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
                    result_wr = write(sockfd, fileLength.c_str(), fileLength.size());

                    if(result_wr < 0) {
                        server_logger.log("Error writing to socket", "error");
                    }

                    server_logger.log("Sent : File length in bytes " + fileLength, "info");
                    bzero(data, 301);
                    read(sockfd, data, 300);
                    response = (data);
                    //response = utils.trim_copy(response, " \f\n\r\t\v");

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

                if(result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : " + JasmineGraphInstanceProtocol::FILE_RECV_CHK, "info");
                server_logger.log("Checking if file is received", "info");
                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);
                //response = utils.trim_copy(response, " \f\n\r\t\v");

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

            //Next we wait till the batch upload completes
            while (true) {
                result_wr = write(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.c_str(),
                      JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.size());

                if(result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                bzero(data, 301);
                read(sockfd, data, 300);
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
    Utils utils;
    int sockfd;
    char data[300];
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot accept connection" << std::endl;
        return 0;
    }

    if (host.find('@') != std::string::npos) {
        host = utils.split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        std::cerr << "ERROR, no host named " << server << std::endl;
    }

    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *) server->h_addr,
          (char *) &serv_addr.sin_addr.s_addr,
          server->h_length);
    serv_addr.sin_port = htons(port);
    if (connect(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
    }

    bzero(data, 301);
    int result_wr = write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if(result_wr < 0) {
        server_logger.log("Error writing to socket", "error");
    }

    server_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        server_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if(result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + masterIP, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            server_logger.log("Received : " + response, "error");
        }

        result_wr = write(sockfd, JasmineGraphInstanceProtocol::UPLOAD_RDF_ATTRIBUTES_CENTRAL.c_str(),
              JasmineGraphInstanceProtocol::UPLOAD_RDF_ATTRIBUTES_CENTRAL.size());

        if(result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + JasmineGraphInstanceProtocol::UPLOAD_RDF_ATTRIBUTES_CENTRAL, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");
        //std::cout << response << std::endl;
        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            //std::cout << graphID << std::endl;
            result_wr = write(sockfd, std::to_string(graphID).c_str(), std::to_string(graphID).size());

            if(result_wr < 0) {
                server_logger.log("Error writing to socket", "error");
            }

            server_logger.log("Sent : Graph ID " + std::to_string(graphID), "info");
            std::string fileName = utils.getFileName(filePath);
            int fileSize = utils.getFileSize(filePath);
            std::string fileLength = to_string(fileSize);

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");

            if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_NAME) == 0) {
                server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");
                //std::cout << fileName << std::endl;
                result_wr = write(sockfd, fileName.c_str(), fileName.size());

                if(result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : File name " + fileName, "info");
                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);
                //response = utils.trim_copy(response, " \f\n\r\t\v");

                if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_LEN) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
                    result_wr = write(sockfd, fileLength.c_str(), fileLength.size());

                    if(result_wr < 0) {
                        server_logger.log("Error writing to socket", "error");
                    }

                    server_logger.log("Sent : File length in bytes " + fileLength, "info");
                    bzero(data, 301);
                    read(sockfd, data, 300);
                    response = (data);
                    //response = utils.trim_copy(response, " \f\n\r\t\v");

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

                if(result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : " + JasmineGraphInstanceProtocol::FILE_RECV_CHK, "info");
                server_logger.log("Checking if file is received", "info");
                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);
                //response = utils.trim_copy(response, " \f\n\r\t\v");

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

            //Next we wait till the batch upload completes
            while (true) {
                result_wr = write(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.c_str(),
                      JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.size());

                if(result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                bzero(data, 301);
                read(sockfd, data, 300);
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
    Utils utils;
    int sockfd;
    char data[300];
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot accept connection" << std::endl;
        return 0;
    }

    if (host.find('@') != std::string::npos) {
        host = utils.split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        std::cerr << "ERROR, no host named " << server << std::endl;
    }

    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *) server->h_addr,
          (char *) &serv_addr.sin_addr.s_addr,
          server->h_length);
    serv_addr.sin_port = htons(port);
    if (connect(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
    }

    bzero(data, 301);
    int result_wr = write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if(result_wr < 0) {
        server_logger.log("Error writing to socket", "error");
    }

    server_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        server_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if(result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + masterIP, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            server_logger.log("Received : " + response, "error");
        }

        result_wr = write(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_COMPOSITE_CENTRAL.c_str(),
                          JasmineGraphInstanceProtocol::BATCH_UPLOAD_COMPOSITE_CENTRAL.size());

        if(result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_COMPOSITE_CENTRAL, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");

            result_wr = write(sockfd, std::to_string(graphID).c_str(), std::to_string(graphID).size());

            if(result_wr < 0) {
                server_logger.log("Error writing to socket", "error");
            }

            server_logger.log("Sent : Graph ID " + std::to_string(graphID), "info");
            std::string fileName = utils.getFileName(filePath);
            int fileSize = utils.getFileSize(filePath);
            std::string fileLength = to_string(fileSize);

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");

            if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_NAME) == 0) {
                server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");

                result_wr = write(sockfd, fileName.c_str(), fileName.size());

                if(result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : File name " + fileName, "info");
                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);

                if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_LEN) == 0) {
                    server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
                    result_wr = write(sockfd, fileLength.c_str(), fileLength.size());

                    if(result_wr < 0) {
                        server_logger.log("Error writing to socket", "error");
                    }

                    server_logger.log("Sent : File length in bytes " + fileLength, "info");
                    bzero(data, 301);
                    read(sockfd, data, 300);
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

                if(result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : " + JasmineGraphInstanceProtocol::FILE_RECV_CHK, "info");
                server_logger.log("Checking if file is received", "info");
                bzero(data, 301);
                read(sockfd, data, 300);
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

            //Next we wait till the batch upload completes
            while (true) {
                result_wr = write(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.c_str(),
                                  JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.size());

                if(result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                bzero(data, 301);
                read(sockfd, data, 300);
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
    Utils utils;
    int sockfd;
    char data[300];
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot accept connection" << std::endl;
        return 0;
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        std::cerr << "ERROR, no host named " << server << std::endl;
        exit(0);
    }

    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *) server->h_addr,
          (char *) &serv_addr.sin_addr.s_addr,
          server->h_length);
    serv_addr.sin_port = htons(dataPort);
    if (connect(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting to port " << dataPort << std::endl;
    }

    int result_wr = write(sockfd, fileName.c_str(), fileName.size());

    if(result_wr < 0) {
        server_logger.log("Error writing to socket", "error");
    }

    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);
    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE) == 0) {
        std::cout << "Sending file " << filePath << " through port " << dataPort << std::endl;

        FILE *fp = fopen(filePath.c_str(), "r");
        if (fp == NULL) {
            //printf("Error opening file\n");
            close(sockfd);
            return 0;
        }

        for (;;) {
            unsigned char buff[1024] = {0};
            int nread = fread(buff, 1, 1024, fp);
            //printf("Bytes read %d \n", nread);

            /* If read was success, send data. */
            if (nread > 0) {
                //printf("Sending \n");
                write(sockfd, buff, nread);
            }

            if (nread < 1024) {
                if (feof(fp))
                    printf("End of file\n");
                if (ferror(fp))
                    printf("Error reading\n");
                break;
            }
        }

        fclose(fp);
        close(sockfd);
    }
}

void JasmineGraphServer::copyArtifactsToWorkers(std::string workerPath, std::string artifactLocation,
                                                std::string remoteWorker) {
    if (artifactLocation.empty() || artifactLocation.find_first_not_of(' ') == artifactLocation.npos) {
        server_logger.log("Received `" + artifactLocation + "` for `artifactLocation` value!", "error");
        throw std::invalid_argument("Received empty string for `artifactLocation` value!");
    }
    std::string pathCheckCommand = "test -e " + workerPath + "&& echo file exists || echo file not found";
    std::string artifactCopyCommand;
    const int ARTIFACTS_COUNT = 4;
    std::string artifactsArray[ARTIFACTS_COUNT] = {"JasmineGraph", "run.sh", "conf", "GraphSAGE"};

    std::string localWorkerArtifactCopyCommandArray[ARTIFACTS_COUNT];
    std::string remoteWorkerArtifactCopyCommandArray[ARTIFACTS_COUNT];

    for(int i = 0; i < ARTIFACTS_COUNT; i++){
        localWorkerArtifactCopyCommandArray[i] = "cp -r " + artifactLocation + "/" + artifactsArray[i] + " " + workerPath;
        remoteWorkerArtifactCopyCommandArray[i] = "scp -r " + artifactLocation + "/" + artifactsArray[i] + " " +
        remoteWorker + ":" + workerPath;
    }

    char buffer[128];
    std::string result = "";

    if (remoteWorker.find("localhost") == std::string::npos) {
        std::string remotePathCheckCommand = "ssh -p 22 " + remoteWorker + " " + pathCheckCommand;
        pathCheckCommand = remotePathCheckCommand;
    }

    FILE *input = popen(pathCheckCommand.c_str(), "r");

    if (input) {
        // read the input
        while (!feof(input)) {
            if (fgets(buffer, 128, input) != NULL) {
                result.append(buffer);
            }
        }
        if (!result.empty()) {
            std::cout << result << std::endl;
        }

        deleteWorkerPath(remoteWorker, workerPath);
        createWorkerPath(remoteWorker, workerPath);
        createLogFilePath(remoteWorker, workerPath);
        pclose(input);
    }

    for(int i = 0; i < ARTIFACTS_COUNT; i++) {
        if (remoteWorker.find("localhost") != std::string::npos) {
            artifactCopyCommand = localWorkerArtifactCopyCommandArray[i];
        } else {
            artifactCopyCommand = remoteWorkerArtifactCopyCommandArray[i];
        }

        FILE *copyInput = popen(artifactCopyCommand.c_str(), "r");
        result = "";
        if (copyInput) {
            // read the input
            while (!feof(copyInput)) {
                if (fgets(buffer, 128, copyInput) != NULL) {
                    result.append(buffer);
                }
            }
            if (!result.empty()) {
                server_logger.log("Error executing command for copying worker artifacts : " + result, "error");
            }
            pclose(copyInput);
        }
    }
}

void JasmineGraphServer::deleteWorkerPath(std::string workerHost, std::string workerPath) {
    std::string pathDeletionCommand = "rm -rf " + workerPath;
    char buffer[BUFFER_SIZE];
    std::string result = "";

    if (workerHost.find("localhost") == std::string::npos) {
        std::string tmpPathCreation = pathDeletionCommand;
        pathDeletionCommand = "ssh -p 22 " + workerHost + " " + tmpPathCreation;
    }

    FILE *input = popen(pathDeletionCommand.c_str(), "r");

    if (input) {
        // read the input
        while (!feof(input)) {
            if (fgets(buffer, BUFFER_SIZE, input) != NULL) {
                result.append(buffer);
            }
        }
        if (!result.empty()) {
            server_logger.log("Error executing command for deleting worker path : " + result, "error");
        }
        pclose(input);
    }
}

void JasmineGraphServer::createWorkerPath(std::string workerHost, std::string workerPath) {
    std::string pathCreationCommand = "mkdir -p " + workerPath;
    char buffer[BUFFER_SIZE];
    std::string result = "";

    if (workerHost.find("localhost") == std::string::npos) {
        std::string tmpPathCreation = pathCreationCommand;
        pathCreationCommand = "ssh -p 22 " + workerHost + " " + tmpPathCreation;
    }

    FILE *input = popen(pathCreationCommand.c_str(), "r");

    if (input) {
        // read the input
        while (!feof(input)) {
            if (fgets(buffer, BUFFER_SIZE, input) != NULL) {
                result.append(buffer);
            }
        }
        if (!result.empty()) {
            server_logger.log("Error executing command for creating worker path : " + result, "error");
        }
        pclose(input);
    }
}

void JasmineGraphServer::createLogFilePath(std::string workerHost, std::string workerPath) {
    std::string pathCreationCommand = "mkdir -p " + workerPath + "/logs";
    char buffer[BUFFER_SIZE];
    std::string result = "";

    if (workerHost.find("localhost") == std::string::npos) {
        std::string tmpPathCreation = pathCreationCommand;
        pathCreationCommand = "ssh -p 22 " + workerHost + " " + tmpPathCreation;
    }

    FILE *input = popen(pathCreationCommand.c_str(), "r");

    if (input) {
        // read the input
        while (!feof(input)) {
            if (fgets(buffer, BUFFER_SIZE, input) != NULL) {
                result.append(buffer);
            }
        }
        if (!result.empty()) {
            server_logger.log("Error executing command for creating log file path : " + result, "error");
        }
        pclose(input);
    }
}

void JasmineGraphServer::addHostsToMetaDB(std::string host, std::vector<int> portVector, std::vector<int> dataPortVector) {
    Utils utils;
    string name = host;
    string sqlStatement = "";
    string ip_address = "";
    string user = "";
    if (host.find('@') != std::string::npos) {
        vector<string> splitted = utils.split(host, '@');
        ip_address = splitted[1];
        user = splitted[0];
    } else {
        ip_address = host;
    }

    for (int i =0 ; i < portVector.size() ; i++) {
        int workerPort = portVector.at(i);
        int workerDataPort = dataPortVector.at(i);

        if (!utils.hostExists(name, ip_address, std::to_string(workerPort), this->sqlite)) {
            string hostID = Utils::getHostID(name, this->sqlite);
            sqlStatement = ("INSERT INTO worker (host_idhost,name,ip,user,is_public,server_port,server_data_port) VALUES(\"" +
                    hostID + "\", \"" + name + "\", \"" + ip_address + "\",\""+user+"\", \"\",\""+ std::to_string(workerPort) +
                    "\", \""+ std::to_string(workerDataPort) +"\")");
            this->sqlite.runInsert(sqlStatement);
        }
    }

}

map<string, string> JasmineGraphServer::getLiveHostIDList() {
    server_logger.log("###MASTER### Loading Live Host ID List", "info");
    map<string, string> hostIDMap;
    std::vector<vector<pair<string, string>>> v = this->sqlite.runSelect(
                "SELECT host_idhost,user,ip,server_port FROM worker;");
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

        hostIDMap.insert(make_pair(host,id));
    }

    return hostIDMap;
}

void JasmineGraphServer::updateMetaDB(vector<JasmineGraphServer::workers> hostWorkerMap,
                                      std::map<int, string> partitionFileList, int graphID, string uploadEndTime) {
    SQLiteDBInterface refToSqlite = *new SQLiteDBInterface();
    refToSqlite.init();
    string sqlStatement2 =
            "UPDATE graph SET upload_end_time = '" + uploadEndTime + "' ,graph_status_idgraph_status = '" +
            to_string(Conts::GRAPH_STATUS::OPERATIONAL) + "' WHERE idgraph = '" + to_string(graphID) + "'";
    refToSqlite.runUpdate(sqlStatement2);
}

void JasmineGraphServer::removeGraph(vector<pair<string, string>> hostHasPartition, string graphID, std::string masterIP) {
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
    Utils utils;
    std::cout << pthread_self() << " host : " << host << " port : " << port << std::endl;
    int sockfd;
    char data[300];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot accept connection" << std::endl;
        return 0;
    }

    if (host.find('@') != std::string::npos) {
        host = utils.split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        std::cerr << "ERROR, no host named " << server << std::endl;
    }

    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *) server->h_addr,
          (char *) &serv_addr.sin_addr.s_addr,
          server->h_length);
    serv_addr.sin_port = htons(port);
    if (connect(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
        //TODO::exit
    }

    bzero(data, 301);
    int result_wr = write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if(result_wr < 0) {
        server_logger.log("Error writing to socket", "error");
    }

    server_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        server_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if(result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + masterIP, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            server_logger.log("Received : " + response, "error");
        }

        result_wr = write(sockfd, JasmineGraphInstanceProtocol::DELETE_GRAPH_FRAGMENT.c_str(),
              JasmineGraphInstanceProtocol::DELETE_GRAPH_FRAGMENT.size());

        if(result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + JasmineGraphInstanceProtocol::DELETE_GRAPH_FRAGMENT, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");
        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, graphID.c_str(), graphID.size());

            if(result_wr < 0) {
                server_logger.log("Error writing to socket", "error");
            }

            server_logger.log("Sent : Graph ID " + graphID, "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");
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

int JasmineGraphServer::removePartitionThroughService(string host, int port, string graphID, string partitionID, string masterIP) {
    Utils utils;
    std::cout << pthread_self() << " host : " << host << " port : " << port << std::endl;
    int sockfd;
    char data[300];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot accept connection" << std::endl;
        return 0;
    }

    if (host.find('@') != std::string::npos) {
        host = utils.split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        std::cerr << "ERROR, no host named " << server << std::endl;
    }

    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *) server->h_addr,
          (char *) &serv_addr.sin_addr.s_addr,
          server->h_length);
    serv_addr.sin_port = htons(port);
    if (connect(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
        //TODO::exit
    }

    bzero(data, 301);
    int result_wr = write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if(result_wr < 0) {
        server_logger.log("Error writing to socket", "error");
    }

    server_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        server_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if(result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + masterIP, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            server_logger.log("Received : " + response, "error");
        }

        result_wr = write(sockfd, JasmineGraphInstanceProtocol::DELETE_GRAPH.c_str(),
              JasmineGraphInstanceProtocol::DELETE_GRAPH.size());

        if(result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + JasmineGraphInstanceProtocol::DELETE_GRAPH, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, graphID.c_str(), graphID.size());

            if(result_wr < 0) {
                server_logger.log("Error writing to socket", "error");
            }

            server_logger.log("Sent : Graph ID " + graphID, "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");

            if (response.compare(JasmineGraphInstanceProtocol::SEND_PARTITION_ID) == 0) {
                server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_PARTITION_ID, "info");
                result_wr = write(sockfd, partitionID.c_str(), partitionID.size());

                if(result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : Partition ID " + partitionID, "info");

                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);
                response = utils.trim_copy(response, " \f\n\r\t\v");
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

int JasmineGraphServer::initiateEntityResolution(vector<pair<string, string>> hostHasPartition, string graphID, std::string masterIP) {
    int count = 0;
    int partitionCount = hostPortMap.size();
    //Create Bloom filters
    //Define threads for each host
    std::thread *workerThreads = new std::thread[partitionCount];
    //Iterate through all hosts
    cout << "Starting Entity Resolution.." << endl;
    cout << "Creating Bloom filters" << endl;
    for (std::vector<pair<string, string>>::iterator it = (hostHasPartition.begin()); it != hostHasPartition.end(); ++it) {
        //Fetch hostname, port and partition id
        string hostname = it -> first;
        int port = hostPortMap[it->first].first;
        string partitionID = it->second;
        //Initialize threads for host
        workerThreads[count++] = std::thread(createBloomFilters, hostname, port, graphID, partitionID, masterIP);
        sleep(1);
    }

    for (int threadCount = 0; threadCount < count; threadCount++) {
        if(workerThreads[threadCount].joinable()) {
            workerThreads[threadCount].join();
        }
        std::cout << "Thread [A]: " << threadCount << " joined" << std::endl;
    }
    //Bloom Filter creation end

    cout << "Sharing created Bloom filters with master" << endl;
    //Identify coordinating worker
    string coordinatorHost = hostHasPartition.at(0).first;
    int workerPort = hostPortMap[coordinatorHost].first;
    //Iterate through all partition owners to share partitions with coordinating worker
    for (std::vector<pair<string, string>>::iterator it = (hostHasPartition.begin()); it != hostHasPartition.end(); ++it) {
        //Fetch hostname, port and partition id
        string hostname = it -> first;
        int port = hostPortMap[it->first].first;
        string partitionID = it->second;
        //Initialize threads for host
        workerThreads[count++] = std::thread(createBloomFilters, hostname, port, graphID, partitionID, masterIP);
        sleep(1);
    }

    for (int threadCount = 0; threadCount < count; threadCount++) {
        if(workerThreads[threadCount].joinable()) {
            workerThreads[threadCount].join();
        }
        std::cout << "Thread [A]: " << threadCount << " joined" << std::endl;
    }
    //Cluster Bloom Filters
    cout << "Clustering Bloom filters into" << partitionCount << "clusters" << endl;

    EntityResolver er;
    er.clusterFilters(graphID, partitionCount, partitionCount);

    //Reshuffle clusters into workers
    cout << "Reshuffling cluster files to workers" << endl;

    //Bucket local clusters
    cout << "Bucketing worker-local clusters" << endl;
    for (std::vector<pair<string, string>>::iterator it = (hostHasPartition.begin()); it != hostHasPartition.end(); ++it) {
        //Fetch hostname, port and partition id
        string hostname = it -> first;
        int port = hostPortMap[it->first].first;
        string partitionID = it->second;
        //Initialize threads for host
        workerThreads[count++] = std::thread(createBloomFilters, hostname, port, graphID, partitionID, masterIP);
        sleep(1);
    }

    for (int threadCount = 0; threadCount < count; threadCount++) {
        if(workerThreads[threadCount].joinable()) {
            workerThreads[threadCount].join();
        }
        std::cout << "Thread [A]: " << threadCount << " joined" << std::endl;
    }



}

int JasmineGraphServer::createBloomFilters(std::string host, int port, std::string graphID, string partitionID, std::string masterIP) {
    Utils utils;
    std::cout << pthread_self() << " host : " << host << " port : " << port << std::endl;
    int sockfd;
    char data[300];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot accept connection" << std::endl;
        return 0;
    }

    if (host.find('@') != std::string::npos) {
        host = utils.split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        std::cerr << "ERROR, no host named " << server << std::endl;
    }

    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *) server->h_addr,
          (char *) &serv_addr.sin_addr.s_addr,
          server->h_length);
    serv_addr.sin_port = htons(port);
    if (connect(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
        //TODO::exit
    }

    bzero(data, 301);
    int result_wr = write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if(result_wr < 0) {
        server_logger.log("Error writing to socket", "error");
    }

    server_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        server_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if(result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + masterIP, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            server_logger.log("Received : " + response, "error");
        }

        result_wr = write(sockfd, JasmineGraphInstanceProtocol::CREATE_BLOOM_FILTERS.c_str(),
                          JasmineGraphInstanceProtocol::CREATE_BLOOM_FILTERS.size());

        if(result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + JasmineGraphInstanceProtocol::CREATE_BLOOM_FILTERS, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, graphID.c_str(), graphID.size());

            if(result_wr < 0) {
                server_logger.log("Error writing to socket", "error");
            }

            server_logger.log("Sent : Graph ID " + graphID, "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");

            if (response.compare(JasmineGraphInstanceProtocol::SEND_PARTITION_ID) == 0) {
                server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_PARTITION_ID, "info");
                result_wr = write(sockfd, partitionID.c_str(), partitionID.size());

                if(result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : Partition ID " + partitionID, "info");

                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);
                response = utils.trim_copy(response, " \f\n\r\t\v");
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

int JasmineGraphServer::bucketLocalClusers(std::string host, int port, std::string graphID, vector<int> clusters, std::string masterIP) {
    Utils utils;
    std::cout << pthread_self() << " host : " << host << " port : " << port << std::endl;
    int sockfd;
    char data[300];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot accept connection" << std::endl;
        return 0;
    }

    if (host.find('@') != std::string::npos) {
        host = utils.split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        std::cerr << "ERROR, no host named " << server << std::endl;
    }

    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *) server->h_addr,
          (char *) &serv_addr.sin_addr.s_addr,
          server->h_length);
    serv_addr.sin_port = htons(port);
    if (connect(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
        //TODO::exit
    }

    bzero(data, 301);
    int result_wr = write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if(result_wr < 0) {
        server_logger.log("Error writing to socket", "error");
    }

    server_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        server_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if(result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + masterIP, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            server_logger.log("Received : " + response, "error");
        }

        result_wr = write(sockfd, JasmineGraphInstanceProtocol::BUCKET_LOCAL_CLUSTERS.c_str(),
                          JasmineGraphInstanceProtocol::BUCKET_LOCAL_CLUSTERS.size());

        if(result_wr < 0) {
            server_logger.log("Error writing to socket", "error");
        }

        server_logger.log("Sent : " + JasmineGraphInstanceProtocol::BUCKET_LOCAL_CLUSTERS, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            server_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, graphID.c_str(), graphID.size());

            if(result_wr < 0) {
                server_logger.log("Error writing to socket", "error");
            }

            server_logger.log("Sent : Graph ID " + graphID, "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");

            if (response.compare(JasmineGraphInstanceProtocol::SEND_PARTITION_ID) == 0) {
                server_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_PARTITION_ID, "info");
                result_wr = write(sockfd, clusters.c_str(), clusters.size());

                if(result_wr < 0) {
                    server_logger.log("Error writing to socket", "error");
                }

                server_logger.log("Sent : Partition ID " + clusters, "info");

                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);
                response = utils.trim_copy(response, " \f\n\r\t\v");
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

std::vector<JasmineGraphServer::workers> JasmineGraphServer::getHostWorkerMap() {
    return hostWorkerMap;
}

void JasmineGraphServer::updateOperationalGraphList() {
    Utils utils;
    string hosts = "";
    string graphIDs = "";
    std::vector<std::string> hostsList;

    if (profile == "native") {
        hostsList = utils.getHostListFromProperties();
    } else if (profile == "docker") {
        hostsList = getWorkerVector(workerHosts);
    }
    vector<string>::iterator it;
    for (it = hostsList.begin(); it < hostsList.end(); it++) {
        string host = *it;
        hosts += ("'" + host + "', ");
    }
    hosts = hosts.substr(0, hosts.size() - 2);
    string sqlStatement = ("SELECT b.partition_graph_idgraph FROM worker_has_partition AS b "
                           "JOIN worker WHERE worker.idworker = b.worker_idworker AND worker.name IN "
                           "(" + hosts + ") GROUP BY b.partition_graph_idgraph HAVING COUNT(b.partition_idpartition)= "
                           "(SELECT COUNT(a.idpartition) FROM partition AS a "
                           "WHERE a.graph_idgraph = b.partition_graph_idgraph);");
    std::vector<vector<pair<string, string>>> v = this->sqlite.runSelect(sqlStatement);
    for (std::vector<vector<pair<string, string>>>::iterator i = v.begin(); i != v.end(); ++i) {
        for (std::vector<pair<string, string>>::iterator j = (i->begin()); j != i->end(); ++j) {
            graphIDs += (j->second + ", ");
        }
    }
    graphIDs = graphIDs.substr(0, graphIDs.size() - 2);
    string sqlStatement2 = "UPDATE graph SET graph_status_idgraph_status = ("
                           "CASE WHEN idgraph IN (" + graphIDs + ") THEN '" +
                           to_string(Conts::GRAPH_STATUS::OPERATIONAL) + "' ELSE '" +
                           to_string(Conts::GRAPH_STATUS::NONOPERATIONAL) + "' END )";
    this->sqlite.runUpdate(sqlStatement2);
}

std::map<string, JasmineGraphServer::workerPartitions> JasmineGraphServer::getGraphPartitionedHosts(string graphID) {

    vector<pair<string, string>> hostHasPartition;
    SQLiteDBInterface refToSqlite = *new SQLiteDBInterface();
    refToSqlite.init();
    vector<vector<pair<string, string>>> hostPartitionResults = refToSqlite.runSelect(
            "SELECT name, partition_idpartition FROM worker_has_partition INNER JOIN worker ON worker_"
            "idworker = idworker WHERE partition_graph_idgraph = '" + graphID + "'");
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
        graphPartitionedHosts.insert((pair<string, JasmineGraphServer::workerPartitions>(it->first,
                                                                                         {hostPortMap[it->first].first,
                                                                                          hostPortMap[it->first].second,
                                                                                          hostPartitions[it->first]})));
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
    Utils utils;
    std::vector<std::string> workerVector = utils.split(workerList,',');
    return workerVector;
}

void JasmineGraphServer::backupPerformanceDB() {
    Utils utils;
    std::string performanceDBPath = utils.getJasmineGraphProperty("org.jasminegraph.performance.db.location");

    uint64_t currentTimestamp= std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    std::string backupScript = "cp " + performanceDBPath + " " + performanceDBPath + "-" + to_string(currentTimestamp);

    char buffer[BUFFER_SIZE];
    std::string result = "";

    FILE *input = popen(backupScript.c_str(), "r");

    if (input) {
        // read the input
        while (!feof(input)) {
            if (fgets(buffer, BUFFER_SIZE, input) != NULL) {
                result.append(buffer);
            }
        }
        if (!result.empty()) {
            server_logger.log("Error in performance database backup process","error");
        }

        pclose(input);
    }
}

void JasmineGraphServer::clearPerformanceDB() {
    performanceSqlite.runUpdate("delete from host_performance_data");
    performanceSqlite.runUpdate("delete from place_performance_data");
    performanceSqlite.runUpdate("delete from place");
    performanceSqlite.runUpdate("delete from host");
}

void JasmineGraphServer::addInstanceDetailsToPerformanceDB(std::string host, std::vector<int> portVector, std::string isMaster) {
    std::vector<int>::iterator it;
    std::string hostString;
    std::string hostId;
    std::string user;
    std::string ipAddress;
    int count = 0;
    Utils utils;

    if (host.find('@') != std::string::npos) {
        vector<string> splitted = utils.split(host, '@');
        ipAddress = splitted[1];
        user = splitted[0];
    } else {
        ipAddress = host;
    }

    std::string searchHost = "select idhost,ip from host where ip='"+ipAddress+"'";
    vector<vector<pair<string,string>>> selectResult = this->performanceSqlite.runSelect(searchHost);

    if (selectResult.size() > 0) {
        hostId = selectResult[0][0].second;
    } else {
        std::string hostInsertQuery = "insert into host (name, ip, is_public, total_cpu_cores, total_memory) values ('" +
                host + "','" + ipAddress + "','false','','')";
        int insertedHostId = this->performanceSqlite.runInsert(hostInsertQuery);
        hostId = to_string(insertedHostId);
    }

    std::string insertPlaceQuery = "insert into place (host_idhost,server_port,ip,user,is_master,is_host_reporter) values ";

    for (it = portVector.begin(); it < portVector.end(); it++) {
        std::string isHostReporter = "false";
        int port = (*it);
        if (count == 0) {
            std::string searchReporterQuery = "select idplace from place where ip='" + ipAddress + "' and is_host_reporter='true'";
            vector<vector<pair<string,string>>> searchResult = this->performanceSqlite.runSelect(searchReporterQuery);
            if (searchResult.size() == 0) {
                isHostReporter = "true";
            }
        }
        hostString += "('" + hostId + "','" + to_string(port) + "','" + ipAddress + "','" + user + "','" + isMaster + "','" + isHostReporter + "'),";
        count++;
    }

    hostString = hostString.substr(0, hostString.length() - 1);
    insertPlaceQuery = insertPlaceQuery + hostString;

    this->performanceSqlite.runInsert(insertPlaceQuery);
}
