#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <map>
#include "JasmineGraphFederatedInstance.h"
#include "../../server/JasmineGraphInstanceProtocol.h"
#include "../../util/Utils.h"
#include "../trainer/JasmineGraphTrainingSchedular.h"
#include "../../server/JasmineGraphServer.h"
#include <iostream>
#include "../../util/logger/Logger.h"
#include "../../util/Utils.h"


Logger fed_trainer_log;

void JasmineGraphFederatedInstance::initiateFiles(std::string graphID, std::string trainingArgs) {
    std::cout << "Initiating training.." << std::endl;
    int count = 0;
    JasmineGraphTrainingSchedular *schedular = new JasmineGraphTrainingSchedular();
    map<string, map<int, int>> scheduleForAllHosts = schedular->schedulePartitionTraining(graphID);
    JasmineGraphServer *jasmineServer = new JasmineGraphServer();
    std::map<std::string, JasmineGraphServer::workerPartitions> graphPartitionedHosts = jasmineServer->getGraphPartitionedHosts(
            graphID);
    int partition_count = 0;
    std::map<std::string, JasmineGraphServer::workerPartitions>::iterator mapIterator;
    for (mapIterator = graphPartitionedHosts.begin(); mapIterator != graphPartitionedHosts.end(); mapIterator++) {
        JasmineGraphServer::workerPartitions workerPartition = mapIterator->second;
        std::vector<std::string> partitions = workerPartition.partitionID;
        std::vector<std::string>::iterator it;
        for (it = partitions.begin(); it < partitions.end(); it++) {
            partition_count++;
        }
    }
    std::cout << partition_count << std::endl;
    std::thread *workerThreads = new std::thread[partition_count];

    Utils utils;
    string prefix = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.trainedmodelfolder");
    string attr_prefix = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    trainingArgs = trainingArgs;
    std::map<std::string, JasmineGraphServer::workerPartitions>::iterator j;
    for (j = graphPartitionedHosts.begin(); j != graphPartitionedHosts.end(); j++) {
        JasmineGraphServer::workerPartitions workerPartition = j->second;
        std::vector<std::string> partitions = workerPartition.partitionID;
        string partitionCount = std::to_string(partitions.size());
        std::vector<std::string>::iterator k;
        map<int, int> scheduleOfHost = scheduleForAllHosts[j->first];
        for (k = partitions.begin(); k != partitions.end(); k++) {
            int iterationOfPart = scheduleOfHost[stoi(*k)];
            workerThreads[count] = std::thread(initiateTrain, j->first, workerPartition.port, workerPartition.dataPort,trainingArgs + " " + *k,iterationOfPart, partitionCount);
            count++;
            sleep(3);
        }
    }

    for (int threadCount = 0; threadCount < count; threadCount++) {
        workerThreads[threadCount].join();
        std::cout << "Thread " << threadCount << " joined" << std::endl;
    }
}

void JasmineGraphFederatedInstance::initiateCommunication(std::string graphID, std::string trainingArgs,SQLiteDBInterface sqlite) {

    Utils utils;
    std::vector<Utils::worker> workerArr = utils.getWorkerList(sqlite);
    std::thread *workerThreads = new std::thread[workerArr.size()+1];

    int limit = workerArr.size();
    int thread_id = 0;
    int poolSize = workerArr.size()+1;

    for (int i = 0; i< limit; i++) {
        if (i==0) {
            workerThreads[thread_id] = std::thread(initiateServer,"localhost", 7780, 7781,trainingArgs,2, "1");
            sleep(10);   
        }

        
        workerThreads[thread_id] = std::thread(initiateClient,"localhost", 7780, 7781,trainingArgs + ' '+ '0',2, "1");    
        sleep(5);
        thread_id += 1;
    }  

    for (int threadCount = 0; threadCount < poolSize; threadCount++) {
        workerThreads[threadCount].join();
        std::cout << "Fed Thread " << threadCount << " joined" << std::endl;
    }
}


bool JasmineGraphFederatedInstance::initiateTrain(std::string host, int port, int dataPort,std::string trainingArgs,int iteration, string partCount) {
    Utils utils;
    bool result = true;
    std::cout << pthread_self() << " host : " << host << " port : " << port << " DPort : " << dataPort << std::endl;
    int sockfd;
    char data[FED_DATA_LENGTH];
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

    bzero(data, FED_DATA_LENGTH);
    write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());
    fed_trainer_log.log("Sent fed : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, FED_DATA_LENGTH);
    read(sockfd, data, FED_DATA_LENGTH);
    string response = (data);

    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        fed_trainer_log.log("Received fed : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        string server_host = utils.getJasmineGraphProperty("org.jasminegraph.server.host");
        write(sockfd, server_host.c_str(), server_host.size());
        fed_trainer_log.log("Sent fed : " + server_host, "info");

        write(sockfd, JasmineGraphInstanceProtocol::INITIATE_FILES.c_str(),
              JasmineGraphInstanceProtocol::INITIATE_FILES.size());
        fed_trainer_log.log("Sent fed : " + JasmineGraphInstanceProtocol::INITIATE_FILES, "info");
        bzero(data, FED_DATA_LENGTH);
        read(sockfd, data, FED_DATA_LENGTH);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");
        std::cout << response << std::endl;
        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            fed_trainer_log.log("Received fed : " + JasmineGraphInstanceProtocol::OK, "info");
            write(sockfd, (trainingArgs).c_str(), (trainingArgs).size());
            fed_trainer_log.log("Sent fed: training args " + trainingArgs, "info");
            bzero(data, FED_DATA_LENGTH);
            return 0;

            }
    } else {
        fed_trainer_log.log("There was an error in the invoking training process and the response is :: " + response,
                        "error");
    }

    close(sockfd);
    return 0;
}


bool JasmineGraphFederatedInstance::initiateServer(std::string host, int port, int dataPort,std::string trainingArgs,int iteration, string partCount) {
    Utils utils;
    bool result = true;
    std::cout << pthread_self() << " host : " << host << " port : " << port << " DPort : " << dataPort << std::endl;
    int sockfd;
    char data[FED_DATA_LENGTH];
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

    bzero(data, FED_DATA_LENGTH);
    write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());
    fed_trainer_log.log("Sent fed : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, FED_DATA_LENGTH);
    read(sockfd, data, FED_DATA_LENGTH);
    string response = (data);

    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        fed_trainer_log.log("Received fed : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        string server_host = utils.getJasmineGraphProperty("org.jasminegraph.server.host");
        write(sockfd, server_host.c_str(), server_host.size());
        fed_trainer_log.log("Sent fed : " + server_host, "info");

        write(sockfd, JasmineGraphInstanceProtocol::INITIATE_SERVER.c_str(),
              JasmineGraphInstanceProtocol::INITIATE_SERVER.size());
        fed_trainer_log.log("Sent fed : " + JasmineGraphInstanceProtocol::INITIATE_SERVER, "info");
        bzero(data, FED_DATA_LENGTH);
        read(sockfd, data, FED_DATA_LENGTH);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");
        std::cout << response << std::endl;
        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            fed_trainer_log.log("Received fed : " + JasmineGraphInstanceProtocol::OK, "info");
            write(sockfd, (trainingArgs).c_str(), (trainingArgs).size());
            fed_trainer_log.log("Sent fed : training args " + trainingArgs, "info");
            bzero(data, FED_DATA_LENGTH);
            return 0;

            }
    } else {
        fed_trainer_log.log("There was an error in the invoking training process and the response is :: " + response,
                        "error");
    }

    close(sockfd);
    return 0;
}

bool JasmineGraphFederatedInstance::initiateClient(std::string host, int port, int dataPort,std::string trainingArgs,int iteration, string partCount) {
    Utils utils;
    bool result = true;
    std::cout << pthread_self() << " host : " << host << " port : " << port << " DPort : " << dataPort << std::endl;
    int sockfd;
    char data[FED_DATA_LENGTH];
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

    bzero(data, FED_DATA_LENGTH);
    write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());
    fed_trainer_log.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, FED_DATA_LENGTH);
    read(sockfd, data, FED_DATA_LENGTH);
    string response = (data);

    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        fed_trainer_log.log("Received fed : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        string server_host = utils.getJasmineGraphProperty("org.jasminegraph.server.host");
        write(sockfd, server_host.c_str(), server_host.size());
        fed_trainer_log.log("Sent fed : " + server_host, "info");

        write(sockfd, JasmineGraphInstanceProtocol::INITIATE_CLIENT.c_str(),
              JasmineGraphInstanceProtocol::INITIATE_CLIENT.size());
        fed_trainer_log.log("Sent fed : " + JasmineGraphInstanceProtocol::INITIATE_CLIENT, "info");
        bzero(data, FED_DATA_LENGTH);
        read(sockfd, data, FED_DATA_LENGTH);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");
        std::cout << response << std::endl;
        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            fed_trainer_log.log("Received fed : " + JasmineGraphInstanceProtocol::OK, "info");
            write(sockfd, (trainingArgs).c_str(), (trainingArgs).size());
            fed_trainer_log.log("Sent fed : training args " + trainingArgs, "info");
            bzero(data, FED_DATA_LENGTH);
            return 0;

            }
    } else {
        fed_trainer_log.log("There was an error in the invoking training process and the response is :: " + response,
                        "error");
    }

    close(sockfd);
    return 0;
}