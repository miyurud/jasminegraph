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
#include "JasminGraphTrainingInitiator.h"
#include "../util/logger/Logger.h"
#include "../util/Utils.h"
#include "../server/JasmineGraphServer.h"
#include "../server/JasmineGraphInstanceProtocol.h"

Logger trainer_log;

void JasminGraphTrainingInitiator::initiateTrainingLocally(std::string graphID, std::string trainingArgs) {
    std::cout << "Initiating training.." << std::endl;
    int count = 0;
//    std::vector<JasmineGraphServer::workers> hostWorkerMap = JasmineGraphServer::getHostWorkerMap();
//    std::cout << hostWorkerMap.size() << std::endl;
//    std::thread *workerThreads = new std::thread[hostWorkerMap.size()];

    JasmineGraphServer *jasmineServer = new JasmineGraphServer();
    std::map<std::string, JasmineGraphServer::workerPartitions> graphPartitionedHosts = jasmineServer->getGraphPartitionedHosts(
            graphID);
    int partition_count = 0;
    std::map<std::string, JasmineGraphServer::workerPartitions>::iterator mapIterator;
    for (mapIterator = graphPartitionedHosts.begin(); mapIterator != graphPartitionedHosts.end(); mapIterator++) {
        JasmineGraphServer::workerPartitions workerPartition = mapIterator->second;
        std::vector<std::string> partitions = workerPartition.partitionID;
        std::vector<std::string>::iterator it;
        for(it = partitions.begin(); it < partitions.end(); it++){
            partition_count++;
        }
    }
    cout<<partition_count<<endl;
    std::thread *workerThreads = new std::thread[partition_count];
//    std::thread *workerThreads = new std::thread[1];

    Utils utils;
    string prefix = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.trainedmodelfolder");
    string trainarg_prefix = "Graphsage Unsupervised_train ";
    trainingArgs= trainarg_prefix+trainingArgs+" --train_prefix "+prefix+"/"+graphID;

    std::map<std::string, JasmineGraphServer::workerPartitions>::iterator j;
    for (j = graphPartitionedHosts.begin(); j != graphPartitionedHosts.end(); j++) {
        JasmineGraphServer::workerPartitions workerPartition = j->second;
        std::vector<std::string> partitions = workerPartition.partitionID;
        std::vector<std::string>::iterator k;
        for(k = partitions.begin(); k != partitions.end(); k++){
            workerThreads[count] = std::thread(initiateTrain, j->first, workerPartition.port, workerPartition.dataPort, trainingArgs+" --train_worker "+*k);
//            std::thread workThread = std::thread(initiateTrain, j->first, workerPartition.port, workerPartition.dataPort, trainingArgs+" --train_worker "+*k);
//            workThread.detach();
            count++;
//            break;
        }
    }

//    std::vector<JasmineGraphServer::workers, std::allocator<JasmineGraphServer::workers>>::iterator mapIterator;
//    for (mapIterator = hostWorkerMap.begin(); mapIterator < hostWorkerMap.end(); mapIterator++) {
//        JasmineGraphServer::workers worker = *mapIterator;
//        workerThreads[count] = std::thread(initiateTrain, worker.hostname, worker.port, worker.dataPort, trainingArgs);
//        count++;
//    }


    for (int threadCount = 0; threadCount < count; threadCount++) {
        workerThreads[threadCount].join();
        std::cout << "Thread " << threadCount << " joined" << std::endl;
    }
    SQLiteDBInterface refToSqlite = *new SQLiteDBInterface();
    refToSqlite.init();
    string sqlStatement =
            "UPDATE graph SET train_status = '" + (Conts::TRAIN_STATUS::TRAINED) + "' WHERE idgraph = '" + graphID + "'";
    refToSqlite.runUpdate(sqlStatement);

}

bool JasminGraphTrainingInitiator::initiateTrain(std::string host, int port, int dataPort, std::string trainingArgs) {
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
    write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());
    trainer_log.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        trainer_log.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        string server_host = utils.getJasmineGraphProperty("org.jasminegraph.server.host");
        write(sockfd, server_host.c_str(), server_host.size());
        trainer_log.log("Sent : " + server_host, "info");


        write(sockfd, JasmineGraphInstanceProtocol::INITIATE_TRAIN.c_str(),
              JasmineGraphInstanceProtocol::INITIATE_TRAIN.size());
        trainer_log.log("Sent : " + JasmineGraphInstanceProtocol::INITIATE_TRAIN, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");
        std::cout << response << std::endl;
        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            trainer_log.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            std::cout << trainingArgs << std::endl;
            write(sockfd, (trainingArgs).c_str(), (trainingArgs).size());
            trainer_log.log("Sent : training args " + trainingArgs, "info");
            return 0;
        }
    } else {
        trainer_log.log("There was an error in the upload process and the response is :: " + response, "error");
    }

    close(sockfd);
    return 0;
}