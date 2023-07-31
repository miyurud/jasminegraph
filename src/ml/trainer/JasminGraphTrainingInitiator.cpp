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
#include "../../util/logger/Logger.h"
#include "../../util/Utils.h"
#include "../../server/JasmineGraphServer.h"
#include "../../server/JasmineGraphInstanceProtocol.h"
#include "../trainer/JasmineGraphTrainingSchedular.h"

Logger trainer_log;

void JasminGraphTrainingInitiator::initiateTrainingLocally(std::string graphID, std::string trainingArgs) {
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
    cout << partition_count << endl;
    std::thread *workerThreads = new std::thread[partition_count];

    Utils utils;
    string prefix = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.trainedmodelfolder");
    string attr_prefix = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    string trainarg_prefix = "Graphsage Unsupervised_train ";
    trainingArgs =
            trainarg_prefix + trainingArgs + " --train_prefix " + prefix + graphID + " --train_attr_prefix " +
            attr_prefix + graphID;


    std::map<std::string, JasmineGraphServer::workerPartitions>::iterator j;
    for (j = graphPartitionedHosts.begin(); j != graphPartitionedHosts.end(); j++) {
        JasmineGraphServer::workerPartitions workerPartition = j->second;
        std::vector<std::string> partitions = workerPartition.partitionID;
        string partitionCount = to_string(partitions.size());
        std::vector<std::string>::iterator k;
        map<int, int> scheduleOfHost = scheduleForAllHosts[j->first];
        for (k = partitions.begin(); k != partitions.end(); k++) {
            int iterationOfPart = scheduleOfHost[stoi(*k)];
            workerThreads[count] = std::thread(initiateTrain, j->first, workerPartition.port, workerPartition.dataPort,
                                               trainingArgs + " --train_worker " + *k, iterationOfPart, partitionCount);
            count++;
            sleep(3);
        }
    }

    for (int threadCount = 0; threadCount < count; threadCount++) {
        workerThreads[threadCount].join();
        std::cout << "Thread " << threadCount << " joined" << std::endl;
    }
    SQLiteDBInterface refToSqlite = *new SQLiteDBInterface();
    refToSqlite.init();
    string sqlStatement =
            "UPDATE graph SET train_status = '" + (Conts::TRAIN_STATUS::TRAINED) + "' WHERE idgraph = '" + graphID +
            "'";
    refToSqlite.runUpdate(sqlStatement);

}

bool JasminGraphTrainingInitiator::initiateTrain(std::string host, int port, int dataPort, std::string trainingArgs,
                                                 int iteration, string partCount) {
    Utils utils;
    bool result = true;
    std::cout << pthread_self() << " host : " << host << " port : " << port << " DPort : " << dataPort << std::endl;
    int sockfd;
    char data[DATA_LENGTH + 1];
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

    bzero(data, DATA_LENGTH + 1);
    int result_wr = write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());
    if(result_wr < 0) {
        trainer_log.log("Error writing to socket", "error");
        return false;
    }

    trainer_log.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, DATA_LENGTH + 1);
    read(sockfd, data, DATA_LENGTH);
    string response = (data);

    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        trainer_log.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        string server_host = utils.getJasmineGraphProperty("org.jasminegraph.server.host");
        int result_wr = write(sockfd, server_host.c_str(), server_host.size());
        if(result_wr < 0) {
            trainer_log.log("Error writing to socket", "error");
            return false;
        }

        trainer_log.log("Sent : " + server_host, "info");

        bzero(data, DATA_LENGTH + 1);
        read(sockfd, data, DATA_LENGTH);
        string response = (data);

        response = utils.trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            trainer_log.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            trainer_log.log("Received : " + response, "error");
        }

        result_wr = write(sockfd, JasmineGraphInstanceProtocol::INITIATE_TRAIN.c_str(),
                          JasmineGraphInstanceProtocol::INITIATE_TRAIN.size());
        if(result_wr < 0) {
            trainer_log.log("Error writing to socket", "error");
            return false;
        }

        trainer_log.log("Sent : " + JasmineGraphInstanceProtocol::INITIATE_TRAIN, "info");
        bzero(data, DATA_LENGTH + 1);
        read(sockfd, data, DATA_LENGTH);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");
        std::cout << response << std::endl;
        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            trainer_log.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, (trainingArgs).c_str(), (trainingArgs).size());
            if(result_wr < 0) {
                trainer_log.log("Error writing to socket", "error");
                return false;
            }

            trainer_log.log("Sent : training args " + trainingArgs, "info");
            bzero(data, DATA_LENGTH + 1);
            read(sockfd, data, DATA_LENGTH);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");
            if (response.compare(JasmineGraphInstanceProtocol::SEND_PARTITION_ITERATION) == 0) {
                trainer_log.log("Received : " + JasmineGraphInstanceProtocol::SEND_PARTITION_ITERATION, "info");
                result_wr = write(sockfd, to_string(iteration).c_str(), to_string(iteration).size());
                if(result_wr < 0) {
                    trainer_log.log("Error writing to socket", "error");
                    return false;
                }

                trainer_log.log("Sent : partition iteration " + to_string(iteration), "info");

                bzero(data, DATA_LENGTH + 1);
                read(sockfd, data, DATA_LENGTH);
                response = (data);
                response = utils.trim_copy(response, " \f\n\r\t\v");
                if (response.compare(JasmineGraphInstanceProtocol::SEND_PARTITION_COUNT) == 0) {
                    trainer_log.log("Received : " + JasmineGraphInstanceProtocol::SEND_PARTITION_COUNT, "info");
                    result_wr = write(sockfd, partCount.c_str(), partCount.size());
                    if(result_wr < 0) {
                        trainer_log.log("Error writing to socket", "error");
                        return false;
                    }
                    trainer_log.log("Sent : partition count " + partCount, "info");
                    return 0;
                }
            }
        }
    } else {
        trainer_log.log("There was an error in the invoking training process and the response is :: " + response,
                        "error");
    }

    close(sockfd);
    return 0;
}
