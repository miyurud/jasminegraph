/**
Copyright 2018 JasminGraph Team
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

#include <string>
#include <cmath>
#include <cctype>
#include "JasmineGraphInstanceService.h"
#include "../util/logger/Logger.h"
#include "JasmineGraphInstance.h"
#include "../server/JasmineGraphServer.h"
#include "../query/algorithms/entityresolution/BloomFilter.hpp"
#include "../query/algorithms/entityresolution/EntityResolver.hpp"
#include "../query/algorithms/entityresolution/Kmeans.hpp"
#include "../query/algorithms/entityresolution/MinHash.hpp"
#include <armadillo>

using namespace std;
using namespace arma;
Logger instance_logger;
pthread_mutex_t file_lock;
pthread_mutex_t map_lock;
StatisticCollector collector;
int JasmineGraphInstanceService::partitionCounter = 0;
std::map<int,std::vector<std::string>> JasmineGraphInstanceService::iterationData;

char *converter(const std::string &s) {
    char *pc = new char[s.size() + 1];
    std::strcpy(pc, s.c_str());
    return pc;
}

void *instanceservicesession(void *dummyPt) {
    instanceservicesessionargs *sessionargs = (instanceservicesessionargs *) dummyPt;
    int connFd = sessionargs->connFd;
    std::map<std::string,JasmineGraphHashMapLocalStore> graphDBMapLocalStores = sessionargs->graphDBMapLocalStores;
    std::map<std::string,JasmineGraphHashMapCentralStore> graphDBMapCentralStores = sessionargs->graphDBMapCentralStores;
    std::map<std::string,JasmineGraphHashMapDuplicateCentralStore> graphDBMapDuplicateCentralStores = sessionargs->graphDBMapDuplicateCentralStores;

    string serverName = sessionargs->host;
    string masterHost = sessionargs->masterHost;
    string profile = sessionargs->profile;
    int serverPort = sessionargs->port;
    int serverDataPort = sessionargs->dataPort;


    instance_logger.log("New service session started on thread " + to_string(pthread_self()), "info");
    Utils utils;
    collector.init();

    utils.createDirectory(utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder"));

    char data[INSTANCE_DATA_LENGTH];
    bool loop = false;
    while (!loop) {
        bzero(data, INSTANCE_DATA_LENGTH);
        read(connFd, data, INSTANCE_DATA_LENGTH);

        string line = (data);
        line = utils.trim_copy(line, " \f\n\r\t\v");

        Utils utils;
        line = utils.trim_copy(line, " \f\n\r\t\v");
        if (line.compare(JasmineGraphInstanceProtocol::HANDSHAKE) == 0) {
            instance_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
            write(connFd, JasmineGraphInstanceProtocol::HANDSHAKE_OK.c_str(),
                  JasmineGraphInstanceProtocol::HANDSHAKE_OK.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            line = (data);
            line = utils.trim_copy(line, " \f\n\r\t\v");
            string server_hostname = line;
            write(connFd, JasmineGraphInstanceProtocol::HOST_OK.c_str(),
                  JasmineGraphInstanceProtocol::HOST_OK.size());
            instance_logger.log("Received hostname : " + line, "info");
            std::cout << "ServerName : " << server_hostname << std::endl;
        } else if (line.compare(JasmineGraphInstanceProtocol::CLOSE) == 0) {
            write(connFd, JasmineGraphInstanceProtocol::CLOSE_ACK.c_str(),
                  JasmineGraphInstanceProtocol::CLOSE_ACK.size());
            close(connFd);
        } else if (line.compare(JasmineGraphInstanceProtocol::SHUTDOWN) == 0) {
            write(connFd, JasmineGraphInstanceProtocol::SHUTDOWN_ACK.c_str(),
                  JasmineGraphInstanceProtocol::SHUTDOWN_ACK.size());
            close(connFd);
            exit(0);
        } else if (line.compare(JasmineGraphInstanceProtocol::READY) == 0) {
            write(connFd, JasmineGraphInstanceProtocol::OK.c_str(), JasmineGraphInstanceProtocol::OK.size());
        }
        else if (line.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD) == 0) {
            instance_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD, "info");
            write(connFd, JasmineGraphInstanceProtocol::OK.c_str(), JasmineGraphInstanceProtocol::OK.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::OK, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string graphID = (data);
            graphID = utils.trim_copy(graphID, " \f\n\r\t\v");
            instance_logger.log("Received Graph ID: " + graphID, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_NAME.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_NAME.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string fileName = (data);
            instance_logger.log("Received File name: " + fileName, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_LEN.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_LEN.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string size = (data);
            //int fileSize = atoi(size.c_str());
            instance_logger.log("Received file size in bytes: " + size, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_CONT.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_CONT.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT, "info");
            string fullFilePath =
                    utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + fileName;
            int fileSize = atoi(size.c_str());
            while (true){
                if (utils.fileExists(fullFilePath)){
                    while (utils.getFileSize(fullFilePath) < fileSize) {
                        bzero(data, INSTANCE_DATA_LENGTH);
                        read(connFd, data, INSTANCE_DATA_LENGTH);
                        line = (data);
                        if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) == 0) {
                            write(connFd, JasmineGraphInstanceProtocol::FILE_RECV_WAIT.c_str(),
                                  JasmineGraphInstanceProtocol::FILE_RECV_WAIT.size());
                        }
                    }
                    break;
                }else{
                    sleep(1);
                    continue;
                }
            }
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            line = (data);

            if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) == 0) {
                instance_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_RECV_CHK, "info");
                write(connFd, JasmineGraphInstanceProtocol::FILE_ACK.c_str(),
                      JasmineGraphInstanceProtocol::FILE_ACK.size());
                instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::FILE_ACK, "info");
            }

            instance_logger.log("File received and saved to " + fullFilePath, "info");
            loop = true;

            utils.unzipFile(fullFilePath);
            size_t lastindex = fileName.find_last_of(".");
            string rawname = fileName.substr(0, lastindex);
            fullFilePath = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + rawname;

            string partitionID = rawname.substr(rawname.find_last_of("_") + 1);
            pthread_mutex_lock(&file_lock);
            writeCatalogRecord(graphID +":"+partitionID);
            pthread_mutex_unlock(&file_lock);

            while (!utils.fileExists(fullFilePath)) {
                bzero(data, INSTANCE_DATA_LENGTH);
                read(connFd, data, INSTANCE_DATA_LENGTH);
                string response = (data);
                response = utils.trim_copy(response, " \f\n\r\t\v");
                if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK) == 0) {
                    instance_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                    write(connFd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT.c_str(),
                          JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT.size());
                    instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT, "info");
                }
            }
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            line = (data);
            if (line.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK) == 0) {
                instance_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                write(connFd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK.c_str(),
                      JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK.size());
                instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK, "info");
            }

        } else if (line.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_CENTRAL) == 0) {
            instance_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CENTRAL, "info");
            write(connFd, JasmineGraphInstanceProtocol::OK.c_str(), JasmineGraphInstanceProtocol::OK.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::OK, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string graphID = (data);
            graphID = utils.trim_copy(graphID, " \f\n\r\t\v");
            instance_logger.log("Received Graph ID: " + graphID, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_NAME.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_NAME.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string fileName = (data);

            instance_logger.log("Received File name: " + fileName, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_LEN.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_LEN.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string size = (data);
            instance_logger.log("Received file size in bytes: " + size, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_CONT.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_CONT.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT, "info");
            string fullFilePath =
                    utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + fileName;

            int fileSize = atoi(size.c_str());
            while (true){
                if (utils.fileExists(fullFilePath)){
                    while (utils.getFileSize(fullFilePath) < fileSize) {
                        bzero(data, INSTANCE_DATA_LENGTH);
                        read(connFd, data, INSTANCE_DATA_LENGTH);
                        line = (data);

                        if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) == 0) {
                            write(connFd, JasmineGraphInstanceProtocol::FILE_RECV_WAIT.c_str(),
                                  JasmineGraphInstanceProtocol::FILE_RECV_WAIT.size());
                        }
                    }
                    break;
                }else{
                    sleep(1);
                    continue;
                }
            }

            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            line = (data);

            if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) == 0) {
                instance_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_RECV_CHK, "info");
                write(connFd, JasmineGraphInstanceProtocol::FILE_ACK.c_str(),
                      JasmineGraphInstanceProtocol::FILE_ACK.size());
                instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::FILE_ACK, "info");
            }

            instance_logger.log("File received and saved to " + fullFilePath, "info");
            loop = true;

            utils.unzipFile(fullFilePath);
            size_t lastindex = fileName.find_last_of(".");
            string rawname = fileName.substr(0, lastindex);
            fullFilePath = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + rawname;

            while (!utils.fileExists(fullFilePath)) {
                bzero(data, INSTANCE_DATA_LENGTH);
                read(connFd, data, INSTANCE_DATA_LENGTH);
                string response = (data);
                response = utils.trim_copy(response, " \f\n\r\t\v");
                if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK) == 0) {
                    instance_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                    write(connFd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT.c_str(),
                          JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT.size());
                    instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT, "info");
                }
            }
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            line = (data);
            if (line.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK) == 0) {
                instance_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                write(connFd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK.c_str(),
                      JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK.size());
                instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK, "info");
            }
        } else if (line.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_COMPOSITE_CENTRAL) == 0) {
            instance_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_COMPOSITE_CENTRAL, "info");
            write(connFd, JasmineGraphInstanceProtocol::OK.c_str(), JasmineGraphInstanceProtocol::OK.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::OK, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string graphID = (data);
            graphID = utils.trim_copy(graphID, " \f\n\r\t\v");
            instance_logger.log("Received Graph ID: " + graphID, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_NAME.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_NAME.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string fileName = (data);

            instance_logger.log("Received File name: " + fileName, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_LEN.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_LEN.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string size = (data);
            instance_logger.log("Received file size in bytes: " + size, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_CONT.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_CONT.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT, "info");
            string fullFilePath =
                    utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + fileName;

            int fileSize = atoi(size.c_str());
            while (true){
                if (utils.fileExists(fullFilePath)){
                    while (utils.getFileSize(fullFilePath) < fileSize) {
                        bzero(data, INSTANCE_DATA_LENGTH);
                        read(connFd, data, INSTANCE_DATA_LENGTH);
                        line = (data);

                        if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) == 0) {
                            write(connFd, JasmineGraphInstanceProtocol::FILE_RECV_WAIT.c_str(),
                                  JasmineGraphInstanceProtocol::FILE_RECV_WAIT.size());
                        }
                    }
                    break;
                }else{
                    sleep(1);
                    continue;
                }
            }

            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            line = (data);

            if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) == 0) {
                instance_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_RECV_CHK, "info");
                write(connFd, JasmineGraphInstanceProtocol::FILE_ACK.c_str(),
                      JasmineGraphInstanceProtocol::FILE_ACK.size());
                instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::FILE_ACK, "info");
            }

            instance_logger.log("File received and saved to " + fullFilePath, "info");
            loop = true;

            utils.unzipFile(fullFilePath);
            size_t lastindex = fileName.find_last_of(".");
            string rawname = fileName.substr(0, lastindex);
            fullFilePath = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + rawname;

            while (!utils.fileExists(fullFilePath)) {
                bzero(data, INSTANCE_DATA_LENGTH);
                read(connFd, data, INSTANCE_DATA_LENGTH);
                string response = (data);
                response = utils.trim_copy(response, " \f\n\r\t\v");
                if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK) == 0) {
                    instance_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                    write(connFd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT.c_str(),
                          JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT.size());
                    instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT, "info");
                }
            }
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            line = (data);
            if (line.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK) == 0) {
                instance_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                write(connFd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK.c_str(),
                      JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK.size());
                instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK, "info");
            }
        } else if (line.compare(JasmineGraphInstanceProtocol::UPLOAD_RDF_ATTRIBUTES) == 0) {
            instance_logger.log("Received : " + JasmineGraphInstanceProtocol::UPLOAD_RDF_ATTRIBUTES, "info");
            write(connFd, JasmineGraphInstanceProtocol::OK.c_str(), JasmineGraphInstanceProtocol::OK.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::OK, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string graphID = (data);
            graphID = utils.trim_copy(graphID, " \f\n\r\t\v");
            instance_logger.log("Received Graph ID: " + graphID, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_NAME.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_NAME.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string fileName = (data);
            //fileName = utils.trim_copy(fileName, " \f\n\r\t\v");
            instance_logger.log("Received File name: " + fileName, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_LEN.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_LEN.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string size = (data);
            instance_logger.log("Received file size in bytes: " + size, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_CONT.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_CONT.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT, "info");
            string fullFilePath =
                    utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + fileName;
            int fileSize = atoi(size.c_str());
            while (true){
                if (utils.fileExists(fullFilePath)){
                    while (utils.getFileSize(fullFilePath) < fileSize) {
                        bzero(data, INSTANCE_DATA_LENGTH);
                        read(connFd, data, INSTANCE_DATA_LENGTH);
                        line = (data);
                        if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) == 0) {
                            write(connFd, JasmineGraphInstanceProtocol::FILE_RECV_WAIT.c_str(),
                                  JasmineGraphInstanceProtocol::FILE_RECV_WAIT.size());
                        }
                    }
                    break;
                }else{
                    sleep(1);
                    continue;
                }

            }

            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            line = (data);

            if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) == 0) {
                instance_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_RECV_CHK, "info");
                write(connFd, JasmineGraphInstanceProtocol::FILE_ACK.c_str(),
                      JasmineGraphInstanceProtocol::FILE_ACK.size());
                instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::FILE_ACK, "info");
            }

            instance_logger.log("File received and saved to " + fullFilePath, "info");
            loop = true;

            utils.unzipFile(fullFilePath);
            size_t lastindex = fileName.find_last_of(".");
            string rawname = fileName.substr(0, lastindex);
            fullFilePath = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + rawname;

            while (!utils.fileExists(fullFilePath)) {
                bzero(data, INSTANCE_DATA_LENGTH);
                read(connFd, data, INSTANCE_DATA_LENGTH);
                string response = (data);
                response = utils.trim_copy(response, " \f\n\r\t\v");
                if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK) == 0) {
                    instance_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                    write(connFd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT.c_str(),
                          JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT.size());
                    instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT, "info");
                }
            }
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            line = (data);
            if (line.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK) == 0) {
                instance_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                write(connFd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK.c_str(),
                      JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK.size());
                instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK, "info");
            }
        }else if (line.compare(JasmineGraphInstanceProtocol::UPLOAD_RDF_ATTRIBUTES_CENTRAL) == 0) {
            instance_logger.log("Received : " + JasmineGraphInstanceProtocol::UPLOAD_RDF_ATTRIBUTES_CENTRAL, "info");
            write(connFd, JasmineGraphInstanceProtocol::OK.c_str(), JasmineGraphInstanceProtocol::OK.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::OK, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string graphID = (data);
            graphID = utils.trim_copy(graphID, " \f\n\r\t\v");
            instance_logger.log("Received Graph ID: " + graphID, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_NAME.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_NAME.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string fileName = (data);
            //fileName = utils.trim_copy(fileName, " \f\n\r\t\v");
            instance_logger.log("Received File name: " + fileName, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_LEN.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_LEN.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string size = (data);
            instance_logger.log("Received file size in bytes: " + size, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_CONT.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_CONT.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT, "info");
            string fullFilePath =
                    utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + fileName;
            int fileSize = atoi(size.c_str());
            while (true){
                if (utils.fileExists(fullFilePath)){
                    while (utils.getFileSize(fullFilePath) < fileSize) {
                        bzero(data, INSTANCE_DATA_LENGTH);
                        read(connFd, data, INSTANCE_DATA_LENGTH);
                        line = (data);
                        if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) == 0) {
                            write(connFd, JasmineGraphInstanceProtocol::FILE_RECV_WAIT.c_str(),
                                  JasmineGraphInstanceProtocol::FILE_RECV_WAIT.size());
                        }
                    }
                    break;
                }else{
                    sleep(1);
                    continue;
                }

            }

            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            line = (data);

            if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) == 0) {
                instance_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_RECV_CHK, "info");
                write(connFd, JasmineGraphInstanceProtocol::FILE_ACK.c_str(),
                      JasmineGraphInstanceProtocol::FILE_ACK.size());
                instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::FILE_ACK, "info");
            }

            instance_logger.log("File received and saved to " + fullFilePath, "info");
            loop = true;

            utils.unzipFile(fullFilePath);
            size_t lastindex = fileName.find_last_of(".");
            string rawname = fileName.substr(0, lastindex);
            fullFilePath = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + rawname;

            while (!utils.fileExists(fullFilePath)) {
                bzero(data, INSTANCE_DATA_LENGTH);
                read(connFd, data, INSTANCE_DATA_LENGTH);
                string response = (data);
                response = utils.trim_copy(response, " \f\n\r\t\v");
                if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK) == 0) {
                    instance_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                    write(connFd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT.c_str(),
                          JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT.size());
                    instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT, "info");
                }
            }
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            line = (data);
            if (line.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK) == 0) {
                instance_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                write(connFd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK.c_str(),
                      JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK.size());
                instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK, "info");
            }
        } else if (line.compare(JasmineGraphInstanceProtocol::DELETE_GRAPH) == 0) {
            instance_logger.log("Received : " + JasmineGraphInstanceProtocol::DELETE_GRAPH, "info");
            write(connFd, JasmineGraphInstanceProtocol::OK.c_str(), JasmineGraphInstanceProtocol::OK.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::OK, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string graphID = (data);
            graphID = utils.trim_copy(graphID, " \f\n\r\t\v");
            instance_logger.log("Received Graph ID: " + graphID, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_PARTITION_ID.c_str(),
                  JasmineGraphInstanceProtocol::SEND_PARTITION_ID.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_PARTITION_ID, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string partitionID = (data);
            instance_logger.log("Received partition ID: " + partitionID, "info");
            deleteGraphPartition(graphID,partitionID);
            //pthread_mutex_lock(&file_lock);
            //TODO :: Update catalog file
            //pthread_mutex_unlock(&file_lock);
            string result = "1";
            write(connFd, result.c_str(), result.size());
            instance_logger.log("Sent : " + result, "info");
        } else if (line.compare(JasmineGraphInstanceProtocol::DELETE_GRAPH_FRAGMENT) == 0) {
            //Conditional block for deleting all graph fragments when protocol is used
            instance_logger.log("Received : " + JasmineGraphInstanceProtocol::DELETE_GRAPH_FRAGMENT, "info");
            write(connFd, JasmineGraphInstanceProtocol::OK.c_str(), JasmineGraphInstanceProtocol::OK.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::OK, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            //Read the message
            read(connFd, data, INSTANCE_DATA_LENGTH);
            //Get graph ID from message
            string graphID = (data);
            graphID = utils.trim_copy(graphID, " \f\n\r\t\v");
            instance_logger.log("Received Graph ID: " + graphID, "info");
            //Method call for graph fragment deletion
            removeGraphFragments(graphID);
            //pthread_mutex_lock(&file_lock);
            //TODO :: Update catalog file
            //pthread_mutex_unlock(&file_lock);
            string result = "1";
            write(connFd, result.c_str(), result.size());
            instance_logger.log("Sent : " + result, "info");
        } else if (line.compare(JasmineGraphInstanceProtocol::TRIANGLES) == 0) {
            instance_logger.log("Received : " + JasmineGraphInstanceProtocol::TRIANGLES, "info");
            write(connFd, JasmineGraphInstanceProtocol::OK.c_str(), JasmineGraphInstanceProtocol::OK.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::OK, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string graphID = (data);
            graphID = utils.trim_copy(graphID, " \f\n\r\t\v");
            instance_logger.log("Received Graph ID: " + graphID, "info");

            write(connFd, JasmineGraphInstanceProtocol::OK.c_str(), JasmineGraphInstanceProtocol::OK.size());
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string partitionId = (data);
            partitionId = utils.trim_copy(partitionId, " \f\n\r\t\v");
            instance_logger.log("Received Partition ID: " + partitionId, "info");
            long localCount = countLocalTriangles(graphID,partitionId,graphDBMapLocalStores,graphDBMapCentralStores,graphDBMapDuplicateCentralStores);
            std::string result = to_string(localCount);
            write(connFd, result.c_str(), result.size());
        } else if (line.compare(JasmineGraphInstanceProtocol::SEND_CENTRALSTORE_TO_AGGREGATOR) == 0) {
            instance_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_CENTRALSTORE_TO_AGGREGATOR, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_NAME.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_NAME.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string fileName = (data);

            instance_logger.log("Received File name: " + fileName, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_LEN.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_LEN.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string size = (data);
            instance_logger.log("Received file size in bytes: " + size, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_CONT.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_CONT.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT, "info");
            string fullFilePath =
                    utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + fileName;

            int fileSize = atoi(size.c_str());
            while (true){
                if (utils.fileExists(fullFilePath)){
                    while (utils.getFileSize(fullFilePath) < fileSize) {
                        bzero(data, INSTANCE_DATA_LENGTH);
                        read(connFd, data, INSTANCE_DATA_LENGTH);
                        line = (data);

                        if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) == 0) {
                            write(connFd, JasmineGraphInstanceProtocol::FILE_RECV_WAIT.c_str(),
                                  JasmineGraphInstanceProtocol::FILE_RECV_WAIT.size());
                        }
                    }
                    break;
                }else{
                    sleep(1);
                    continue;
                }
            }

            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            line = (data);

            if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) == 0) {
                instance_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_RECV_CHK, "info");
                write(connFd, JasmineGraphInstanceProtocol::FILE_ACK.c_str(),
                      JasmineGraphInstanceProtocol::FILE_ACK.size());
                instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::FILE_ACK, "info");
            }

            instance_logger.log("File received and saved to " + fullFilePath, "info");
            loop = true;

            utils.unzipFile(fullFilePath);
            size_t lastindex = fileName.find_last_of(".");
            string rawname = fileName.substr(0, lastindex);
            fullFilePath = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + rawname;
            std::string aggregatorFilePath = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.aggregatefolder");

            DIR* dir = opendir(aggregatorFilePath.c_str());

            if (dir) {
                closedir(dir);
            } else {
                std::string createDirCommand = "mkdir -p " + aggregatorFilePath;
                FILE *createDirInput = popen(createDirCommand.c_str(),"r");
                pclose(createDirInput);
            }

            std::string copyCommand = "cp " + fullFilePath + " " + aggregatorFilePath;

            FILE *copyInput = popen(copyCommand.c_str(),"r");
            pclose(copyInput);

            std::string movedFullFilePath = aggregatorFilePath + "/" + rawname;

            while (!utils.fileExists(movedFullFilePath)) {
                bzero(data, INSTANCE_DATA_LENGTH);
                read(connFd, data, INSTANCE_DATA_LENGTH);
                string response = (data);
                response = utils.trim_copy(response, " \f\n\r\t\v");
                if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK) == 0) {
                    instance_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                    write(connFd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT.c_str(),
                          JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT.size());
                    instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT, "info");
                }
            }
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            line = (data);
            if (line.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK) == 0) {
                instance_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                write(connFd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK.c_str(),
                      JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK.size());
                instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK, "info");
            }
        } else if (line.compare(JasmineGraphInstanceProtocol::SEND_COMPOSITE_CENTRALSTORE_TO_AGGREGATOR) == 0) {
            instance_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_COMPOSITE_CENTRALSTORE_TO_AGGREGATOR, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_NAME.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_NAME.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string fileName = (data);

            instance_logger.log("Received File name: " + fileName, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_LEN.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_LEN.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string size = (data);
            instance_logger.log("Received file size in bytes: " + size, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_CONT.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_CONT.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT, "info");
            string fullFilePath =
                    utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + fileName;

            int fileSize = atoi(size.c_str());
            while (true){
                if (utils.fileExists(fullFilePath)){
                    while (utils.getFileSize(fullFilePath) < fileSize) {
                        bzero(data, INSTANCE_DATA_LENGTH);
                        read(connFd, data, INSTANCE_DATA_LENGTH);
                        line = (data);

                        if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) == 0) {
                            write(connFd, JasmineGraphInstanceProtocol::FILE_RECV_WAIT.c_str(),
                                  JasmineGraphInstanceProtocol::FILE_RECV_WAIT.size());
                        }
                    }
                    break;
                }else{
                    sleep(1);
                    continue;
                }
            }

            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            line = (data);

            if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) == 0) {
                instance_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_RECV_CHK, "info");
                write(connFd, JasmineGraphInstanceProtocol::FILE_ACK.c_str(),
                      JasmineGraphInstanceProtocol::FILE_ACK.size());
                instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::FILE_ACK, "info");
            }

            instance_logger.log("File received and saved to " + fullFilePath, "info");
            loop = true;

            utils.unzipFile(fullFilePath);
            size_t lastindex = fileName.find_last_of(".");
            string rawname = fileName.substr(0, lastindex);
            fullFilePath = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + rawname;
            std::string aggregatorFilePath = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.aggregatefolder");

            DIR* dir = opendir(aggregatorFilePath.c_str());

            if (dir) {
                closedir(dir);
            } else {
                std::string createDirCommand = "mkdir -p " + aggregatorFilePath;
                FILE *createDirInput = popen(createDirCommand.c_str(),"r");
                pclose(createDirInput);
            }

            std::string copyCommand = "cp " + fullFilePath + " " + aggregatorFilePath;

            FILE *copyInput = popen(copyCommand.c_str(),"r");
            pclose(copyInput);

            std::string movedFullFilePath = aggregatorFilePath + "/" + rawname;

            while (!utils.fileExists(movedFullFilePath)) {
                bzero(data, INSTANCE_DATA_LENGTH);
                read(connFd, data, INSTANCE_DATA_LENGTH);
                string response = (data);
                response = utils.trim_copy(response, " \f\n\r\t\v");
                if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK) == 0) {
                    instance_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                    write(connFd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT.c_str(),
                          JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT.size());
                    instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT, "info");
                }
            }
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            line = (data);
            if (line.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK) == 0) {
                instance_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                write(connFd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK.c_str(),
                      JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK.size());
                instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK, "info");
            }
        } else if (line.compare(JasmineGraphInstanceProtocol::AGGREGATE_CENTRALSTORE_TRIANGLES) == 0) {
            instance_logger.log("Received : " + JasmineGraphInstanceProtocol::AGGREGATE_CENTRALSTORE_TRIANGLES, "info");
            write(connFd, JasmineGraphInstanceProtocol::OK.c_str(), JasmineGraphInstanceProtocol::OK.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::OK, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string graphId = (data);
            graphId = utils.trim_copy(graphId, " \f\n\r\t\v");
            instance_logger.log("Received Graph ID: " + graphId, "info");

            write(connFd, JasmineGraphInstanceProtocol::OK.c_str(), JasmineGraphInstanceProtocol::OK.size());
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string partitionId = (data);
            partitionId = utils.trim_copy(partitionId, " \f\n\r\t\v");
            instance_logger.log("Received Partition ID: " + partitionId, "info");

            write(connFd, JasmineGraphInstanceProtocol::OK.c_str(), JasmineGraphInstanceProtocol::OK.size());
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string partitionIdList = (data);
            partitionIdList = utils.trim_copy(partitionIdList, " \f\n\r\t\v");
            instance_logger.log("Received Partition ID List : " + partitionIdList, "info");


            std::string aggregatedTriangles= JasmineGraphInstanceService::aggregateCentralStoreTriangles(graphId, partitionId, partitionIdList);

            std::vector<std::string> chunksVector;

            for (unsigned i = 0; i < aggregatedTriangles.length(); i += INSTANCE_DATA_LENGTH - 10) {
                std::string chunk = aggregatedTriangles.substr(i, INSTANCE_DATA_LENGTH - 10);
                if (i + INSTANCE_DATA_LENGTH - 10 < aggregatedTriangles.length()) {
                    chunk += "/SEND";
                } else {
                    chunk += "/CMPT";
                }
                chunksVector.push_back(chunk);
            }

            for (int loopCount = 0; loopCount < chunksVector.size(); loopCount++) {
                if (loopCount == 0) {
                    std::string chunk = chunksVector.at(loopCount);
                    write(connFd, chunk.c_str(), chunk.size());
                } else {
                    bzero(data, INSTANCE_DATA_LENGTH);
                    read(connFd, data, INSTANCE_DATA_LENGTH);
                    string chunkStatus = (data);
                    std::string chunk = chunksVector.at(loopCount);
                    write(connFd, chunk.c_str(), chunk.size());
                }
            }

        } else if (line.compare(JasmineGraphInstanceProtocol::AGGREGATE_COMPOSITE_CENTRALSTORE_TRIANGLES) == 0) {
            instance_logger.log("Received : " + JasmineGraphInstanceProtocol::AGGREGATE_COMPOSITE_CENTRALSTORE_TRIANGLES, "info");
            write(connFd, JasmineGraphInstanceProtocol::OK.c_str(), JasmineGraphInstanceProtocol::OK.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::OK, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string availableFiles = (data);
            availableFiles = utils.trim_copy(availableFiles, " \f\n\r\t\v");
            instance_logger.log("Received Available Files: " + availableFiles, "info");

            write(connFd, JasmineGraphInstanceProtocol::OK.c_str(), JasmineGraphInstanceProtocol::OK.size());
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");

            string status = response.substr(response.size() - 5);
            std::string compositeFileList = response.substr(0, response.size() - 5);

            while (status == "/SEND") {
                write(connFd, status.c_str(), status.size());
                bzero(data, 301);
                read(connFd, data, 300);
                response = (data);
                response = utils.trim_copy(response, " \f\n\r\t\v");
                status = response.substr(response.size() - 5);
                std::string fileList = response.substr(0, response.size() - 5);
                compositeFileList = compositeFileList + fileList;
            }
            response = compositeFileList;

            instance_logger.log("Received Composite File List : " + compositeFileList, "info");


            std::string aggregatedTriangles = JasmineGraphInstanceService::aggregateCompositeCentralStoreTriangles(
                    response, availableFiles);

            std::vector<std::string> chunksVector;

            for (unsigned i = 0; i < aggregatedTriangles.length(); i += INSTANCE_DATA_LENGTH - 10) {
                std::string chunk = aggregatedTriangles.substr(i, INSTANCE_DATA_LENGTH - 10);
                if (i + INSTANCE_DATA_LENGTH - 10 < aggregatedTriangles.length()) {
                    chunk += "/SEND";
                } else {
                    chunk += "/CMPT";
                }
                chunksVector.push_back(chunk);
            }

            for (int loopCount = 0; loopCount < chunksVector.size(); loopCount++) {
                if (loopCount == 0) {
                    std::string chunk = chunksVector.at(loopCount);
                    write(connFd, chunk.c_str(), chunk.size());
                } else {
                    bzero(data, INSTANCE_DATA_LENGTH);
                    read(connFd, data, INSTANCE_DATA_LENGTH);
                    string chunkStatus = (data);
                    std::string chunk = chunksVector.at(loopCount);
                    write(connFd, chunk.c_str(), chunk.size());
                }
            }

        } else if (line.compare(JasmineGraphInstanceProtocol::PERFORMANCE_STATISTICS) == 0) {
            instance_logger.log("Received : " + JasmineGraphInstanceProtocol::PERFORMANCE_STATISTICS, "info");
            write(connFd, JasmineGraphInstanceProtocol::OK.c_str(), JasmineGraphInstanceProtocol::OK.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::OK, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string isVMStatManager = (data);
            isVMStatManager = utils.trim_copy(isVMStatManager, " \f\n\r\t\v");

            write(connFd, JasmineGraphInstanceProtocol::OK.c_str(), JasmineGraphInstanceProtocol::OK.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::OK, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string isResourceAllocationRequired = (data);
            isResourceAllocationRequired = utils.trim_copy(isResourceAllocationRequired, " \f\n\r\t\v");

            std::string memoryUsage = JasmineGraphInstanceService::requestPerformanceStatistics(isVMStatManager,
                                                                                                isResourceAllocationRequired);
            write(connFd, memoryUsage.c_str(), memoryUsage.size());
        } else if (line.compare(JasmineGraphInstanceProtocol::INITIATE_TRAIN) == 0) {
            instance_logger.log("Received : " + JasmineGraphInstanceProtocol::INITIATE_TRAIN, "info");
            write(connFd, JasmineGraphInstanceProtocol::OK.c_str(), JasmineGraphInstanceProtocol::OK.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::OK, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string trainData(data);

            std::vector<std::string> trainargs = Utils::split(trainData, ' ');

            string graphID;
            string partitionID = trainargs[trainargs.size() - 1];

            for (int i = 0; i < trainargs.size(); i++) {
                if (trainargs[i] == "--graph_id") {
                    graphID = trainargs[i + 1];
                    break;
                }
            }

            std::thread *workerThreads = new std::thread[2];
            workerThreads[0] = std::thread(&JasmineGraphInstanceService::createPartitionFiles, graphID, partitionID,
                                           "local");
            workerThreads[1] = std::thread(&JasmineGraphInstanceService::createPartitionFiles, graphID, partitionID,
                                           "centralstore");

            for (int threadCount = 0; threadCount < 2; threadCount++) {
                workerThreads[threadCount].join();
                std::cout << "Thread " << threadCount << " joined" << std::endl;
            }

            write(connFd, JasmineGraphInstanceProtocol::SEND_PARTITION_ITERATION.c_str(),
                  JasmineGraphInstanceProtocol::SEND_PARTITION_ITERATION.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_PARTITION_ITERATION, "info");

            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string partIteration(data);

            write(connFd, JasmineGraphInstanceProtocol::SEND_PARTITION_COUNT.c_str(),
                  JasmineGraphInstanceProtocol::SEND_PARTITION_ITERATION.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_PARTITION_COUNT, "info");

            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string partCount(data);

            instance_logger.log("Received partition iteration - " + partIteration, "info");
            JasmineGraphInstanceService::collectExecutionData(partIteration, trainData, partCount);
            instance_logger.log("After calling collector ", "info");

        } else if (line.compare(JasmineGraphInstanceProtocol::INITIATE_PREDICT) == 0) {
            instance_logger.log("Received : " + JasmineGraphInstanceProtocol::INITIATE_PREDICT, "info");
            write(connFd, JasmineGraphInstanceProtocol::OK.c_str(), JasmineGraphInstanceProtocol::OK.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::OK, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string graphID = (data);
            graphID = utils.trim_copy(graphID, " \f\n\r\t\v");
            instance_logger.log("Received Graph ID: " + graphID, "info");

            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string vertexCount = (data);
            vertexCount = utils.trim_copy(vertexCount, " \f\n\r\t\v");
            instance_logger.log("Received vertexCount: " + vertexCount, "info");

            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string ownPartitions = (data);
            ownPartitions = utils.trim_copy(ownPartitions, " \f\n\r\t\v");
            instance_logger.log("Received Own Partitions No: " + ownPartitions, "info");

            /*Receive hosts' detail*/
            write(connFd, JasmineGraphInstanceProtocol::SEND_HOSTS.c_str(),
                  JasmineGraphInstanceProtocol::SEND_HOSTS.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_HOSTS, "info");

            char dataBuffer[INSTANCE_LONG_DATA_LENGTH];
            bzero(dataBuffer, INSTANCE_LONG_DATA_LENGTH);
            read(connFd, dataBuffer, INSTANCE_LONG_DATA_LENGTH);
            string hostList = (dataBuffer);
            instance_logger.log("Received Hosts List: " + hostList, "info");

            //Put all hosts to a map
            std::map<std::string, JasmineGraphInstanceService::workerPartitions> graphPartitionedHosts;
            std::vector<std::string> hosts = Utils::split(hostList, '|');
            int count = 0;
            int totalPartitions = 0;
            for (std::vector<std::string>::iterator it = hosts.begin();
                 it != hosts.end(); ++it) {
                if (count != 0) {
                    std::vector<std::string> hostDetail = Utils::split(*it, ',');
                    std::string hostName;
                    int port;
                    int dataport;
                    std::vector<string> partitionIDs;
                    for (std::vector<std::string>::iterator j = hostDetail.begin();
                         j != hostDetail.end(); ++j) {
                        int index = std::distance(hostDetail.begin(), j);
                        if (index == 0) {
                            hostName = *j;
                        } else if (index == 1) {
                            port = stoi(*j);
                        } else if (index == 2) {
                            dataport = stoi(*j);
                        } else {
                            partitionIDs.push_back(*j);
                            totalPartitions += 1;
                        }
                    }
                    graphPartitionedHosts.insert(
                            pair<string, JasmineGraphInstanceService::workerPartitions>(hostName,
                                                                                        {port, dataport,
                                                                                         partitionIDs}));
                }
                count++;
            }
            /*Receive file*/
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_NAME.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_NAME.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");

            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string fileName = (data);
            instance_logger.log("Received File name: " + fileName, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_LEN.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_LEN.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");

            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string size = (data);
            instance_logger.log("Received file size in bytes: " + size, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_CONT.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_CONT.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT, "info");

            string fullFilePath =
                    utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + fileName;
            int fileSize = atoi(size.c_str());
            while (utils.fileExists(fullFilePath) && utils.getFileSize(fullFilePath) < fileSize) {
                bzero(data, INSTANCE_DATA_LENGTH);
                read(connFd, data, INSTANCE_DATA_LENGTH);
                line = (data);

                if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) == 0) {
                    write(connFd, JasmineGraphInstanceProtocol::FILE_RECV_WAIT.c_str(),
                          JasmineGraphInstanceProtocol::FILE_RECV_WAIT.size());
                }
            }

            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            line = (data);

            if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) == 0) {
                instance_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_RECV_CHK, "info");
                write(connFd, JasmineGraphInstanceProtocol::FILE_ACK.c_str(),
                      JasmineGraphInstanceProtocol::FILE_ACK.size());
                instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::FILE_ACK, "info");
            }
            if (totalPartitions != 0) {
                JasmineGraphInstanceService::collectTrainedModels(sessionargs, graphID, graphPartitionedHosts,
                                                                  totalPartitions);
            }
            std::vector<std::string> predictargs;
            predictargs.push_back(graphID);
            predictargs.push_back(vertexCount);
            predictargs.push_back(fullFilePath);
            predictargs.push_back(utils.getJasmineGraphProperty("org.jasminegraph.server.instance.trainedmodelfolder"));
            predictargs.push_back(to_string(totalPartitions + stoi(ownPartitions)));
            std::vector<char *> predict_agrs_vector;
            std::transform(predictargs.begin(), predictargs.end(), std::back_inserter(predict_agrs_vector), converter);

            std::string path = "cd " + utils.getJasmineGraphProperty("org.jasminegraph.graphsage") + " && ";
            std::string command = path + "python3.5 predict.py ";

            int argc = predictargs.size();
            for (int i = 0; i < argc; ++i) {
                command += predictargs[i];
                command += " ";
            }

            cout << command << endl;
            system(command.c_str());
            loop = true;
        } else if (line.compare(JasmineGraphInstanceProtocol::INITIATE_MODEL_COLLECTION) == 0) {
            instance_logger.log("Received : " + JasmineGraphInstanceProtocol::INITIATE_MODEL_COLLECTION, "info");
            write(connFd, JasmineGraphInstanceProtocol::OK.c_str(), JasmineGraphInstanceProtocol::OK.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::OK, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string serverHostName = (data);
            serverHostName = utils.trim_copy(serverHostName, " \f\n\r\t\v");
            instance_logger.log("Received HostName: " + serverHostName, "info");

            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string serverHostPort = (data);
            serverHostPort = utils.trim_copy(serverHostPort, " \f\n\r\t\v");
            instance_logger.log("Received Port: " + serverHostPort, "info");

            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string serverHostDataPort = (data);
            serverHostDataPort = utils.trim_copy(serverHostDataPort, " \f\n\r\t\v");
            instance_logger.log("Received Data Port: " + serverHostDataPort, "info");

            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string graphID = (data);
            graphID = utils.trim_copy(graphID, " \f\n\r\t\v");
            instance_logger.log("Received Graph ID: " + graphID, "info");

            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string partitionID = (data);
            partitionID = utils.trim_copy(partitionID, " \f\n\r\t\v");
            instance_logger.log("Received Partition ID: " + partitionID, "info");

            std::string fileName = graphID + "_model_" + partitionID;
            std::string filePath =
                    utils.getJasmineGraphProperty("org.jasminegraph.server.instance.trainedmodelfolder") + "/" +
                    fileName;

            //zip the folder
            utils.compressDirectory(filePath);
            fileName = fileName + ".tar.gz";
            filePath = filePath + ".tar.gz";

            int fileSize = utils.getFileSize(filePath);
            std::string fileLength = to_string(fileSize);
            //send file name
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            line = (data);
            if (line.compare(JasmineGraphInstanceProtocol::SEND_FILE_NAME) == 0) {
                write(connFd, fileName.c_str(), fileName.size());
                instance_logger.log("Sent : File name " + fileName, "info");

                bzero(data, INSTANCE_DATA_LENGTH);
                read(connFd, data, INSTANCE_DATA_LENGTH);
                line = (data);
                //send file length
                if (line.compare(JasmineGraphInstanceProtocol::SEND_FILE_LEN) == 0) {
                    instance_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
                    write(connFd, fileLength.c_str(), fileLength.size());
                    instance_logger.log("Sent : File length in bytes " + fileLength, "info");

                    bzero(data, INSTANCE_DATA_LENGTH);
                    read(connFd, data, INSTANCE_DATA_LENGTH);
                    line = (data);
                    //send content
                    if (line.compare(JasmineGraphInstanceProtocol::SEND_FILE_CONT) == 0) {
                        instance_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT, "info");
                        instance_logger.log("Going to send file through service", "info");
                        JasmineGraphInstance::sendFileThroughService(serverHostName, stoi(serverHostDataPort), fileName,
                                                                     filePath);
                    }
                }
            }
            int count = 0;
            while (true) {
                write(connFd, JasmineGraphInstanceProtocol::FILE_RECV_CHK.c_str(),
                      JasmineGraphInstanceProtocol::FILE_RECV_CHK.size());
                instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::FILE_RECV_CHK, "info");
                instance_logger.log("Checking if file is received", "info");
                bzero(data, INSTANCE_DATA_LENGTH);
                read(connFd, data, INSTANCE_DATA_LENGTH);
                line = (data);

                if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_WAIT) == 0) {
                    instance_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_RECV_WAIT, "info");
                    instance_logger.log("Checking file status : " + to_string(count), "info");
                    count++;
                    sleep(1);
                    continue;
                } else if (line.compare(JasmineGraphInstanceProtocol::FILE_ACK) == 0) {
                    instance_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_ACK, "info");
                    instance_logger.log("File transfer completed", "info");
                    break;
                }
            }
            while (true) {
                write(connFd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.c_str(),
                      JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.size());
                instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                bzero(data, INSTANCE_DATA_LENGTH);
                read(connFd, data, INSTANCE_DATA_LENGTH);
                line = (data);

                if (line.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT) == 0) {
                    instance_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT, "info");
                    sleep(1);
                    continue;
                } else if (line.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK) == 0) {
                    instance_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK, "info");
                    instance_logger.log("Trained Model Batch upload completed", "info");
                    break;
                }
            }
            loop = true;
        } else if (line.compare(JasmineGraphInstanceProtocol::INITIATE_FRAGMENT_RESOLUTION) == 0) {
            instance_logger.log("Received : " + JasmineGraphInstanceProtocol::INITIATE_FRAGMENT_RESOLUTION, "info");
            write(connFd, JasmineGraphInstanceProtocol::OK.c_str(), JasmineGraphInstanceProtocol::OK.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::OK, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string listOfPartitions = (data);
            listOfPartitions = utils.trim_copy(listOfPartitions, " \f\n\r\t\v");
            instance_logger.log("Received ===>: " + listOfPartitions, "info");
            std::stringstream ss;
            ss << listOfPartitions;
            while (true) {
                write(connFd, JasmineGraphInstanceProtocol::FRAGMENT_RESOLUTION_CHK.c_str(),
                      JasmineGraphInstanceProtocol::FRAGMENT_RESOLUTION_CHK.size());
                instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::FRAGMENT_RESOLUTION_CHK, "info");

                bzero(data, INSTANCE_DATA_LENGTH);
                read(connFd, data, INSTANCE_DATA_LENGTH);
                string listOfPartitions = (data);

                if (listOfPartitions.compare(JasmineGraphInstanceProtocol::FRAGMENT_RESOLUTION_DONE) == 0) {
                    break;
                } else {
                    instance_logger.log("Received ===>: " + listOfPartitions, "info");
                    ss << listOfPartitions;
                }
            }
            std::vector<std::string> partitions = Utils::split(ss.str(), ',');
            std::vector<std::string> graphIDs;
            for (std::vector<string>::iterator x = partitions.begin(); x != partitions.end(); ++x) {
                string graphID = x->substr(0, x->find_first_of("_"));
                graphIDs.push_back(graphID);
            }

            Utils utils;
            string dataFolder = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
            std::vector<string> listOfFiles = utils.getListOfFilesInDirectory(dataFolder);

            std::vector<std::string> graphIDsFromFileSystem;
            for (std::vector<string>::iterator x = listOfFiles.begin(); x != listOfFiles.end(); ++x) {
                string graphID = x->substr(0, x->find_first_of("_"));
                graphIDsFromFileSystem.push_back(graphID);
            }

            std::vector<string> notInGraphIDList;

            for (std::vector<std::string>::iterator it = graphIDsFromFileSystem.begin();
                 it != graphIDsFromFileSystem.end(); it++) {
                bool found = false;
                for (std::vector<std::string>::iterator itRemoteID = graphIDs.begin();
                     itRemoteID != graphIDs.end(); itRemoteID++) {
                    if (it->compare(itRemoteID->c_str()) == 0) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    notInGraphIDList.push_back(it->c_str());
                }
            }

            string notInItemsString = "";
            std::vector<int> notInItemsList;
            for (std::vector<string>::iterator it = notInGraphIDList.begin(); it != notInGraphIDList.end(); it++) {
                if (isdigit(it->c_str()[0])) {
                    bool found = false;
                    for (std::vector<int>::iterator it2 = notInItemsList.begin(); it2 != notInItemsList.end(); it2++) {
                        if (atoi(it->c_str()) == *it2) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        notInItemsList.push_back(stoi(it->c_str()));
                    }
                }
            }

            bool firstFlag = true;
            for (std::vector<int>::iterator it = notInItemsList.begin(); it != notInItemsList.end(); it++) {
                int x = *it;
                if (firstFlag) {
                    notInItemsString = std::to_string(x);
                    firstFlag = false;
                } else {
                    notInItemsString = notInItemsString + "," + std::to_string(x);
                };
            }

            string graphIDList = notInItemsString;
            write(connFd, graphIDList.c_str(), graphIDList.size());
            instance_logger.log("Sent : " + graphIDList, "info");
        } else if (line.compare(JasmineGraphInstanceProtocol::CHECK_FILE_ACCESSIBLE) == 0) {
            instance_logger.log("Received : " + JasmineGraphInstanceProtocol::CHECK_FILE_ACCESSIBLE, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_TYPE.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_TYPE.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_TYPE, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string fileType = (data);
            fileType = utils.trim_copy(fileType, " \f\n\r\t\v");

            if (fileType.compare(JasmineGraphInstanceProtocol::FILE_TYPE_CENTRALSTORE_AGGREGATE) == 0) {
                write(connFd, JasmineGraphInstanceProtocol::OK.c_str(), JasmineGraphInstanceProtocol::OK.size());
                instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::OK, "info");
                bzero(data, INSTANCE_DATA_LENGTH);
                read(connFd, data, INSTANCE_DATA_LENGTH);
                string graphId = (data);
                graphId = utils.trim_copy(graphId, " \f\n\r\t\v");
                instance_logger.log("Received Graph ID: " + graphId, "info");

                write(connFd, JasmineGraphInstanceProtocol::OK.c_str(), JasmineGraphInstanceProtocol::OK.size());
                bzero(data, INSTANCE_DATA_LENGTH);
                read(connFd, data, INSTANCE_DATA_LENGTH);
                string partitionId = (data);
                partitionId = utils.trim_copy(partitionId, " \f\n\r\t\v");
                instance_logger.log("Received Partition ID: " + partitionId, "info");

                string aggregateLocation = utils.getJasmineGraphProperty(
                        "org.jasminegraph.server.instance.aggregatefolder");
                string fileName = graphId + "_centralstore_" + partitionId;
                string fullFilePath = aggregateLocation + "/" + fileName;
                string result = "false";

                bool fileExists = utils.fileExists(fullFilePath);

                if (fileExists) {
                    result = "true";
                }

                write(connFd, result.c_str(), result.size());
                instance_logger.log("Sent : " + result, "info");
            } else if (fileType.compare(JasmineGraphInstanceProtocol::FILE_TYPE_CENTRALSTORE_COMPOSITE) == 0) {
                write(connFd, JasmineGraphInstanceProtocol::OK.c_str(), JasmineGraphInstanceProtocol::OK.size());
                instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::OK, "info");
                bzero(data, INSTANCE_DATA_LENGTH);
                read(connFd, data, INSTANCE_DATA_LENGTH);
                string fileName = (data);
                fileName = utils.trim_copy(fileName, " \f\n\r\t\v");
                instance_logger.log("Received File name: " + fileName, "info");

                string aggregateLocation = utils.getJasmineGraphProperty(
                        "org.jasminegraph.server.instance.aggregatefolder");
                string fullFilePath = aggregateLocation + "/" + fileName;
                string result = "false";

                bool fileExists = utils.fileExists(fullFilePath);

                if (fileExists) {
                    result = "true";
                }

                write(connFd, result.c_str(), result.size());
                instance_logger.log("Sent : " + result, "info");
            }
        } else if (line.compare(JasmineGraphInstanceProtocol::CREATE_BLOOM_FILTERS) == 0) {
            instance_logger.log("Received : " + JasmineGraphInstanceProtocol::CREATE_BLOOM_FILTERS, "info");
            write(connFd, JasmineGraphInstanceProtocol::OK.c_str(), JasmineGraphInstanceProtocol::OK.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::OK, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string graphID = (data);
            graphID = utils.trim_copy(graphID, " \f\n\r\t\v");
            instance_logger.log("Received Graph ID: " + graphID, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_PARTITION_ID.c_str(),
                  JasmineGraphInstanceProtocol::SEND_PARTITION_ID.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_PARTITION_ID, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(connFd, data, INSTANCE_DATA_LENGTH);
            string partitionID = (data);
            instance_logger.log("Received partition ID: " + partitionID, "info");

            //pthread_mutex_lock(&file_lock);
            //TODO :: Update catalog file
            //pthread_mutex_unlock(&file_lock);
            string result = "1";
            write(connFd, result.c_str(), result.size());
            createFilters(graphID, partitionID);
            instance_logger.log("Sent : " + result, "info");
            //host, port
            //share created filters with hostname and port
        } else if (line.compare(JasmineGraphInstanceProtocol::BUCKET_LOCAL_CLUSTERS) == 0) {

        }
    }
    instance_logger.log("Closing thread " + to_string(pthread_self()), "info");
    close(connFd);
}

JasmineGraphInstanceService::JasmineGraphInstanceService() {

}

int
JasmineGraphInstanceService::run(string profile, string masterHost, string host, int serverPort, int serverDataPort) {
    int listenFd;
    socklen_t len;
    struct sockaddr_in svrAdd;
    struct sockaddr_in clntAdd;

    //create socket
    listenFd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenFd < 0) {
        std::cerr << "Cannot open socket" << std::endl;
        return 0;
    }

    bzero((char *) &svrAdd, sizeof(svrAdd));

    svrAdd.sin_family = AF_INET;
    svrAdd.sin_addr.s_addr = INADDR_ANY;
    svrAdd.sin_port = htons(serverPort);

    int yes = 1;

    if (setsockopt(listenFd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes) == -1) {
        perror("setsockopt");
        exit(1);
    }


    //bind socket
    if (bind(listenFd, (struct sockaddr *) &svrAdd, sizeof(svrAdd)) < 0) {
        std::cerr << "Cannot bind on port " + serverPort << std::endl;
        return 0;
    }

    listen(listenFd, 10);

    len = sizeof(clntAdd);

    int connectionCounter = 0;
    pthread_mutex_init(&file_lock, NULL);
    pthread_t threadA[MAX_CONNECTION_COUNT];

    // TODO :: What is the maximum number of connections allowed??
    instance_logger.log("Worker listening on port " + to_string(serverPort), "info");
    while (connectionCounter < MAX_CONNECTION_COUNT) {
        int connFd = accept(listenFd, (struct sockaddr *) &clntAdd, &len);
        std::map<std::string, JasmineGraphHashMapLocalStore> graphDBMapLocalStores;
        std::map<std::string, JasmineGraphHashMapCentralStore> graphDBMapCentralStores;
        std::map<std::string, JasmineGraphHashMapDuplicateCentralStore> graphDBMapDuplicateCentralStores;

        if (connFd < 0) {
            instance_logger.log("Cannot accept connection to port " + to_string(serverPort), "error");
        } else {
            instance_logger.log("Connection successful to port " + to_string(serverPort), "info");
            struct instanceservicesessionargs instanceservicesessionargs1;
            instanceservicesessionargs1.connFd = connFd;
            instanceservicesessionargs1.graphDBMapLocalStores = graphDBMapLocalStores;
            instanceservicesessionargs1.graphDBMapCentralStores = graphDBMapCentralStores;
            instanceservicesessionargs1.graphDBMapDuplicateCentralStores = graphDBMapDuplicateCentralStores;
            instanceservicesessionargs1.profile = profile;
            instanceservicesessionargs1.masterHost = masterHost;
            instanceservicesessionargs1.port = serverPort;
            instanceservicesessionargs1.dataPort = serverDataPort;
            instanceservicesessionargs1.host = host;


            pthread_create(&threadA[connectionCounter], NULL, instanceservicesession,
                           &instanceservicesessionargs1);
            //pthread_detach(threadA[connectionCounter]);
            //pthread_join(threadA[connectionCounter], NULL);
            connectionCounter++;
        }
    }

    for (int i = 0; i < connectionCounter; i++) {
        pthread_join(threadA[i], NULL);
        std::cout << "service Threads joined" << std::endl;
    }

    pthread_mutex_destroy(&file_lock);
}

void deleteGraphPartition(std::string graphID, std::string partitionID) {
    Utils utils;
    string partitionFilePath =
            utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + graphID + +"_" +
            partitionID;
    utils.deleteDirectory(partitionFilePath);
    string centalStoreFilePath =
            utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + graphID +
            +"_centralstore_" + partitionID;
    utils.deleteDirectory(centalStoreFilePath);
    string centalStoreDuplicateFilePath =
            utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + graphID +
            +"_centralstore_dp_" + partitionID;
    utils.deleteDirectory(centalStoreDuplicateFilePath);
    string attributeFilePath =
            utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + graphID +
            +"_attributes_" + partitionID;
    utils.deleteDirectory(attributeFilePath);
    string attributeCentalStoreFilePath =
            utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + graphID +
            +"_centralstore_attributes_" + partitionID;
    utils.deleteDirectory(attributeCentalStoreFilePath);
    instance_logger.log("Graph partition and centralstore files are now deleted", "info");
}

/** Method for deleting all graph fragments given a graph ID
 *
 * @param graphID ID of graph fragments to be deleted in the instance
 */
void removeGraphFragments(std::string graphID) {
    Utils utils;
    //Delete all files in the datafolder starting with the graphID
    string partitionFilePath =
            utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + graphID + "_*";
    utils.deleteDirectory(partitionFilePath);
}

void writeCatalogRecord(string record) {
    Utils utils;
    utils.createDirectory(utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder"));
    string catalogFilePath =
            utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/catalog.txt";
    ofstream outfile;
    outfile.open(catalogFilePath.c_str(), std::ios_base::app);
    outfile << record << endl;
    outfile.close();
}


long countLocalTriangles(std::string graphId, std::string partitionId,
                         std::map<std::string, JasmineGraphHashMapLocalStore> graphDBMapLocalStores,
                         std::map<std::string, JasmineGraphHashMapCentralStore> graphDBMapCentralStores,
                         std::map<std::string, JasmineGraphHashMapDuplicateCentralStore> graphDBMapDuplicateCentralStores) {
    long result;

    instance_logger.log("###INSTANCE### Local Triangle Count : Started", "info");
    std::string graphIdentifier = graphId + "_" + partitionId;
    std::string centralGraphIdentifier = graphId + +"_centralstore_" + partitionId;
    std::string duplicateCentralGraphIdentifier = graphId + +"_centralstore_dp_" + partitionId;
    JasmineGraphHashMapLocalStore graphDB;
    JasmineGraphHashMapCentralStore centralGraphDB;
    JasmineGraphHashMapDuplicateCentralStore duplicateCentralGraphDB;

    std::map<std::string, JasmineGraphHashMapLocalStore>::iterator localMapIterator = graphDBMapLocalStores.find(
            graphIdentifier);
    std::map<std::string, JasmineGraphHashMapCentralStore>::iterator centralStoreIterator = graphDBMapCentralStores.find(
            graphIdentifier);
    std::map<std::string, JasmineGraphHashMapDuplicateCentralStore>::iterator duplicateCentralStoreIterator = graphDBMapDuplicateCentralStores.find(
            graphIdentifier);


    if (localMapIterator == graphDBMapLocalStores.end()) {
        if (JasmineGraphInstanceService::isGraphDBExists(graphId, partitionId)) {
            JasmineGraphInstanceService::loadLocalStore(graphId, partitionId, graphDBMapLocalStores);
        }
        graphDB = graphDBMapLocalStores[graphIdentifier];
    } else {
        graphDB = graphDBMapLocalStores[graphIdentifier];
    }

    if (centralStoreIterator == graphDBMapCentralStores.end()) {
        if (JasmineGraphInstanceService::isInstanceCentralStoreExists(graphId, partitionId)) {
            JasmineGraphInstanceService::loadInstanceCentralStore(graphId, partitionId, graphDBMapCentralStores);
        }
        centralGraphDB = graphDBMapCentralStores[centralGraphIdentifier];
    } else {
        centralGraphDB = graphDBMapCentralStores[centralGraphIdentifier];
    }

    if (duplicateCentralStoreIterator == graphDBMapDuplicateCentralStores.end()) {
        if (JasmineGraphInstanceService::isInstanceDuplicateCentralStoreExists(graphId, partitionId)) {
            JasmineGraphInstanceService::loadInstanceDuplicateCentralStore(graphId, partitionId,
                                                                           graphDBMapDuplicateCentralStores);
        }
        duplicateCentralGraphDB = graphDBMapDuplicateCentralStores[duplicateCentralGraphIdentifier];
    } else {
        duplicateCentralGraphDB = graphDBMapDuplicateCentralStores[duplicateCentralGraphIdentifier];
    }

    result = Triangles::run(graphDB, centralGraphDB, duplicateCentralGraphDB, graphId, partitionId);

    instance_logger.log("###INSTANCE### Local Triangle Count : Completed: Triangles: " + to_string(result), "info");

    return result;

}

bool JasmineGraphInstanceService::isGraphDBExists(std::string graphId, std::string partitionId) {
    Utils utils;
    std::string dataFolder = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    std::string fileName = dataFolder + "/" + graphId + "_" + partitionId;
    std::ifstream dbFile(fileName, std::ios::binary);
    if (!dbFile) {
        return false;
    }
    return true;
}

bool JasmineGraphInstanceService::isInstanceCentralStoreExists(std::string graphId, std::string partitionId) {
    Utils utils;
    std::string dataFolder = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    std::string filename = dataFolder + "/" + graphId + +"_centralstore_" + partitionId;
    std::ifstream dbFile(filename, std::ios::binary);
    if (!dbFile) {
        return false;
    }
    return true;
}

bool JasmineGraphInstanceService::isInstanceDuplicateCentralStoreExists(std::string graphId, std::string partitionId) {
    Utils utils;
    std::string dataFolder = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    std::string filename = dataFolder + "/" + graphId + +"_centralstore_dp_" + partitionId;
    std::ifstream dbFile(filename, std::ios::binary);
    if (!dbFile) {
        return false;
    }
    return true;
}

void JasmineGraphInstanceService::loadLocalStore(std::string graphId, std::string partitionId,
                                                 std::map<std::string, JasmineGraphHashMapLocalStore> &graphDBMapLocalStores) {
    instance_logger.log("###INSTANCE### Loading Local Store : Started", "info");
    std::string graphIdentifier = graphId + "_" + partitionId;
    Utils utils;
    std::string folderLocation = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    JasmineGraphHashMapLocalStore *jasmineGraphHashMapLocalStore = new JasmineGraphHashMapLocalStore(
            atoi(graphId.c_str()), atoi(partitionId.c_str()), folderLocation);
    jasmineGraphHashMapLocalStore->loadGraph();
    graphDBMapLocalStores.insert(std::make_pair(graphIdentifier, *jasmineGraphHashMapLocalStore));
    instance_logger.log("###INSTANCE### Loading Local Store : Completed", "info");
}

void JasmineGraphInstanceService::loadInstanceCentralStore(std::string graphId, std::string partitionId,
                                                           std::map<std::string, JasmineGraphHashMapCentralStore> &graphDBMapCentralStores) {
    std::string graphIdentifier = graphId + +"_centralstore_" + partitionId;
    Utils utils;
    JasmineGraphHashMapCentralStore *jasmineGraphHashMapCentralStore = new JasmineGraphHashMapCentralStore(
            atoi(graphId.c_str()), atoi(partitionId.c_str()));
    jasmineGraphHashMapCentralStore->loadGraph();
    graphDBMapCentralStores.insert(std::make_pair(graphIdentifier, *jasmineGraphHashMapCentralStore));
}

void JasmineGraphInstanceService::loadInstanceDuplicateCentralStore(std::string graphId, std::string partitionId,
                                                                    std::map<std::string, JasmineGraphHashMapDuplicateCentralStore> &graphDBMapDuplicateCentralStores) {
    std::string graphIdentifier = graphId + +"_centralstore_dp_" + partitionId;
    Utils utils;
    JasmineGraphHashMapDuplicateCentralStore *jasmineGraphHashMapCentralStore = new JasmineGraphHashMapDuplicateCentralStore(
            atoi(graphId.c_str()), atoi(partitionId.c_str()));
    jasmineGraphHashMapCentralStore->loadGraph();
    graphDBMapDuplicateCentralStores.insert(std::make_pair(graphIdentifier, *jasmineGraphHashMapCentralStore));
}

JasmineGraphHashMapCentralStore JasmineGraphInstanceService::loadCentralStore(std::string centralStoreFileName) {
    instance_logger.log("###INSTANCE### Loading Central Store File : Started " + centralStoreFileName, "info");
    JasmineGraphHashMapCentralStore *jasmineGraphHashMapCentralStore = new JasmineGraphHashMapCentralStore();
    jasmineGraphHashMapCentralStore->loadGraph(centralStoreFileName);
    instance_logger.log("###INSTANCE### Loading Central Store File : Completed", "info");
    return *jasmineGraphHashMapCentralStore;
}

std::string JasmineGraphInstanceService::copyCentralStoreToAggregator(std::string graphId, std::string partitionId,
                                                                      std::string aggregatorHost,
                                                                      std::string aggregatorPort, std::string host) {
    Utils utils;
    char buffer[128];
    std::string result = "SUCCESS";
    std::string centralGraphIdentifier = graphId + +"_centralstore_" + partitionId;
    std::string dataFolder = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    std::string aggregatorFilePath = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.aggregatefolder");

    if (JasmineGraphInstanceService::isInstanceCentralStoreExists(graphId, partitionId)) {
        std::string centralStoreFile = dataFolder + "/" + centralGraphIdentifier;
        std::string copyCommand;

        DIR *dir = opendir(aggregatorFilePath.c_str());

        if (dir) {
            closedir(dir);
        } else {
            std::string createDirCommand = "mkdir -p " + aggregatorFilePath;
            FILE *createDirInput = popen(createDirCommand.c_str(), "r");
            pclose(createDirInput);
        }

        if (aggregatorHost == host) {
            copyCommand = "cp " + centralStoreFile + " " + aggregatorFilePath;
        } else {
            copyCommand = "scp " + centralStoreFile + " " + aggregatorHost + ":" + aggregatorFilePath;
        }

        FILE *copyInput = popen(copyCommand.c_str(), "r");

        if (copyInput) {
            // read the input
            while (!feof(copyInput)) {
                if (fgets(buffer, 128, copyInput) != NULL) {
                    result.append(buffer);
                }
            }
            if (!result.empty()) {
                std::cout << result << std::endl;
            }
            pclose(copyInput);
        }


    }

    return result;

}

std::string JasmineGraphInstanceService::aggregateCentralStoreTriangles(std::string graphId, std::string partitionId,
                                                                        std::string partitionIdList) {
    Utils utils;
    instance_logger.log("###INSTANCE### Started Aggregating Central Store Triangles", "info");
    std::string aggregatorFilePath = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.aggregatefolder");
    std::vector<std::string> fileNames;
    map<long, unordered_set<long>> aggregatedCentralStore;
    std::string centralGraphIdentifier = graphId + +"_centralstore_" + partitionId;
    std::string dataFolder = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    std::string workerCentralStoreFile = dataFolder + "/" + centralGraphIdentifier;
    instance_logger.log("###INSTANCE### Loading Central Store : Started " + workerCentralStoreFile, "info");
    JasmineGraphHashMapCentralStore workerCentralStore = JasmineGraphInstanceService::loadCentralStore(
            workerCentralStoreFile);
    instance_logger.log("###INSTANCE### Loading Central Store : Completed", "info");
    map<long, unordered_set<long>> workerCentralGraphMap = workerCentralStore.getUnderlyingHashMap();

    map<long, unordered_set<long>>::iterator workerCentalGraphIterator;

    for (workerCentalGraphIterator = workerCentralGraphMap.begin();
         workerCentalGraphIterator != workerCentralGraphMap.end(); ++workerCentalGraphIterator) {
        long startVid = workerCentalGraphIterator->first;
        unordered_set<long> endVidSet = workerCentalGraphIterator->second;

        unordered_set<long> aggregatedEndVidSet = aggregatedCentralStore[startVid];
        aggregatedEndVidSet.insert(endVidSet.begin(), endVidSet.end());
        aggregatedCentralStore[startVid] = aggregatedEndVidSet;
    }

    std::vector<std::string> paritionIdList = Utils::split(partitionIdList, ',');
    std::vector<std::string>::iterator partitionIdListIterator;

    for (partitionIdListIterator = paritionIdList.begin();
         partitionIdListIterator != paritionIdList.end(); ++partitionIdListIterator) {
        std::string aggregatePartitionId = *partitionIdListIterator;
        struct stat s;

        std::string centralGraphIdentifier = graphId + +"_centralstore_" + aggregatePartitionId;

        std::string centralStoreFile = aggregatorFilePath + "/" + centralGraphIdentifier;

        if (stat(centralStoreFile.c_str(), &s) == 0) {
            if (s.st_mode & S_IFREG) {
                JasmineGraphHashMapCentralStore centralStore = JasmineGraphInstanceService::loadCentralStore(
                        centralStoreFile);
                map<long, unordered_set<long>> centralGraphMap = centralStore.getUnderlyingHashMap();
                map<long, unordered_set<long>>::iterator centralGraphMapIterator;

                for (centralGraphMapIterator = centralGraphMap.begin();
                     centralGraphMapIterator != centralGraphMap.end(); ++centralGraphMapIterator) {
                    long startVid = centralGraphMapIterator->first;
                    unordered_set<long> endVidSet = centralGraphMapIterator->second;

                    unordered_set<long> aggregatedEndVidSet = aggregatedCentralStore[startVid];
                    aggregatedEndVidSet.insert(endVidSet.begin(), endVidSet.end());
                    aggregatedCentralStore[startVid] = aggregatedEndVidSet;
                }
            }
        }
    }


    instance_logger.log("###INSTANCE### Central Store Aggregation : Completed", "info");

    map<long, long> distributionHashMap = JasmineGraphInstanceService::getOutDegreeDistributionHashMap(
            aggregatedCentralStore);

    std::string triangles = Triangles::countCentralStoreTriangles(aggregatedCentralStore, distributionHashMap);

    return triangles;

}

string JasmineGraphInstanceService::aggregateCompositeCentralStoreTriangles(std::string compositeFileList,
                                                                            std::string availableFileList) {
    Utils utils;
    instance_logger.log("###INSTANCE### Started Aggregating Composite Central Store Triangles", "info");
    std::string aggregatorFilePath = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.aggregatefolder");
    std::string dataFolder = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    map<long, unordered_set<long>> aggregatedCompositeCentralStore;


    std::vector<std::string> compositeCentralStoreFileList = Utils::split(compositeFileList, ':');
    std::vector<std::string>::iterator compositeCentralStoreFileIterator;
    std::vector<std::string> availableCompositeFileList = Utils::split(availableFileList, ':');
    std::vector<std::string>::iterator availableCompositeFileIterator;

    for (availableCompositeFileIterator = availableCompositeFileList.begin();
         availableCompositeFileIterator != availableCompositeFileList.end(); ++availableCompositeFileIterator) {
        std::string availableCompositeFileName = *availableCompositeFileIterator;
        size_t lastindex = availableCompositeFileName.find_last_of(".");
        string rawFileName = availableCompositeFileName.substr(0, lastindex);
        struct stat st;

        std::string availableCompositeFile = dataFolder + "/" + rawFileName;

        if (stat(availableCompositeFile.c_str(), &st) == 0) {
            if (st.st_mode & S_IFREG) {
                JasmineGraphHashMapCentralStore centralStore = JasmineGraphInstanceService::loadCentralStore(
                        availableCompositeFile);
                map<long, unordered_set<long>> compositeCentralGraphMap = centralStore.getUnderlyingHashMap();
                map<long, unordered_set<long>>::iterator compositeCentralGraphMapIterator;

                for (compositeCentralGraphMapIterator = compositeCentralGraphMap.begin();
                     compositeCentralGraphMapIterator !=
                     compositeCentralGraphMap.end(); ++compositeCentralGraphMapIterator) {
                    long startVid = compositeCentralGraphMapIterator->first;
                    unordered_set<long> endVidSet = compositeCentralGraphMapIterator->second;

                    unordered_set<long> aggregatedEndVidSet = aggregatedCompositeCentralStore[startVid];
                    aggregatedEndVidSet.insert(endVidSet.begin(), endVidSet.end());
                    aggregatedCompositeCentralStore[startVid] = aggregatedEndVidSet;
                }
            }
        }
    }

    for (compositeCentralStoreFileIterator = compositeCentralStoreFileList.begin(); compositeCentralStoreFileIterator !=
                                                                                    compositeCentralStoreFileList.end(); ++compositeCentralStoreFileIterator) {
        std::string compositeCentralStoreFileName = *compositeCentralStoreFileIterator;
        size_t lastindex = compositeCentralStoreFileName.find_last_of(".");
        string rawFileName = compositeCentralStoreFileName.substr(0, lastindex);
        struct stat s;

        std::string compositeCentralStoreFile = aggregatorFilePath + "/" + rawFileName;

        if (stat(compositeCentralStoreFile.c_str(), &s) == 0) {
            if (s.st_mode & S_IFREG) {
                JasmineGraphHashMapCentralStore centralStore = JasmineGraphInstanceService::loadCentralStore(
                        compositeCentralStoreFile);
                map<long, unordered_set<long>> centralGraphMap = centralStore.getUnderlyingHashMap();
                map<long, unordered_set<long>>::iterator centralGraphMapIterator;

                for (centralGraphMapIterator = centralGraphMap.begin();
                     centralGraphMapIterator != centralGraphMap.end(); ++centralGraphMapIterator) {
                    long startVid = centralGraphMapIterator->first;
                    unordered_set<long> endVidSet = centralGraphMapIterator->second;

                    unordered_set<long> aggregatedEndVidSet = aggregatedCompositeCentralStore[startVid];
                    aggregatedEndVidSet.insert(endVidSet.begin(), endVidSet.end());
                    aggregatedCompositeCentralStore[startVid] = aggregatedEndVidSet;
                }
            }
        }
    }


    instance_logger.log("###INSTANCE### Central Store Aggregation : Completed", "info");

    map<long, long> distributionHashMap = JasmineGraphInstanceService::getOutDegreeDistributionHashMap(
            aggregatedCompositeCentralStore);

    std::string triangles = Triangles::countCentralStoreTriangles(aggregatedCompositeCentralStore, distributionHashMap);

    return triangles;
}

map<long, long> JasmineGraphInstanceService::getOutDegreeDistributionHashMap(map<long, unordered_set<long>> graphMap) {
    map<long, long> distributionHashMap;

    for (map<long, unordered_set<long>>::iterator it = graphMap.begin(); it != graphMap.end(); ++it) {
        long distribution = (it->second).size();
        distributionHashMap.insert(std::make_pair(it->first, distribution));
    }
    return distributionHashMap;
}

string JasmineGraphInstanceService::requestPerformanceStatistics(std::string isVMStatManager,
                                                                 std::string isResourceAllocationRequested) {
    Utils utils;
    int memoryUsage = collector.getMemoryUsageByProcess();
    double cpuUsage = collector.getCpuUsage();
    std::string vmLevelStatistics = collector.collectVMStatistics(isVMStatManager, isResourceAllocationRequested);
    auto executedTime = std::chrono::system_clock::now();
    std::time_t reportTime = std::chrono::system_clock::to_time_t(executedTime);
    std::string reportTimeString(std::ctime(&reportTime));
    reportTimeString = utils.trim_copy(reportTimeString, " \f\n\r\t\v");
    std::string usageString = reportTimeString + "," + to_string(memoryUsage) + "," + to_string(cpuUsage);
    if (!vmLevelStatistics.empty()) {
        usageString = usageString + "," + vmLevelStatistics;
    }
    return usageString;
}

void JasmineGraphInstanceService::collectTrainedModels(instanceservicesessionargs *sessionargs, std::string graphID,
                                                       std::map<std::string, JasmineGraphInstanceService::workerPartitions> graphPartitionedHosts,
                                                       int totalPartitions) {

    int total_threads = totalPartitions;
    std::thread *workerThreads = new std::thread[total_threads];
    int count = 0;
    std::map<std::string, JasmineGraphInstanceService::workerPartitions>::iterator mapIterator;
    for (mapIterator = graphPartitionedHosts.begin(); mapIterator != graphPartitionedHosts.end(); mapIterator++) {
        string hostName = mapIterator->first;
        JasmineGraphInstanceService::workerPartitions workerPartitions = mapIterator->second;
        std::vector<std::string>::iterator it;
        for (it = workerPartitions.partitionID.begin(); it != workerPartitions.partitionID.end(); it++) {
            workerThreads[count] = std::thread(&JasmineGraphInstanceService::collectTrainedModelThreadFunction,
                                               sessionargs, hostName, workerPartitions.port,
                                               workerPartitions.dataPort, graphID, *it);
            count++;
        }
    }

    for (int threadCount = 0; threadCount < count; threadCount++) {
        workerThreads[threadCount].join();
        std::cout << "Thread [C]: " << threadCount << " joined" << std::endl;
    }
}

int JasmineGraphInstanceService::collectTrainedModelThreadFunction(instanceservicesessionargs *sessionargs,
                                                                   std::string host, int port, int dataPort,
                                                                   std::string graphID, std::string partition) {
    Utils utils;
    bool result = true;
    std::cout << pthread_self() << " host : " << host << " port : " << port << " DPort : " << dataPort << std::endl;
    int sockfd;
    char data[INSTANCE_DATA_LENGTH];
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
    bzero(data, INSTANCE_DATA_LENGTH);
    write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());
    instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, INSTANCE_DATA_LENGTH);
    read(sockfd, data, INSTANCE_DATA_LENGTH);
    string response = (data);

    response = utils.trim_copy(response, " \f\n\r\t\v");
    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        instance_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");

        string server_host = sessionargs->host;
        write(sockfd, server_host.c_str(), server_host.size());
        instance_logger.log("Sent : " + server_host, "info");

        write(sockfd, JasmineGraphInstanceProtocol::INITIATE_MODEL_COLLECTION.c_str(),
              JasmineGraphInstanceProtocol::INITIATE_MODEL_COLLECTION.size());
        instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::INITIATE_MODEL_COLLECTION, "info");

        bzero(data, INSTANCE_DATA_LENGTH);
        read(sockfd, data, INSTANCE_DATA_LENGTH);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            instance_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");

            string server_host = sessionargs->host;
            write(sockfd, server_host.c_str(), server_host.size());
            instance_logger.log("Sent : " + server_host, "info");

            int server_port = sessionargs->port;
            write(sockfd, to_string(server_port).c_str(), to_string(server_port).size());
            instance_logger.log("Sent : " + server_port, "info");

            int server_data_port = sessionargs->dataPort;
            write(sockfd, to_string(server_data_port).c_str(), to_string(server_data_port).size());
            instance_logger.log("Sent : " + server_data_port, "info");

            write(sockfd, graphID.c_str(), (graphID).size());
            instance_logger.log("Sent : Graph ID " + graphID, "info");

            write(sockfd, partition.c_str(), (partition).size());
            instance_logger.log("Sent : Partition ID " + partition, "info");

            write(sockfd, JasmineGraphInstanceProtocol::SEND_FILE_NAME.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_NAME.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");

            bzero(data, INSTANCE_DATA_LENGTH);
            read(sockfd, data, INSTANCE_DATA_LENGTH);
            string fileName = (data);
            instance_logger.log("Received File name: " + fileName, "info");
            write(sockfd, JasmineGraphInstanceProtocol::SEND_FILE_LEN.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_LEN.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
            bzero(data, INSTANCE_DATA_LENGTH);
            read(sockfd, data, INSTANCE_DATA_LENGTH);
            string size = (data);
            instance_logger.log("Received file size in bytes: " + size, "info");

            write(sockfd, JasmineGraphInstanceProtocol::SEND_FILE_CONT.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_CONT.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT, "info");
            string fullFilePath =
                    utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + fileName;
            int fileSize = atoi(size.c_str());
            while (utils.fileExists(fullFilePath) && utils.getFileSize(fullFilePath) < fileSize) {
                bzero(data, INSTANCE_DATA_LENGTH);
                read(sockfd, data, INSTANCE_DATA_LENGTH);
                response = (data);

                if (response.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) == 0) {
                    write(sockfd, JasmineGraphInstanceProtocol::FILE_RECV_WAIT.c_str(),
                          JasmineGraphInstanceProtocol::FILE_RECV_WAIT.size());
                }
            }

            bzero(data, INSTANCE_DATA_LENGTH);
            read(sockfd, data, INSTANCE_DATA_LENGTH);
            response = (data);

            if (response.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) == 0) {
                instance_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_RECV_CHK, "info");
                write(sockfd, JasmineGraphInstanceProtocol::FILE_ACK.c_str(),
                      JasmineGraphInstanceProtocol::FILE_ACK.size());
                instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::FILE_ACK, "info");
            }

            utils.unzipDirectory(fullFilePath);
            size_t lastindex = fileName.find_last_of(".");
            string pre_rawname = fileName.substr(0, lastindex);
            size_t next_lastindex = pre_rawname.find_last_of(".");
            string rawname = fileName.substr(0, next_lastindex);
            fullFilePath = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.trainedmodelfolder") + "/" +
                           rawname;

            while (!utils.fileExists(fullFilePath)) {
                bzero(data, INSTANCE_DATA_LENGTH);
                read(sockfd, data, INSTANCE_DATA_LENGTH);
                string response = (data);
                response = utils.trim_copy(response, " \f\n\r\t\v");
                if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK) == 0) {
                    instance_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                    write(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT.c_str(),
                          JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT.size());
                    instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT, "info");
                }
            }
            bzero(data, INSTANCE_DATA_LENGTH);
            read(sockfd, data, INSTANCE_DATA_LENGTH);
            response = (data);
            if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK) == 0) {
                instance_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                write(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK.c_str(),
                      JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK.size());
                instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK, "info");
            }
        }
    } else {
        instance_logger.log("There was an error in the model collection process and the response is :: " + response,
                            "error");
    }

    close(sockfd);
    return 0;
}

void
JasmineGraphInstanceService::createPartitionFiles(std::string graphID, std::string partitionID, std::string fileType) {
    Utils utils;
    utils.createDirectory(utils.getJasmineGraphProperty("org.jasminegraph.server.instance.trainedmodelfolder"));
    JasmineGraphHashMapLocalStore *hashMapLocalStore = new JasmineGraphHashMapLocalStore();
    string inputFilePath =
            utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + graphID + "_" +
            partitionID;
    string outputFilePath =
            utils.getJasmineGraphProperty("org.jasminegraph.server.instance.trainedmodelfolder") + "/" + graphID + "_" +
            partitionID;
    if (fileType == "centralstore") {
        inputFilePath = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + graphID +
                        "_centralstore_" + partitionID;
        outputFilePath =
                utils.getJasmineGraphProperty("org.jasminegraph.server.instance.trainedmodelfolder") + "/" + graphID +
                "_centralstore_" + partitionID;
    }
    std::map<int, std::vector<int>> partEdgeMap = hashMapLocalStore->getEdgeHashMap(inputFilePath);
    if (!partEdgeMap.empty()) {
        std::ofstream localFile(outputFilePath);

        if (localFile.is_open()) {
            for (auto it = partEdgeMap.begin(); it != partEdgeMap.end(); ++it) {
                int vertex = it->first;
                std::vector<int> destinationSet = it->second;

                if (!destinationSet.empty()) {
                    for (std::vector<int>::iterator itr = destinationSet.begin(); itr != destinationSet.end(); ++itr) {
                        string edge;

                        edge = std::to_string(vertex) + " " + std::to_string((*itr));

                        localFile << edge;
                        localFile << "\n";
                    }
                }
            }
        }
        localFile.flush();
        localFile.close();
    }

}

void JasmineGraphInstanceService::collectExecutionData(string iteration, string trainArgs, string partCount) {
    pthread_mutex_lock(&map_lock);
    if (iterationData.find(stoi(iteration)) == iterationData.end()) {
        vector<string> trainData;
        trainData.push_back(trainArgs);
        iterationData[stoi(iteration)] = trainData;
    } else {
        vector<string> trainData = iterationData[stoi(iteration)];
        trainData.push_back(trainArgs);
        iterationData[stoi(iteration)] = trainData;
    }
    partitionCounter++;
    pthread_mutex_unlock(&map_lock);
    if (partitionCounter == stoi(partCount)) {
        int maxPartCountInVector = 0;
        instance_logger.log("Data collection done for all iterations", "info");
        for (auto bin = iterationData.begin(); bin != iterationData.end(); ++bin) {
            if (maxPartCountInVector < bin->second.size()) {
                maxPartCountInVector = bin->second.size();
            }
        }
        JasmineGraphInstanceService::executeTrainingIterations(maxPartCountInVector);
        return;
    } else {
        return;
    }
}

void JasmineGraphInstanceService::executeTrainingIterations(int maxThreads) {
    int iterCounter = 0;
    std::thread *threadList = new std::thread[maxThreads];
    for (auto bin = iterationData.begin(); bin != iterationData.end(); ++bin) {
        vector<string> partVector = bin->second;
        int count = 0;

        for (auto trainarg = partVector.begin(); trainarg != partVector.end(); ++trainarg) {
            string trainData = *trainarg;
            threadList[count] = std::thread(trainPartition, trainData);
            count++;
        }
        iterCounter++;
        instance_logger.log("Trainings initiated for iteration " + to_string(iterCounter), "info");
        for (int threadCount = 0; threadCount < count; threadCount++) {
            threadList[threadCount].join();
        }
        instance_logger.log("Trainings completed for iteration " + to_string(iterCounter), "info");
    }
    iterationData.clear();
    partitionCounter = 0;
}

void JasmineGraphInstanceService::trainPartition(string trainData) {
    Utils utils;
    std::vector<std::string> trainargs = Utils::split(trainData, ' ');
    string graphID;
    string partitionID = trainargs[trainargs.size() - 1];

    for (int i = 0; i < trainargs.size(); i++) {
        if (trainargs[i] == "--graph_id") {
            graphID = trainargs[i + 1];
            break;
        }
    }

    std::vector<char *> vc;
    std::transform(trainargs.begin(), trainargs.end(), std::back_inserter(vc), converter);

    std::string path = "cd " + utils.getJasmineGraphProperty("org.jasminegraph.graphsage") + " && ";
    std::string command = path + "python3 -m unsupervised_train ";

    int argc = trainargs.size();
    for (int i = 0; i < argc - 2; ++i) {
        command += trainargs[i + 2];
        command += " ";
    }
    system(command.c_str());
}


void writeBloomFiltersToFile(string filename, map<int, string> filterMap) {
    //Write bloom filters into file

    ofstream stream(filename);
    cout << "Writing filters" << endl;
    for (auto filter : filterMap) {
        stream << to_string(filter.first) << filter.second << '\n';
        // Add '\n' character  ^^^^
    }
    stream << '\n';
    stream.flush();
}

/**
 * Instance service for creating Bloom filters for a given paritiion of a given graph
 * @param graphID Corresponding graph ID
 * @param partitionID Corresponding graph ID
 * @return
 */
void createFilters(string graphID, string partitionID) {
    Utils utils;
    map<int, vector<string>> entityData;

    std::string filePath = "";
    std::string attrfilePath = "";
    std::string attrFilterPath = "";

    //Read attributes from file
    std::ifstream dataFile;
    cout << "reading file" << endl;


    filePath = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + graphID + +"_" +
               partitionID;;
    dataFile.open(filePath, std::ios::binary | std::ios::in);

    char splitter;
    string line;

    std::getline(dataFile, line);
    splitter = ' ';

    cout << "Getting data" << endl;
    while (!line.empty()) {

        int strpos = line.find(splitter);
        string nodeID = line.substr(0, strpos);
        string weakIDStr = line.substr(strpos + 1, -1);
        entityData[stoi(nodeID)] = utils.split(weakIDStr, splitter);
        cout << line << endl;


        std::getline(dataFile, line);
        while (!line.empty() && line.find_first_not_of(splitter) == std::string::npos) {
            std::getline(dataFile, line);
        }
    }
    dataFile.close();

    EntityResolver er;
    map<int, string> attrFilters = er.createFilters(entityData, 256, 4);

    attrFilterPath =
            utils.getJasmineGraphProperty("org.jasminegraph.server.instance.entityresolutionfolder") + "AttrFilters_" +
            graphID + "_" + partitionID + ".txt";

    //Write attr and struct filters separately into files
    writeBloomFiltersToFile(attrFilterPath, attrFilters);
}

map<unsigned long, set<string>> combineLocalBuckets(vector<map<unsigned long, vector<string>>> totalWorkerBuckets) {
    map<unsigned long, set<string>> combinedBuckets;
    for (auto workerBuckets: totalWorkerBuckets) {
        for (auto bucket: workerBuckets) {
            unsigned long bucketID = bucket.first;
            vector<string> clusters = bucket.second;
            set<string> existingBucket = combinedBuckets[bucketID];
            copy(clusters.begin(), clusters.end(), std::inserter(existingBucket, existingBucket.end()));
        }
    }

    return combinedBuckets;
}

/**
 * Method call for party coordinator to combine buckets of clusters given by each party to compute the similar clusters
 * Clusters that fall under the same bucket will be considered similar
 * Buckets that have clusters from all the parties will be kept since only they correspond to the possible common entities
 * across all parties
 * @param allBuckets Map of party IDs to mapping of bucket IDs to sets of clusters of that party
 * @return Map of bucket ID to clusters across all parties
 */
map<unsigned long, map<string, set<string>>>
getSimilarClusters(map<string, map<unsigned long, set<string>>> allBuckets) {
    map<unsigned long, map<string, set<string>>> combinedBuckets; //Map of bucket to organisation-cluster
    //Combine all organization buckets
    for (auto orgBuckets: allBuckets) {
        string partyID = orgBuckets.first;

        for (auto bucket: orgBuckets.second) {
            unsigned long bucketID = bucket.first;
            set<string> clusters = bucket.second;
            combinedBuckets[bucketID][partyID].insert(clusters.begin(), clusters.end());
        }
    }

    //Filter buckets
    map<unsigned long, map<string, set<string>>> filteredBuckets;

    for (auto bucket: combinedBuckets) {
        if (bucket.second.size() >= 3) {
            filteredBuckets[bucket.first] = bucket.second;
        }
    }

    return filteredBuckets;
}

/**
 * Compare filters against each other and get the most similar.
 * Classify as similar or not using a similarity threshold
 * @param selfFilters Filters coming from the party doing the computation
 * @param otherFilters Filters from the other party
 * @param similarityThreshold Similarity threshold for classification
 * @return Vector of two maps
 */
vector<map<string, string>>
compareFilters(Mat<short> &selfFilters, Mat<short> &otherFilters, float similarityThreshold = 0.9) {
    map<string, string> commonEntityMapSelf;
    map<string, string> commonEntityMapOther;

    Row<short> denominator = arma::sum(selfFilters, 0) + arma::sum(otherFilters, 0); //denominator of dice coeff

    //For each filter in self cluster, compare against filters from other clusters and determine most similar filter
    for (int i = 0; i < selfFilters.n_cols; i++) {
        //Compute dice coefficient values
        Col<short> selfFilter = selfFilters.col(i);
        Row<short> numerator =
                2 * arma::sum(otherFilters.each_col() % selfFilter, 0); //Numerator to compute the dice coeff
        Row<float> diceCoeff = (conv_to<Mat<float>>::from(numerator) / conv_to<Mat<float>>::from(denominator));

        //Get the most similar filter and check if it's meets the similarity threshold
        uword maxIndex = arma::index_max(diceCoeff); //arg max
        if (diceCoeff(maxIndex) > similarityThreshold) {
            //Assign the two filters as the same common entity
            commonEntityMapSelf[to_string(i)] = to_string(maxIndex);
            commonEntityMapOther[to_string(maxIndex)] = to_string(i);
        }

    }

    return {commonEntityMapSelf, commonEntityMapOther};
}

/**
 * @param results A vector of maps
 */
void combineFilterwiseResults(vector<map<string, string>> results) {
    map<string, string> combinedEntityMap;
    for (auto filterEntityMap: results) {
        for (auto entity: filterEntityMap) {
            combinedEntityMap[entity.first] = entity.second;
        }
    }
}

/**
 * Compute the common entities accross all parties given a chainable pairwise common entity information
 * @param pairwiseCommonEntities Map of party-ids to the mappings of common entities between other parties
 * @return A map of party ID to a vector of cluster indices that correspond to the common entities
 */
map<string, vector<string>>
synchronizeCommonEntities(map<string, map<string, map<string, string>>> pairwiseCommonEntities) {
    map<string, vector<string>> partyCommonEntityMap;
    string firstPassEndParty;

    //Get common entitiy ids of self
    string currentParty = "A"; //Self party ID
    string nextParty = pairwiseCommonEntities[currentParty].begin()->first;
    vector<string> currentPartyIds;
    for (auto idPairs: pairwiseCommonEntities[currentParty][nextParty]) {
        currentPartyIds.emplace_back(idPairs.first);
    }

    //First pass iteration through intermediate pairwise results
    for (int i = 0; i < pairwiseCommonEntities.size(); i++) {
        auto commonEntityMap = pairwiseCommonEntities[currentParty][nextParty];
        //Iterate through common entity ids of currentParty with nextParty
        vector<string> nextPartyIds;
        for (string id: currentPartyIds) {
            partyCommonEntityMap[currentParty].emplace_back(id);
            //If id is present in the next party common entities, mark it to check in the next iterationa
            if (commonEntityMap.find(id) != commonEntityMap.end()) {
                nextPartyIds.emplace_back(pairwiseCommonEntities[currentParty][nextParty][id]);
            }
        }
        //Party to iterate
        currentParty = nextParty;
        //Filtered next set of ids, when the loop terminates we will have entities of self which are common for all parties
        currentPartyIds = nextPartyIds;
        //Change pointer to next paty in the chain
        nextParty = pairwiseCommonEntities[currentParty].begin()->first;
    }

    //Second pass to compute common entities across all entities
    for (int i = 0; i < pairwiseCommonEntities.size(); i++) {
        //Replace with the entities of current party which are common for all parties
        partyCommonEntityMap[currentParty] = currentPartyIds;

        nextParty = pairwiseCommonEntities[currentParty].begin()->first;
        vector<string> nextPartyIds;
        //For only the entities common for all, get mapping entities of next party
        for (string id: currentPartyIds) {
            nextPartyIds.emplace_back(pairwiseCommonEntities[currentParty][nextParty][id]);
        }
        //Party to iterate
        currentParty = nextParty;
        //Filtered next set of ids
        currentPartyIds = nextPartyIds;
        //Change pointer to next paty in the chain
        nextParty = pairwiseCommonEntities[currentParty].begin()->first;
    }

    return partyCommonEntityMap;
}

void JasmineGraphInstanceService::entityRes(string trainData) {

    Utils utils;
    map<int, vector<string>> entityData;
    map<int, vector<int>> neighborhoodData;

    std::string filePath = "";
    std::string edgefilePath = "";
    std::string attrfilePath = "";
    std::string attrFilterPath = "";
    std::string structFilterPath = "";

    std::vector<std::string> trainargs = Utils::split(trainData, ' ');
    string graphID = trainargs[1];
    string partitionID = trainargs[2];

    //Read attributes from file
    std::ifstream dataFile;
    cout << "reading file" << endl;


    filePath = utils.getJasmineGraphProperty("org.jasminegraph.entity.location");
    dataFile.open("/home/damitha/ubuntu/entity_data/entityData.txt", std::ios::binary | std::ios::in);

    char splitter;
    string line;

    std::getline(dataFile, line);
    splitter = ' ';

    cout << "Getting data" << endl;
    while (!line.empty()) {

        int strpos = line.find(splitter);
        string nodeID = line.substr(0, strpos);
        string weakIDStr = line.substr(strpos + 1, -1);
        entityData[stoi(nodeID)] = utils.split(weakIDStr, splitter);
        cout << line << endl;


        std::getline(dataFile, line);
        while (!line.empty() && line.find_first_not_of(splitter) == std::string::npos) {
            std::getline(dataFile, line);
        }
    }
    dataFile.close();

    //Read edgelist from file
    std::ifstream edgeFile;
    std::cout << "reading file" << endl;

    edgefilePath = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.trainedmodelfolder")
                   + graphID + "_" + partitionID;
    edgeFile.open(edgefilePath);
    std::getline(edgeFile, line);

    splitter = ' ';

    cout << "Getting neighbourhood data" << endl;
    while (!line.empty()) {

        int strpos = line.find(splitter);
        string vertex1 = line.substr(0, strpos);
        string vertex2 = line.substr(strpos + 1, -1);
        if (neighborhoodData.size() <= 5) {
            neighborhoodData[stoi(vertex1)].emplace_back(stoi(vertex2));
        }


        std::getline(edgeFile, line);
        while (!line.empty() && line.find_first_not_of(splitter) == std::string::npos) {
            std::getline(edgeFile, line);
        }
    }
    edgeFile.close();

    map<int, string> attrFilters = createFilters(entityData, 256, 4);

    attrFilterPath =
            utils.getJasmineGraphProperty("org.jasminegraph.entity.location") + "attrfilters_" + graphID + "_" +
            partitionID + ".txt";
    structFilterPath =
            utils.getJasmineGraphProperty("org.jasminegraph.entity.location") + "structfilters_" + graphID + "_" +
            partitionID + ".txt";

    //Write attr and struct filters separately into files
    writeBloomFiltersToFile(attrFilterPath, attrFilters);
//    writeBloomFiltersToFile(structFilterPath, structFilters);

}

/**
 * Create attribute filters and structural filters for entities given the feature infomation and who its neighbors are
 * @param entityData A map of node IDs to a vector of string values pertaining to the weak identifiers of that node
 * @param neighborhoodData A map of node IDs to a vector its neighbors' node IDs
 * @param filterSize The length of bloom filters
 * @param numHashes  no of hash functions to use when creating the bloom filter
 * @return Map of node IDs to bloom filter string
 */
vector<map<int, string>>
JasmineGraphInstanceService::createFilters(map<int, vector<string>> entityData, map<int, vector<int>> neighborhoodData,
                                           int filterSize = 256,
                                           int numHashes = 4) {
    //Create bloom filters
    cout << "Creating filters" << endl;
    Utils utils;
    map<int, string> attrFilters;
    map<int, string> structFilters;
    //For each entity create attr and structural bloom filters
    for (const auto &entity: entityData) {
        //Create attr bloom filter
        BloomFilter attrFilter(filterSize, numHashes);
        vector<string> attributes = entity.second;
        //Add node attributes to bloom filter
        for (auto attr: attributes) {
            cout << attr << endl;
            attrFilter.insert(attr);
        }
        //Convert bloom filter to appropriate string
        string filterStr = attrFilter.m_bits.to_string();
        cout << filterStr << endl;
        filterStr = utils.replace(filterStr, "0", ",0");
        filterStr = utils.replace(filterStr, "1", ",1");
        attrFilters[entity.first] = filterStr;

        //Create structural filter
        BloomFilter structFilter(256, 4);
        //For each neighbour add selected attribute to bloom filter
        for (auto neighbour: neighborhoodData[entity.first]) {
            string selectedAttr = entityData[neighbour][0];
            structFilter.insert(selectedAttr);
        }
        //Convert bloom filter to appropriate string
        filterStr = structFilter.m_bits.to_string();
        filterStr = utils.replace(filterStr, "0", ",0");
        filterStr = utils.replace(filterStr, "1", ",1");
        structFilters[entity.first] = filterStr;
    }

    return {attrFilters, structFilters};
}

Mat<short> JasmineGraphInstanceService::generateCRVs(int minhashSize, int noClusters) { //minHash def 100
    Mat<short> CRVs(minhashSize, noClusters);
    for (int i = 0; i < noClusters; i++) {
        //Load cluster file into memory
        string filename = "/root/CLionProjects/EntityResolution/cluster" + to_string(i) + "filters.txt";
        Mat<float> clusterData;
        clusterData.load(filename, arma::csv_ascii);
        //Remove node id column
        clusterData.shed_col(0);
        inplace_trans(clusterData, "lowmem");
        //Create minhash signature of cluster
        MinHash minHash(minhashSize, 256);
        Col<short> crv = minHash.generateCRV(clusterData, 50);
        //Store in matrix
        CRVs.col(i) = crv;
    }
}

void JasmineGraphInstanceService::generateLocalCandidateSets(int filterSize = 256, int noClusters = 3) {

    Utils utils;

    Mat<short> CRVs(filterSize, noClusters);
    for (int i = 0; i < noClusters; i++) {
        //Load cluster file into memory
        string filename =
                utils.getJasmineGraphProperty("org.jasminegraph.entity.location") + to_string(i) + "filters.txt";
        Mat<float> clusterData;
        clusterData.load(filename, arma::csv_ascii);
        //Remove node id column
        clusterData.shed_col(0);
        inplace_trans(clusterData, "lowmem");
        //Create minhash signature of cluster
        MinHash minHash(100, 256);
        Col<short> crv = minHash.generateCRV(clusterData, 50);
        //Store in matrix
        CRVs.col(i) = crv;
    }

    //Generate local candidate sets
    int bandLength = 10;
    std::ostringstream s;
    hash<string> stdhash;
    map<unsigned long, vector<string>> lshBuckets;
    for (int i = 0; i < noClusters; i++) {
        Col<short> crv = CRVs.col(i);
        crv.st().raw_print(s);
        string crvStr = s.str();
        crvStr = crvStr.substr(0, (crvStr.size() > 0) ? (crvStr.size() - 1) : 0);
        crvStr.erase(remove(crvStr.begin(), crvStr.end(), ' '), crvStr.end());

        for (int j = 0; j < bandLength; j++) {
            //Select appropriate band
            string crvband = crvStr.substr(j * bandLength, (j + 1) * bandLength);
            unsigned long bucket = stdhash(crvband);
            string name = "A" + to_string(i); //Party name + cluster id
            lshBuckets[bucket].emplace_back(name);
        }
    }
}