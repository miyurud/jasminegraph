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

#include <cstring>
#include "JasmineGraphInstanceService.h"
#include "../util/Utils.h"
#include "../util/logger/Logger.h"
#include "../trainer/python-c-api/Python_C_API.h"

using namespace std;
Logger instance_logger;
pthread_mutex_t file_lock;

char *converter(const std::string &s) {
    char *pc = new char[s.size() + 1];
    std::strcpy(pc, s.c_str());
    return pc;
}

void *instanceservicesession(void *dummyPt) {
    instanceservicesessionargs *sessionargs = (instanceservicesessionargs *) dummyPt;
    int connFd = sessionargs->connFd;
    instance_logger.log("New service session started", "info");
    Utils utils;

    utils.createDirectory(utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder"));

    char data[300];
    bool loop = false;
    while (!loop) {
        bzero(data, 301);
        read(connFd, data, 300);

        string line = (data);

        Utils utils;
        line = utils.trim_copy(line, " \f\n\r\t\v");

        if (line.compare(JasmineGraphInstanceProtocol::HANDSHAKE) == 0) {
            instance_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
            write(connFd, JasmineGraphInstanceProtocol::HANDSHAKE_OK.c_str(),
                  JasmineGraphInstanceProtocol::HANDSHAKE_OK.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
            bzero(data, 301);
            read(connFd, data, 300);
            line = (data);
            line = utils.trim_copy(line, " \f\n\r\t\v");
            string server_hostname = line;
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
            break;
        } else if (line.compare(JasmineGraphInstanceProtocol::READY) == 0) {
            write(connFd, JasmineGraphInstanceProtocol::OK.c_str(), JasmineGraphInstanceProtocol::OK.size());
        }
        else if (line.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD) == 0) {
            instance_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD, "info");
            write(connFd, JasmineGraphInstanceProtocol::OK.c_str(), JasmineGraphInstanceProtocol::OK.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::OK, "info");
            bzero(data, 301);
            read(connFd, data, 300);
            string graphID = (data);
            graphID = utils.trim_copy(graphID, " \f\n\r\t\v");
            instance_logger.log("Received Graph ID: " + graphID, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_NAME.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_NAME.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");
            bzero(data, 301);
            read(connFd, data, 300);
            string fileName = (data);
            instance_logger.log("Received File name: " + fileName, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_LEN.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_LEN.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
            bzero(data, 301);
            read(connFd, data, 300);
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
                        bzero(data, 301);
                        read(connFd, data, 300);
                        line = (data);
                        if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) == 0) {
                            write(connFd, JasmineGraphInstanceProtocol::FILE_RECV_WAIT.c_str(),
                                  JasmineGraphInstanceProtocol::FILE_RECV_WAIT.size());
                        }
                    }
                    break;
                }else{
                    continue;
                }

            }
//            while (utils.fileExists(fullFilePath) && utils.getFileSize(fullFilePath) < fileSize) {
//                bzero(data, 301);
//                read(connFd, data, 300);
//                line = (data);
//
//                if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) == 0) {
//                    write(connFd, JasmineGraphInstanceProtocol::FILE_RECV_WAIT.c_str(),
//                          JasmineGraphInstanceProtocol::FILE_RECV_WAIT.size());
//                }
//            }
            bzero(data, 301);
            read(connFd, data, 300);
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
                bzero(data, 301);
                read(connFd, data, 300);
                string response = (data);
                response = utils.trim_copy(response, " \f\n\r\t\v");
                if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK) == 0) {
                    instance_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                    write(connFd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT.c_str(),
                          JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT.size());
                    instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT, "info");
                }
            }
            bzero(data, 301);
            read(connFd, data, 300);
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
            bzero(data, 301);
            read(connFd, data, 300);
            string graphID = (data);
            graphID = utils.trim_copy(graphID, " \f\n\r\t\v");
            instance_logger.log("Received Graph ID: " + graphID, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_NAME.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_NAME.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");
            bzero(data, 301);
            read(connFd, data, 300);
            string fileName = (data);
            //fileName = utils.trim_copy(fileName, " \f\n\r\t\v");
            instance_logger.log("Received File name: " + fileName, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_LEN.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_LEN.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
            bzero(data, 301);
            read(connFd, data, 300);
            string size = (data);
            instance_logger.log("Received file size in bytes: " + size, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_CONT.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_CONT.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT, "info");
            string fullFilePath =
                    utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + fileName;

            /*while (utils.fileExists(fullFilePath) && utils.getFileSize(fullFilePath) < fileSize) {
                bzero(data, 301);
                read(connFd, data, 300);
                line = (data);

                if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) == 0) {
                    write(connFd, JasmineGraphInstanceProtocol::FILE_RECV_WAIT.c_str(),
                          JasmineGraphInstanceProtocol::FILE_RECV_WAIT.size());
                }
            }*/
            int fileSize = atoi(size.c_str());
            while (true){
                if (utils.fileExists(fullFilePath)){
                    while (utils.getFileSize(fullFilePath) < fileSize) {
                        bzero(data, 301);
                        read(connFd, data, 300);
                        line = (data);

                        if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) == 0) {
                            write(connFd, JasmineGraphInstanceProtocol::FILE_RECV_WAIT.c_str(),
                                  JasmineGraphInstanceProtocol::FILE_RECV_WAIT.size());
                        }
                    }
                    break;
                }else{
                    continue;
                }

            }

            bzero(data, 301);
            read(connFd, data, 300);
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
                bzero(data, 301);
                read(connFd, data, 300);
                string response = (data);
                response = utils.trim_copy(response, " \f\n\r\t\v");
                if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK) == 0) {
                    instance_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                    write(connFd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT.c_str(),
                          JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT.size());
                    instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT, "info");
                }
            }
            bzero(data, 301);
            read(connFd, data, 300);
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
            bzero(data, 301);
            read(connFd, data, 300);
            string graphID = (data);
            graphID = utils.trim_copy(graphID, " \f\n\r\t\v");
            instance_logger.log("Received Graph ID: " + graphID, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_NAME.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_NAME.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");
            bzero(data, 301);
            read(connFd, data, 300);
            string fileName = (data);
            //fileName = utils.trim_copy(fileName, " \f\n\r\t\v");
            instance_logger.log("Received File name: " + fileName, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_LEN.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_LEN.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
            bzero(data, 301);
            read(connFd, data, 300);
            string size = (data);
            instance_logger.log("Received file size in bytes: " + size, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_CONT.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_CONT.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT, "info");
            string fullFilePath =
                    utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + fileName;
            int fileSize = atoi(size.c_str());
            while (utils.fileExists(fullFilePath) && utils.getFileSize(fullFilePath) < fileSize) {
                bzero(data, 301);
                read(connFd, data, 300);
                line = (data);

                if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) == 0) {
                    write(connFd, JasmineGraphInstanceProtocol::FILE_RECV_WAIT.c_str(),
                          JasmineGraphInstanceProtocol::FILE_RECV_WAIT.size());
                }
            }

            bzero(data, 301);
            read(connFd, data, 300);
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
                bzero(data, 301);
                read(connFd, data, 300);
                string response = (data);
                response = utils.trim_copy(response, " \f\n\r\t\v");
                if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK) == 0) {
                    instance_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                    write(connFd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT.c_str(),
                          JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT.size());
                    instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT, "info");
                }
            }
            bzero(data, 301);
            read(connFd, data, 300);
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
            bzero(data, 301);
            read(connFd, data, 300);
            string graphID = (data);
            graphID = utils.trim_copy(graphID, " \f\n\r\t\v");
            instance_logger.log("Received Graph ID: " + graphID, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_NAME.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_NAME.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");
            bzero(data, 301);
            read(connFd, data, 300);
            string fileName = (data);
            //fileName = utils.trim_copy(fileName, " \f\n\r\t\v");
            instance_logger.log("Received File name: " + fileName, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_LEN.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_LEN.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
            bzero(data, 301);
            read(connFd, data, 300);
            string size = (data);
            instance_logger.log("Received file size in bytes: " + size, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_FILE_CONT.c_str(),
                  JasmineGraphInstanceProtocol::SEND_FILE_CONT.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT, "info");
            string fullFilePath =
                    utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + fileName;
            int fileSize = atoi(size.c_str());
            while (utils.fileExists(fullFilePath) && utils.getFileSize(fullFilePath) < fileSize) {
                bzero(data, 301);
                read(connFd, data, 300);
                line = (data);

                if (line.compare(JasmineGraphInstanceProtocol::FILE_RECV_CHK) == 0) {
                    write(connFd, JasmineGraphInstanceProtocol::FILE_RECV_WAIT.c_str(),
                          JasmineGraphInstanceProtocol::FILE_RECV_WAIT.size());
                }
            }

            bzero(data, 301);
            read(connFd, data, 300);
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
                bzero(data, 301);
                read(connFd, data, 300);
                string response = (data);
                response = utils.trim_copy(response, " \f\n\r\t\v");
                if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK) == 0) {
                    instance_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
                    write(connFd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT.c_str(),
                          JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT.size());
                    instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT, "info");
                }
            }
            bzero(data, 301);
            read(connFd, data, 300);
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
            bzero(data, 301);
            read(connFd, data, 300);
            string graphID = (data);
            graphID = utils.trim_copy(graphID, " \f\n\r\t\v");
            instance_logger.log("Received Graph ID: " + graphID, "info");
            write(connFd, JasmineGraphInstanceProtocol::SEND_PARTITION_ID.c_str(),
                  JasmineGraphInstanceProtocol::SEND_PARTITION_ID.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_PARTITION_ID, "info");
            bzero(data, 301);
            read(connFd, data, 300);
            string partitionID = (data);
            instance_logger.log("Received partition ID: " + partitionID, "info");
            deleteGraphPartition(graphID,partitionID);
            //pthread_mutex_lock(&file_lock);
            //TODO :: Update catalog file
            //pthread_mutex_unlock(&file_lock);
            string result = "1";
            write(connFd, result.c_str(), result.size());
            instance_logger.log("Sent : " + result, "info");
            instance_logger.log("Done and dusted", "info");
        } else if (line.compare(JasmineGraphInstanceProtocol::INITIATE_TRAIN) == 0) {
            instance_logger.log("Received : " + JasmineGraphInstanceProtocol::INITIATE_TRAIN, "info");
            write(connFd, JasmineGraphInstanceProtocol::OK.c_str(), JasmineGraphInstanceProtocol::OK.size());
            instance_logger.log("Sent : " + JasmineGraphInstanceProtocol::OK, "info");
            bzero(data, 301);
            read(connFd, data, 300);
            string trainData(data);

            Utils utils;
            trainData = utils.trim_copy(trainData, " \f\n\r\t\v");

            std::vector<std::string> trainargs = Utils::split(trainData, ' ');

            std::vector<char *> vc;
            std::transform(trainargs.begin(), trainargs.end(), std::back_inserter(vc), converter);

            Python_C_API::train(vc.size(), &vc[0]);

        }
        // TODO :: Implement the rest of the protocol
        // TODO :: INSERT_EDGES,TRUNCATE,COUNT_VERTICES,COUNT_EDGES,LOADPG etc should be implemented
    }
    instance_logger.log("Closing thread " + to_string(pthread_self()), "info");
    close(connFd);
}

JasmineGraphInstanceService::JasmineGraphInstanceService() {
}

int JasmineGraphInstanceService::run(int serverPort) {

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
        std::cerr << "Cannot bind" << std::endl;
        return 0;
    }

    listen(listenFd, 10);

    len = sizeof(clntAdd);

    int connectionCounter = 0;
    pthread_mutex_init(&file_lock, NULL);
    pthread_t threadA[300];

    // TODO :: What is the maximum number of connections allowed??
    while (connectionCounter < 300) {
        instance_logger.log("Worker listening on port " + to_string(serverPort), "info");
        int connFd = accept(listenFd, (struct sockaddr *) &clntAdd, &len);

        if (connFd < 0) {
            instance_logger.log("Cannot accept connection to port " + to_string(serverPort), "error");
        } else {
            instance_logger.log("Connection successful to port " + to_string(serverPort), "info");
            struct instanceservicesessionargs instanceservicesessionargs1;
            instanceservicesessionargs1.connFd = connFd;

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
    string partitionFilePath = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + graphID + +"_"+ partitionID;
    utils.deleteDirectory(partitionFilePath);
    string centalStoreFilePath = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + graphID + +"_centralstore_"+ partitionID;
    utils.deleteDirectory(centalStoreFilePath);
    string attributeFilePath = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + graphID + +"_attributes_"+ partitionID;
    utils.deleteDirectory(attributeFilePath);
    string attributeCentalStoreFilePath = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder") + "/" + graphID + +"_centralstore_attributes_"+ partitionID;
    utils.deleteDirectory(attributeCentalStoreFilePath);
    instance_logger.log("Graph partition and centralstore files are now deleted", "info");
//    if (!utils.fileExists(partitionFilePath) and !utils.fileExists(centalStoreFilePath)){
//        result = 1;
//    }
}

void writeCatalogRecord(string record) {
    Utils utils;
    utils.createDirectory(utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder"));
    string catalogFilePath = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder")+"/catalog.txt";
    ofstream outfile;
    outfile.open(catalogFilePath.c_str(), std::ios_base::app);
    outfile << record << endl;
    outfile.close();
}
