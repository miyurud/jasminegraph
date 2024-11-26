/*
 * Copyright 2021 JasminGraph Team
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "./DataPublisher.h"

#include <pthread.h>

#include "../server/JasmineGraphInstanceProtocol.h"
#include "../util/Utils.h"
#include "../util/logger/Logger.h"

Logger data_publisher_logger;

DataPublisher::DataPublisher(int worker_port, std::string worker_address,int worker_data_port) {
    this->worker_port = worker_port;
    this->worker_address = worker_address;
    this->data_port=worker_data_port;
    struct hostent *server;

    server = gethostbyname(worker_address.c_str());
    if (server == NULL) {
        data_publisher_logger.error("ERROR, no host named " + worker_address);
        pthread_exit(NULL);
    }

    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(worker_port);
    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        data_publisher_logger.error("Socket creation error!");
    }
    if (Utils::connect_wrapper(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        data_publisher_logger.error("Connection Failed!");
    }
    data_publisher_logger.info("socket created");
}

DataPublisher::~DataPublisher() {
    Utils::send_str_wrapper(sock, JasmineGraphInstanceProtocol::CLOSE);
    close(sock);
}

void DataPublisher::publish(std::string message) {
    char receiver_buffer[MAX_STREAMING_DATA_LENGTH] = {0};

    send(this->sock, JasmineGraphInstanceProtocol::GRAPH_STREAM_START.c_str(),
         JasmineGraphInstanceProtocol::GRAPH_STREAM_START.length(), 0);

    char start_ack[ACK_MESSAGE_SIZE] = {0};
    auto ack_return_status = recv(this->sock, &start_ack, sizeof(start_ack), 0);
    std::string ack(start_ack);
    if (JasmineGraphInstanceProtocol::GRAPH_STREAM_START_ACK != ack) {
        data_publisher_logger.error("Error while receiving start command ack\n");
    }

    int message_length = message.length();
    int converted_number = htonl(message_length);
    data_publisher_logger.debug("Sending content length\n");
    send(this->sock, &converted_number, sizeof(converted_number), 0);

    int received_int = 0;
    data_publisher_logger.debug("Waiting for content length ack\n");
    auto return_status = recv(this->sock, &received_int, sizeof(received_int), 0);

    if (return_status > 0) {
        data_publisher_logger.debug("Received int =" + std::to_string(ntohl(received_int)));
    } else {
        data_publisher_logger.error("Error while receiving content length ack\n");
    }
    send(this->sock, message.c_str(), message.length(), 0);
    data_publisher_logger.debug("Edge data sent\n");
    char CRLF;
    do {
        auto return_status = recv(this->sock, &CRLF, sizeof(CRLF), 0);
        if (return_status < 1) {
            return;
        }
        if (CRLF == '\r') {
            auto return_status = recv(this->sock, &CRLF, sizeof(CRLF), 0);
            if (return_status < 1) {
                return;
            }
            if (CRLF == '\n') {
                break;
            }
        }
    } while (true);
}

void DataPublisher::publish_file_chunk(std::string fileName,std::string filePath,std::string sendingType){

    send(sock, sendingType.c_str(),
         sendingType.length(), 0);

    char start_ack[ACK_MESSAGE_SIZE] = {0};
    recv(this->sock, &start_ack, sizeof(start_ack), 0);
    std::string ack(start_ack);
    if (JasmineGraphInstanceProtocol::HDFS_STREAM_START_ACK != ack) {
        data_publisher_logger.error("Error while receiving hdfs stream start command ack\n");
        return;
    }
    data_publisher_logger.debug("Received :"+ ack);

    int fileNameLength = fileName.length();
//    int converted_number = htonl(fileNameLength);
    data_publisher_logger.debug("Sending file name length\n");
    send(this->sock, &fileNameLength, sizeof(fileNameLength), 0);

    char file_name_size_ack[ACK_MESSAGE_SIZE] = {0};
    recv(this->sock, &file_name_size_ack, sizeof(file_name_size_ack), 0);
    std::string f_name_size_ack(file_name_size_ack);
    if (JasmineGraphInstanceProtocol::HDFS_STREAM_FILE_NAME_LENGTH_ACK != f_name_size_ack) {
        data_publisher_logger.error("Error while receiving file chunk name size ack");
        return;
    }
    data_publisher_logger.debug("Received :" + f_name_size_ack);

    fileName=Utils::getFileName(fileName);
    int fileSize = Utils::getFileSize(filePath);

    //sending file name
    send(this->sock, fileName.c_str(), fileName.length(), 0);
    data_publisher_logger.debug("File chunk name sent");

    char file_name_ack[ACK_MESSAGE_SIZE] = {0};
    recv(this->sock, &file_name_ack, sizeof(file_name_ack), 0);
    std::string f_name_ack(file_name_ack);
    if (JasmineGraphInstanceProtocol::HDFS_STREAM_FILE_NAME_ACK != f_name_ack) {
        data_publisher_logger.error("Error while receiving file chunk name ack");
        return;
    }
    data_publisher_logger.debug("Received :" + f_name_ack);

    //sending file size
    data_publisher_logger.debug("Sending chunk size");
    send(this->sock, &fileSize, sizeof(fileSize), 0);

    char file_size_ack[ACK_MESSAGE_SIZE]={0};
    recv(this->sock, &file_size_ack, sizeof(file_size_ack), 0);
    std::string f_size_ack(file_size_ack);
    if (JasmineGraphInstanceProtocol::HDFS_STREAM_FILE_SIZE_ACK != f_size_ack) {
        data_publisher_logger.error("Error while receiving file chunk size ack");
        return;
    }
    data_publisher_logger.debug("Received :" + f_size_ack);


    bool isSuccess=Utils::sendFileThroughService(worker_address, data_port, fileName, filePath);
    if(isSuccess){
        data_publisher_logger.debug("Sent file successfully");
    }else{
        return;
    }

    string response;
    int count = 0;
    char data[FED_DATA_LENGTH + 1];
    while (true) {
        if (!Utils::send_str_wrapper(sock, JasmineGraphInstanceProtocol::FILE_RECV_CHK)) {
            Utils::send_str_wrapper(sock, JasmineGraphInstanceProtocol::CLOSE);
            close(sock);
            return ;
        }
        data_publisher_logger.debug("Sent: " + JasmineGraphInstanceProtocol::FILE_RECV_CHK);

        data_publisher_logger.debug("Checking if file is received");
        response = Utils::read_str_trim_wrapper(sock, data, FED_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::FILE_RECV_WAIT) == 0) {
            data_publisher_logger.debug("Received: " + JasmineGraphInstanceProtocol::FILE_RECV_WAIT);
            data_publisher_logger.debug("Checking file status : " + to_string(count));
            count++;
            sleep(1);
            continue;
        } else if (response.compare(JasmineGraphInstanceProtocol::FILE_ACK) == 0) {
            data_publisher_logger.debug("Received: " + JasmineGraphInstanceProtocol::FILE_ACK);
            data_publisher_logger.debug("File transfer completed for file : " + filePath);
            break;
        }
    }

    while (true) {
        if (!Utils::send_str_wrapper(sock, JasmineGraphInstanceProtocol::HDFS_STREAM_END_CHK)) {
            Utils::send_str_wrapper(sock, JasmineGraphInstanceProtocol::CLOSE);
            close(sock);
            return ;
        }
        data_publisher_logger.debug("Sent: " + JasmineGraphInstanceProtocol::HDFS_STREAM_END_CHK);

        response = Utils::read_str_trim_wrapper(sock, data, FED_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::HDFS_STREAM_END_WAIT) == 0) {
            data_publisher_logger.debug("Received: " + JasmineGraphInstanceProtocol::HDFS_STREAM_END_WAIT);
            sleep(1);
            continue;
        } else if (response.compare(JasmineGraphInstanceProtocol::HDFS_STREAM_END_ACK) == 0) {
            data_publisher_logger.debug("Received: " + JasmineGraphInstanceProtocol::HDFS_STREAM_END_ACK);
            data_publisher_logger.debug("Batch upload completed: " + fileName);
            break;
        }
    }

}