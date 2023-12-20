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

#include "../../server/JasmineGraphInstanceProtocol.h"
#include "../../util/Utils.h"
#include "../../util/logger/Logger.h"

Logger data_publisher_logger;

DataPublisher::DataPublisher(int worker_port, std::string worker_address) {
    this->worker_port = worker_port;
    this->worker_address = worker_address;
    struct hostent *server;

    server = gethostbyname(worker_address.c_str());
    if (server == NULL) {
        std::cerr << "ERROR, no host named " << server << std::endl;
        exit(0);
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
}

DataPublisher::~DataPublisher() { close(sock); }

void DataPublisher::publish(std::string message) {
    char recever_buffer[MAX_STREAMING_DATA_LENGTH] = {0};

    // Send initial start sending edge command
    send(this->sock, JasmineGraphInstanceProtocol::GRAPH_STREAM_START.c_str(),
         JasmineGraphInstanceProtocol::GRAPH_STREAM_START.length(), 0);

    char start_ack[1024] = {0};
    // Wait to receve an ACK for initial start sending edge command
    auto ack_return_status = recv(this->sock, &start_ack, sizeof(start_ack), 0);
    std::string ack(start_ack);
    std::cout << ack << std::endl;
    if (JasmineGraphInstanceProtocol::GRAPH_STREAM_START_ACK != ack) {
        data_publisher_logger.error("Error while receiving start command ack\n");
    }

    int message_length = message.length();
    int converted_number = htonl(message_length);
    data_publisher_logger.info("Sending content length\n");
    // Sending edge data content length
    send(this->sock, &converted_number, sizeof(converted_number), 0);

    int received_int = 0;
    data_publisher_logger.info("Waiting for content length ack\n");
    auto return_status = recv(this->sock, &received_int, sizeof(received_int), 0);
    // Receve ack for edge data content length

    if (return_status > 0) {
        data_publisher_logger.info("Received int =" + std::to_string(ntohl(received_int)));
    } else {
        data_publisher_logger.error("Error while receiving content length ack\n");
    }
    // Sending edge data
    send(this->sock, message.c_str(), message.length(), 0);
    data_publisher_logger.info("Edge data sent\n");
    char CRLF;
    do {
        // read a single byte
        auto return_status = recv(this->sock, &CRLF, sizeof(CRLF), 0);
        if (return_status < 1) {
            // error or disconnect
            return;
        }

        // has end of line been reached?
        if (CRLF == '\r') {
            // read a single byte
            auto return_status = recv(this->sock, &CRLF, sizeof(CRLF), 0);
            if (return_status < 1) {
                // error or disconnect
                return;
            }
            if (CRLF == '\n') {
                break;  // yes
            }
        }
    } while (true);
}
