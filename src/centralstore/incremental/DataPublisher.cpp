#include "./DataPublisher.h"

#include "../../util/logger/Logger.h"
#include "../../server/JasmineGraphInstanceProtocol.h"

Logger data_publisher_logger;

DataPublisher::DataPublisher(int worker_port, std::string worker_address) {
    this->worker_port = worker_port;
    this->worker_address = worker_address;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(worker_port);
    if (inet_pton(AF_INET, worker_address.c_str(), &serv_addr.sin_addr) <= 0) {
        data_publisher_logger.error("Invalid address/ Address not supported!");
    }
}

void DataPublisher::publish(std::string message) {
    char recever_buffer[1024] = {0};

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        data_publisher_logger.error("Socket creation error!");
    }
    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        data_publisher_logger.error("Connection Failed!");
    }
    // Send initial start sending edge command
    send(this->sock, JasmineGraphInstanceProtocol::GRAPH_STREAM_START_ACK.c_str(), JasmineGraphInstanceProtocol::GRAPH_STREAM_START_ACK.length(), 0);

    char start_ack[1024] = {0};
    auto ack_return_status = recv(this->sock, &start_ack, sizeof(start_ack), 0);
    // Receve ACK for initial start sending edge command

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
        fprintf(stdout, "Received int = %d\n", ntohl(received_int));
    } else {
        data_publisher_logger.error("Error while receiving content length ack\n");
    }
    // Sending edge data
    send(this->sock, message.c_str(), message.length(), 0);
    close(sock);
}
