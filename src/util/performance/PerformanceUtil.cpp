//
// Created by chinthaka on 8/24/19.
//

#include "PerformanceUtil.h"

Utils systemUtils;

Logger scheduler_logger;
SQLiteDBInterface sqlLiteDB = *new SQLiteDBInterface();
PerformanceSQLiteDBInterface perfDb = *new PerformanceSQLiteDBInterface();

int PerformanceUtil::collectPerformanceStatistics() {
    vector<std::string> hostList = systemUtils.getHostList();
    int hostListSize = hostList.size();
    int counter = 0;
    std::vector<std::future<long>> intermRes;
    PlacesToNodeMapper placesToNodeMapper;


    for (int i=0;i<hostListSize;i++) {
        int k = counter;
        string host = placesToNodeMapper.getHost(i);
        std::vector<int> instancePorts = placesToNodeMapper.getInstancePort(i);
        string partitionId;

        std::vector<int>::iterator portsIterator;

        for (portsIterator = instancePorts.begin(); portsIterator != instancePorts.end(); ++portsIterator) {
            int port = *portsIterator;

            std::string sqlStatement = "select vm_manager from instance_details where host_ip=" + host + "and port="+to_string(port)+";";

            std::vector<vector<pair<string, string>>> results = sqlLiteDB.runSelect(sqlStatement);

            vector<pair<string, string>> resultEntry = results.at(0);

            string isReporter = resultEntry.at(0).second;

            int memoryUsage = requestPerformanceData(host,port,isReporter);

        }

    }


    return 0;
}



int PerformanceUtil::requestPerformanceData(std::string host, int port, std::string isVMStatManager) {
    int sockfd;
    char data[300];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;
    Utils utils;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot accept connection" << std::endl;
        return 0;
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
    write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());
    scheduler_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        scheduler_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        string server_host = utils.getJasmineGraphProperty("org.jasminegraph.server.host");
        write(sockfd, server_host.c_str(), server_host.size());
        scheduler_logger.log("Sent : " + server_host, "info");

        write(sockfd, JasmineGraphInstanceProtocol::PERFORMANCE_STATISTICS.c_str(),
              JasmineGraphInstanceProtocol::PERFORMANCE_STATISTICS.size());
        scheduler_logger.log("Sent : " + JasmineGraphInstanceProtocol::PERFORMANCE_STATISTICS, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            scheduler_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            write(sockfd, isVMStatManager.c_str(), isVMStatManager.size());
            scheduler_logger.log("Sent : Graph ID " + isVMStatManager, "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");
        }
    }
}