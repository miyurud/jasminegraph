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

#include "PerformanceUtil.h"

Utils systemUtils;

Logger scheduler_logger;
SQLiteDBInterface sqlLiteDB = *new SQLiteDBInterface();
PerformanceSQLiteDBInterface perfDb = *new PerformanceSQLiteDBInterface();

int PerformanceUtil::init() {
    sqlLiteDB.init();
    perfDb.init();
}

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

            std::string sqlStatement = "select vm_manager,total_memory,cores from instance_details where host_ip=\"" + host + "\" and port="+to_string(port)+";";

            std::vector<vector<pair<string, string>>> results = perfDb.runSelect(sqlStatement);

            vector<pair<string, string>> resultEntry = results.at(0);

            std::string isReporter = resultEntry.at(0).second;
            std::string totalMemory = resultEntry.at(1).second;
            std::string totalCores = resultEntry.at(2).second;

            if (!totalMemory.empty() && totalMemory != "NULL") {
                isReporter = "false";
            }

            requestPerformanceData(host,port,isReporter);

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
        scheduler_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            //scheduler_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            write(sockfd, isVMStatManager.c_str(), isVMStatManager.size());
            scheduler_logger.log("Sent : VM Manager Status " + isVMStatManager, "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");

            scheduler_logger.log("Performance Response " + response, "info");

            std::vector<std::string> strArr = Utils::split(response, ',');

            std::string processTime = strArr[0];
            std::string memoryUsage = strArr[1];
            std::string cpuUsage = strArr[2];

            if (strArr.size() > 3) {
                std::string totalMemory = strArr[3];
                std::string totalCores = strArr[4];
                string vmUpdateSql = ("update instance_details set total_memory=\""+totalMemory+"\", cores=\""+totalCores+"\" where host_ip=\""+host+"\" and port=\""+to_string(port)+"\"");

                perfDb.runUpdate(vmUpdateSql);
            }

            string sql = ("insert into performance_data (ip_address, port, date_time, memory_usage, cpu_usage) values (\""+host+ "\",\""+to_string(port)+"\",\""+processTime+"\",\""+memoryUsage+"\",\""+cpuUsage+"\")");

            perfDb.runInsert(sql);
        }
    }
}

