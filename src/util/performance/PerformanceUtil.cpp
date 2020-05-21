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
    vector<std::string> hostList = systemUtils.getHostListFromProperties();
    int hostListSize = hostList.size();
    int counter = 0;
    std::vector<std::future<long>> intermRes;
    PlacesToNodeMapper placesToNodeMapper;

    std::string placeLoadQuery = "select ip, user, server_port, is_master, is_host_reporter,host_idhost,idplace from place";
    std::vector<vector<pair<string, string>>> placeList = perfDb.runSelect(placeLoadQuery);
    std::vector<vector<pair<string, string>>>::iterator placeListIterator;

    for (placeListIterator = placeList.begin(); placeListIterator != placeList.end(); ++placeListIterator) {
        vector<pair<string, string>> place = *placeListIterator;
        std::string host;
        std::string requestResourceAllocation = "false";

        std::string ip = place.at(0).second;
        std::string user = place.at(1).second;
        std::string serverPort = place.at(2).second;
        std::string isMaster = place.at(3).second;
        std::string isHostReporter = place.at(4).second;
        std::string hostId = place.at(5).second;
        std::string placeId = place.at(6).second;

        if (ip.find("localhost") != std::string::npos) {
            host = "localhost";
        } else {
            host = user + "@" + ip;
        }

        if (isHostReporter.find("true") != std::string::npos) {
            std::string hostSearch = "select total_cpu_cores,total_memory from host where idhost='"+hostId+"'";
            std::vector<vector<pair<string, string>>> hostAllocationList = perfDb.runSelect(hostSearch);

            vector<pair<string, string>> firstHostAllocation = hostAllocationList.at(0);

            std::string totalCPUCores = firstHostAllocation.at(0).second;
            std::string totalMemory = firstHostAllocation.at(1).second;

            if (totalCPUCores.empty() || totalMemory.empty()) {
                requestResourceAllocation = "true";
            }
        }

        if (isMaster.find("true") != std::string::npos) {
            collectLocalPerformanceData(isHostReporter, requestResourceAllocation,hostId,placeId);
        } else {
            collectRemotePerformanceData(host,atoi(serverPort.c_str()),isHostReporter,requestResourceAllocation,hostId,placeId);
        }
    }
    
    return 0;
}



int PerformanceUtil::collectRemotePerformanceData(std::string host, int port, std::string isVMStatManager, std::string isResourceAllocationRequired, std::string hostId, std::string placeId) {
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

            if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
                write(sockfd, isResourceAllocationRequired.c_str(), isResourceAllocationRequired.size());
                scheduler_logger.log("Sent : Resource Allocation Requested " + isResourceAllocationRequired, "info");

                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);
                response = utils.trim_copy(response, " \f\n\r\t\v");

                scheduler_logger.log("Performance Response " + response, "info");

                std::vector<std::string> strArr = Utils::split(response, ',');

                std::string processTime = strArr[0];
                std::string memoryUsage = strArr[1];
                std::string cpuUsage = strArr[2];

                if (isVMStatManager == "true" && strArr.size() > 3) {
                    std::string memoryConsumption = strArr[3];
                    string vmPerformanceSql = "insert into host_performance_data (date_time, memory_usage, cpu_usage, idhost) values ('" + processTime +
                            "','" + memoryConsumption + "','','" + hostId + "')";

                    perfDb.runInsert(vmPerformanceSql);

                    if (isResourceAllocationRequired == "true") {
                        std::string totalMemory = strArr[4];
                        std::string totalCores = strArr[5];
                        string allocationUpdateSql = "update host set total_cpu_cores='" + totalCores + "',total_memory='" + totalMemory + "' where idhost='" + hostId + "'";

                        perfDb.runUpdate(allocationUpdateSql);
                    }
                }

                string placePerfSql = "insert into place_performance_data (idplace, memory_usage, cpu_usage, date_time) values ('"+placeId+ "','"+memoryUsage+"','"+cpuUsage+"','"+processTime+"')";

                perfDb.runInsert(placePerfSql);
            }
        }
    }
}

int PerformanceUtil::collectLocalPerformanceData(std::string isVMStatManager, std::string isResourceAllocationRequired, std::string hostId, std::string placeId) {
    StatisticCollector statisticCollector;
    Utils utils;
    statisticCollector.init();

    int memoryUsage = statisticCollector.getMemoryUsageByProcess();
    double cpuUsage = statisticCollector.getCpuUsage();

    auto executedTime = std::chrono::system_clock::now();
    std::time_t reportTime = std::chrono::system_clock::to_time_t(executedTime);
    std::string reportTimeString(std::ctime(&reportTime));
    reportTimeString = utils.trim_copy(reportTimeString, " \f\n\r\t\v");

    if (isVMStatManager.find("true") != std::string::npos) {
        std::string vmLevelStatistics = statisticCollector.collectVMStatistics(isVMStatManager, std::string());
        std::vector<std::string> strArr = Utils::split(vmLevelStatistics, ',');

        std::string hostPerfInsertQuery = "insert into host_performance_data ()";
    }

}

