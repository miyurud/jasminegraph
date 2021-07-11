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

int PerformanceUtil::collectSLAResourceConsumption(std::string graphId, std::string command, std::string category,
        int iteration, int partitionCount) {
    int counter = 0;
    string slaCategoryId;
    std::vector<std::future<long>> intermRes;
    PlacesToNodeMapper placesToNodeMapper;

    std::string placeLoadQuery = "select ip, user, server_port, is_master, is_host_reporter,host_idhost,idplace from place";
    std::vector<vector<pair<string, string>>> placeList = perfDb.runSelect(placeLoadQuery);
    std::vector<vector<pair<string, string>>>::iterator placeListIterator;

    std::string categoryQuery = "SELECT id from sla_category where command='" + command + "' and category='" + category + "'";

    std::vector<vector<pair<string, string>>> categoryResults = perfDb.runSelect(categoryQuery);

    if (categoryResults.size() == 1) {
        slaCategoryId = categoryResults[0][0].second;
    } else {
        scheduler_logger.log("Invalid SLA " + category + " for " + command + " command", "error");
        return 0;
    }

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

        if (isMaster.find("true") != std::string::npos || host == "localhost") {
            collectLocalSLAResourceUtilization(isHostReporter, requestResourceAllocation,hostId,placeId, graphId,
                                               slaCategoryId, iteration, partitionCount);
        } else {
            collectRemoteSLAResourceUtilization(host,atoi(serverPort.c_str()),isHostReporter,requestResourceAllocation,
                    hostId,placeId,graphId, slaCategoryId, iteration, partitionCount);
        }
    }

    return 0;
}


std::vector<ResourceConsumption> PerformanceUtil::retrieveCurrentResourceUtilization() {

    std::string placeLoadQuery = "select ip, user, server_port, is_master, is_host_reporter,host_idhost,idplace from place";
    std::vector<vector<pair<string, string>>> placeList = perfDb.runSelect(placeLoadQuery);
    std::vector<vector<pair<string, string>>>::iterator placeListIterator;
    std::vector<ResourceConsumption> placeResourceConsumptionList;
    ResourceConsumption resourceConsumption;

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

        if ((isMaster.find("true") != std::string::npos || host == "localhost") && isHostReporter.find("true") != std::string::npos) {
            resourceConsumption = retrieveLocalResourceConsumption(host,placeId);
            placeResourceConsumptionList.push_back(resourceConsumption);
        } else if (isHostReporter.find("true") != std::string::npos) {
            resourceConsumption = retrieveRemoteResourceConsumption(host,atoi(serverPort.c_str()),hostId,placeId);
            placeResourceConsumptionList.push_back(resourceConsumption);
        }
    }

    return placeResourceConsumptionList;
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
                    std::string cpuUsage = strArr[4];
                    string vmPerformanceSql = "insert into host_performance_data (date_time, memory_usage, cpu_usage, idhost) values ('" + processTime +
                            "','" + memoryConsumption + "','"+ cpuUsage +"','" + hostId + "')";

                    perfDb.runInsert(vmPerformanceSql);

                    if (isResourceAllocationRequired == "true") {
                        std::string totalMemory = strArr[5];
                        std::string totalCores = strArr[6];
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
        std::string vmLevelStatistics = statisticCollector.collectVMStatistics(isVMStatManager, isResourceAllocationRequired);
        std::vector<std::string> strArr = Utils::split(vmLevelStatistics, ',');

        string totalMemoryUsed = strArr[0];
        string totalCPUUsage = strArr[1];

        string vmPerformanceSql = "insert into host_performance_data (date_time, memory_usage, cpu_usage, idhost) values ('" + reportTimeString +
                                  "','" + totalMemoryUsed + "','"+ totalCPUUsage +"','" + hostId + "')";

        perfDb.runInsert(vmPerformanceSql);

        if (isResourceAllocationRequired == "true") {
            std::string totalMemory = strArr[2];
            std::string totalCores = strArr[3];
            string allocationUpdateSql = "update host set total_cpu_cores='" + totalCores + "',total_memory='" + totalMemory + "' where idhost='" + hostId + "'";

            perfDb.runUpdate(allocationUpdateSql);
        }

    }

    string placePerfSql = "insert into place_performance_data (idplace, memory_usage, cpu_usage, date_time) values ('"+placeId+ "','"+ to_string(memoryUsage) +"','"+ to_string(cpuUsage) +"','"+reportTimeString+"')";

    perfDb.runInsert(placePerfSql);

}

int PerformanceUtil::collectRemoteSLAResourceUtilization(std::string host, int port, std::string isVMStatManager,
                                                         std::string isResourceAllocationRequired, std::string hostId,
                                                         std::string placeId, std::string graphId, std::string slaCategoryId,
                                                         int iteration, int partitionCount) {
    int sockfd;
    char data[300];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;
    Utils utils;
    std::string graphSlaId;

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

                if (isVMStatManager == "true" && strArr.size() > 3 && isResourceAllocationRequired == "true") {
                    std::string totalMemory = strArr[5];
                    std::string totalCores = strArr[6];
                    string allocationUpdateSql = "update host set total_cpu_cores='" + totalCores + "',total_memory='" + totalMemory + "' where idhost='" + hostId + "'";

                    perfDb.runUpdate(allocationUpdateSql);
                }

                std::string query = "SELECT id from graph_sla where graph_id='" + graphId +
                                    "' and partition_count='" + std::to_string(partitionCount) + "' and id_sla_category='" + slaCategoryId + "';";

                std::vector<vector<pair<string, string>>> results = perfDb.runSelect(query);

                if (results.size() == 1) {
                    graphSlaId = results[0][0].second;
                } else {
                    std::string insertQuery = "insert into graph_sla (id_sla_category, graph_id, partition_count, sla_value, attempt) VALUES ('" +
                                              slaCategoryId + "','" + graphId + "'," + std::to_string(partitionCount) + ",0,0);";

                    int slaId = perfDb.runInsert(insertQuery);
                    graphSlaId = std::to_string(slaId);
                }

                string slaPerfSearch = "select id, memory_usage from graph_place_sla_performance where graph_sla_id = '"
                                       + graphSlaId + "' and place_id = '" + placeId + "';";

                std::vector<vector<pair<string, string>>> slaPerfResult = perfDb.runSelect(slaPerfSearch);

                if (slaPerfResult.size() == 0) {
                    string slaPerfSql = "insert into graph_place_sla_performance (graph_sla_id, place_id, memory_usage) "
                                        "values ('" + graphSlaId + "','" + placeId + "', '" + memoryUsage + "')";

                    perfDb.runInsert(slaPerfSql);
                } else {
                    string graphPlaceSlaPerfId = slaPerfResult[0][0].second;
                    string memoryUsageString = slaPerfResult[0][1].second;

                    long currentAvgMemory = atol(memoryUsageString.c_str());
                    long currentConsumption = atol(memoryUsage.c_str());

                    long newAvgMemory = ((currentAvgMemory * iteration) + currentConsumption) / (iteration + 1);

                    string slaPerfUpdate = "update graph_place_sla_performance set memory_usage = '" + to_string(newAvgMemory) +
                            "' where id = '" + graphPlaceSlaPerfId + "';";

                    perfDb.runUpdate(slaPerfUpdate);
                }

            }
        }
    }
}

int PerformanceUtil::collectLocalSLAResourceUtilization(std::string isVMStatManager,
                                                        std::string isResourceAllocationRequired, std::string hostId,
                                                        std::string placeId, std::string graphId, std::string slaCategoryId,
                                                        int iteration, int partitionCount) {
    StatisticCollector statisticCollector;
    Utils utils;
    statisticCollector.init();
    string graphSlaId;

    int memoryUsage = statisticCollector.getMemoryUsageByProcess();
    double cpuUsage = statisticCollector.getCpuUsage();

    auto executedTime = std::chrono::system_clock::now();
    std::time_t reportTime = std::chrono::system_clock::to_time_t(executedTime);
    std::string reportTimeString(std::ctime(&reportTime));
    reportTimeString = utils.trim_copy(reportTimeString, " \f\n\r\t\v");

    if (isVMStatManager.find("true") != std::string::npos) {
        std::string vmLevelStatistics = statisticCollector.collectVMStatistics(isVMStatManager, isResourceAllocationRequired);
        std::vector<std::string> strArr = Utils::split(vmLevelStatistics, ',');

        string totalMemoryUsed = strArr[0];
        string totalCPUUsage = strArr[1];

        string vmPerformanceSql = "insert into host_performance_data (date_time, memory_usage, cpu_usage, idhost) values ('" + reportTimeString +
                                  "','" + totalMemoryUsed + "','"+ totalCPUUsage +"','" + hostId + "')";

        perfDb.runInsert(vmPerformanceSql);

        if (isResourceAllocationRequired == "true") {
            std::string totalMemory = strArr[2];
            std::string totalCores = strArr[3];
            string allocationUpdateSql = "update host set total_cpu_cores='" + totalCores + "',total_memory='" + totalMemory + "' where idhost='" + hostId + "'";

            perfDb.runUpdate(allocationUpdateSql);
        }

    }

    std::string query = "SELECT id from graph_sla where graph_id='" + graphId +
                        "' and partition_count='" + std::to_string(partitionCount) + "' and id_sla_category='" + slaCategoryId + "';";

    std::vector<vector<pair<string, string>>> results = perfDb.runSelect(query);

    if (results.size() == 1) {
        graphSlaId = results[0][0].second;
    } else {
        std::string insertQuery = "insert into graph_sla (id_sla_category, graph_id, partition_count, sla_value, attempt) VALUES ('" +
                                  slaCategoryId + "','" + graphId + "'," + std::to_string(partitionCount) + ",0,0);";

        int slaId = perfDb.runInsert(insertQuery);
        graphSlaId = std::to_string(slaId);
    }

    string slaPerfSearch = "select id, memory_usage from graph_place_sla_performance where graph_sla_id = '"
                           + graphSlaId + "' and place_id = '" + placeId + "';";

    std::vector<vector<pair<string, string>>> slaPerfResult = perfDb.runSelect(slaPerfSearch);

    if (slaPerfResult.size() == 0) {
        string slaPerfSql = "insert into graph_place_sla_performance (graph_sla_id, place_id, memory_usage) "
                            "values ('" + graphSlaId + "','" + placeId + "', '" + std::to_string(memoryUsage) + "')";

        perfDb.runInsert(slaPerfSql);
    } else {
        string graphPlaceSlaPerfId = slaPerfResult[0][0].second;
        string memoryUsageString = slaPerfResult[0][1].second;

        long currentAvgMemory = atol(memoryUsageString.c_str());
        long currentConsumption = memoryUsage;

        long newAvgMemory = ((currentAvgMemory * iteration) + currentConsumption) / (iteration + 1);

        string slaPerfUpdate = "update graph_place_sla_performance set memory_usage = '" + to_string(newAvgMemory) +
                               "' where id = '" + graphPlaceSlaPerfId + "';";

        perfDb.runUpdate(slaPerfUpdate);
    }
}

ResourceConsumption PerformanceUtil::retrieveLocalResourceConsumption(std::string host, std::string placeId) {
    ResourceConsumption placeResourceConsumption;

    StatisticCollector statisticCollector;
    Utils utils;
    statisticCollector.init();

    int memoryUsage = statisticCollector.getMemoryUsageByProcess();
    double cpuUsage = statisticCollector.getCpuUsage();

    placeResourceConsumption.memoryUsage = memoryUsage;
    placeResourceConsumption.host = host;

    return placeResourceConsumption;
}

ResourceConsumption PerformanceUtil::retrieveRemoteResourceConsumption(std::string host, int port, std::string hostId,
                                                                                    std::string placeId) {
    ResourceConsumption placeResourceConsumption;

    int sockfd;
    char data[300];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;
    Utils utils;
    std::string graphSlaId;
    std::string isVMStatManager = "false";
    std::string isResourceAllocationRequired = "false";

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot accept connection" << std::endl;
        return placeResourceConsumption;
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


                placeResourceConsumption.memoryUsage = atoi(memoryUsage.c_str());
                placeResourceConsumption.host = host;

                return placeResourceConsumption;
            }
        }
    }
}

bool PerformanceUtil::isResourcesSufficient(std::string graphId, std::string command, std::string category) {
    PerformanceUtil performanceUtil;
    performanceUtil.init();
    bool resourcesSufficient = true;
    std::set<std::string> hostSet;
    std::vector<ResourceConsumption> placeResouceConsumptionList = performanceUtil.retrieveCurrentResourceUtilization();

    string sqlStatement = "SELECT worker_idworker, name,ip,user,server_port,server_data_port,partition_idpartition "
                          "FROM worker_has_partition INNER JOIN worker ON worker_has_partition.worker_idworker=worker.idworker "
                          "WHERE partition_graph_idgraph=" + graphId + ";";

    std::vector<vector<pair<string, string>>> results = sqlLiteDB.runSelect(sqlStatement);

    int partitionCount = results.size();

    for (std::vector<vector<pair<string, string>>>::iterator i = results.begin(); i != results.end(); ++i) {
        std::vector<pair<string, string>> rowData = *i;
        string ip = rowData.at(2).second;

        hostSet.insert(ip);
    }

    for (std::set<std::string>::iterator hostSetIterator = hostSet.begin(); hostSetIterator != hostSet.end(); ++hostSetIterator) {
        std::string host = *hostSetIterator;
        std::vector<ResourceConsumption>::iterator consumptionIterator;

        for (consumptionIterator = placeResouceConsumptionList.begin(); consumptionIterator != placeResouceConsumptionList.end(); ++consumptionIterator) {
            ResourceConsumption consumption = *consumptionIterator;

            if (host == consumption.host) {
                std::string hostMemoryAllocation;
                int currentMemoryUsage = consumption.memoryUsage;

                std::string hostMemoryAllocationQuery = "select total_memory from host where ip='" + host + "';";
                std::string slaMemoryRequirementQuery = "select memory_usage from graph_place_sla_performance INNER JOIN "
                                                        "graph_sla INNER JOIN sla_category INNER JOIN place "
                                                        "ON graph_place_sla_performance.graph_sla_id=graph_sla.id AND "
                                                        "graph_sla.partition_count='" + std::to_string(partitionCount) + "' "
                                                                                                                         "AND graph_sla.id_sla_category=sla_category.id AND sla_category.command='" + command + "' "
                                                                                                                                                                                                                "AND sla_category.category='" + category + "' "
                                                                                                                                                                                                                                                           "AND graph_sla.graph_id='" + graphId + "'"
                                                                                                                                                                                                                                                                                                  "AND graph_place_sla_performance.place_id=place.idplace AND place.is_host_reporter='true';";

                std::vector<vector<pair<string, string>>> hostAllocationResults = perfDb.runSelect(hostMemoryAllocationQuery);
                std::vector<vector<pair<string, string>>> slaRequirementResults = perfDb.runSelect(slaMemoryRequirementQuery);

                if (hostAllocationResults.size() > 0 && slaRequirementResults.size() > 0) {
                    hostMemoryAllocation = hostAllocationResults[0][0].second;
                    std::string slaRequiredMemoryString = slaRequirementResults[0][0].second;

                    int hostMemory = atoi(hostMemoryAllocation.c_str());
                    int memoryRequirement = atoi(slaRequiredMemoryString.c_str());

                    if ((currentMemoryUsage + memoryRequirement) >= hostMemory) {
                        return false;
                    }

                } else {
                    scheduler_logger.log("Insufficient entries to check the feasibility. Host Allocation Size : " +
                                        std::to_string(hostAllocationResults.size()) + ". Sla Requirement: " + std::to_string(slaRequirementResults.size()), "error");
                    return false;
                }
            }
        }
    }

    return resourcesSufficient;
}