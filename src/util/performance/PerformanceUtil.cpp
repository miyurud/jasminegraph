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
using namespace std::chrono;
std::map<std::string,std::vector<ResourceUsageInfo>> resourceUsageMap;

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

std::vector<Place> PerformanceUtil::getHostReporterList() {
    std::vector<Place> hostReporterList;

    std::string placeLoadQuery = "select ip, user, server_port, is_master, is_host_reporter,host_idhost,idplace from place"
                                 " where is_host_reporter='true'";
    std::vector<vector<pair<string, string>>> placeList = perfDb.runSelect(placeLoadQuery);
    std::vector<vector<pair<string, string>>>::iterator placeListIterator;

    for (placeListIterator = placeList.begin(); placeListIterator != placeList.end(); ++placeListIterator) {
        vector<pair<string, string>> place = *placeListIterator;
        Place placeInfo;
        std::string host;
        std::string requestResourceAllocation = "false";

        placeInfo.ip = place.at(0).second;
        placeInfo.user = place.at(1).second;
        placeInfo.serverPort = place.at(2).second;
        placeInfo.isMaster = place.at(3).second;
        placeInfo.isHostReporter = place.at(4).second;
        placeInfo.hostId = place.at(5).second;
        placeInfo.placeId = place.at(6).second;

        hostReporterList.push_back(placeInfo);
    }

    return hostReporterList;

}

int PerformanceUtil::collectSLAResourceConsumption(std::vector<Place> placeList, std::string graphId,
        std::string masterIP, int elapsedTime) {
    /*string slaCategoryId;
    std::vector<std::future<long>> intermRes;
    PlacesToNodeMapper placesToNodeMapper;

    std::string placeLoadQuery = "select ip, user, server_port, is_master, is_host_reporter,host_idhost,idplace from place"
                                 " where is_host_reporter='true'";
    std::vector<vector<pair<string, string>>> placeList = perfDb.runSelect(placeLoadQuery);
    std::vector<vector<pair<string, string>>>::iterator placeListIterator;

    std::string categoryQuery = "SELECT id from sla_category where command='" + command + "' and category='" + category + "'";

    std::vector<vector<pair<string, string>>> categoryResults = perfDb.runSelect(categoryQuery);

    if (categoryResults.size() == 1) {
        slaCategoryId = categoryResults[0][0].second;
    } else {
        scheduler_logger.log("Invalid SLA " + category + " for " + command + " command", "error");
        return 0;
    }*/

    std::vector<Place>::iterator placeListIterator;

    for (placeListIterator = placeList.begin(); placeListIterator != placeList.end(); ++placeListIterator) {
        Place place = *placeListIterator;
        std::string host;
        std::string requestResourceAllocation = "false";

        std::string ip = place.ip;
        std::string user = place.user;
        std::string serverPort = place.serverPort;
        std::string isMaster = place.isMaster;
        std::string isHostReporter = place.isHostReporter;
        std::string hostId = place.hostId;
        std::string placeId = place.placeId;

        if (ip.find("localhost") != std::string::npos || ip.compare(masterIP) == 0) {
            host = ip;
        } else {
            host = user + "@" + ip;
        }

        /*if (isHostReporter.find("true") != std::string::npos) {
            std::string hostSearch = "select total_cpu_cores,total_memory from host where idhost='"+hostId+"'";
            std::vector<vector<pair<string, string>>> hostAllocationList = perfDb.runSelect(hostSearch);

            vector<pair<string, string>> firstHostAllocation = hostAllocationList.at(0);

            std::string totalCPUCores = firstHostAllocation.at(0).second;
            std::string totalMemory = firstHostAllocation.at(1).second;

            if (totalCPUCores.empty() || totalMemory.empty()) {
                requestResourceAllocation = "true";
            }
        }*/

        if (isMaster.find("true") != std::string::npos || host == "localhost" || host.compare(masterIP) == 0) {
            collectLocalSLAResourceUtilization(placeId, elapsedTime);
        } else {
            //collectRemoteSLAResourceUtilization(host,atoi(serverPort.c_str()),isHostReporter,requestResourceAllocation,
             //       placeId,elapsedTime, masterIP);
        }
    }

    return 0;
}


std::vector<ResourceConsumption> PerformanceUtil::retrieveCurrentResourceUtilization(std::string masterIP) {

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

        if (ip.find("localhost") != std::string::npos|| ip.compare(masterIP) == 0) {
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

        if ((isMaster.find("true") != std::string::npos || host == "localhost" || host.compare(masterIP) == 0) &&
                                                                isHostReporter.find("true") != std::string::npos) {
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
                std::string loadAverage = strArr[3];

                if (isVMStatManager == "true" && strArr.size() > 4) {
                    std::string memoryConsumption = strArr[4];
                    std::string cpuUsage = strArr[5];
                    string vmPerformanceSql = "insert into host_performance_data (date_time, memory_usage, cpu_usage, idhost) values ('" + processTime +
                            "','" + memoryConsumption + "','"+ cpuUsage +"','" + hostId + "')";

                    perfDb.runInsert(vmPerformanceSql);

                    if (isResourceAllocationRequired == "true") {
                        std::string totalMemory = strArr[6];
                        std::string totalCores = strArr[7];
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
                                                         std::string isResourceAllocationRequired, std::string placeId,
                                                         int elapsedTime, std::string masterIP) {
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
        write(sockfd, masterIP.c_str(), masterIP.size());
        scheduler_logger.log("Sent : " + masterIP, "info");

        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");
        scheduler_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            write(sockfd, JasmineGraphInstanceProtocol::PERFORMANCE_STATISTICS.c_str(),
                  JasmineGraphInstanceProtocol::PERFORMANCE_STATISTICS.size());
            scheduler_logger.log("Sent : " + JasmineGraphInstanceProtocol::PERFORMANCE_STATISTICS, "info");
            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");
            scheduler_logger.log("Received : " + response, "info");

            if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
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
                    std::string loadAverage = strArr[3];

                    ResourceUsageInfo resourceUsageInfo;
                    resourceUsageInfo.elapsedTime = std::to_string(elapsedTime);
                    resourceUsageInfo.loadAverage = loadAverage;
                    resourceUsageInfo.memoryUsage = memoryUsage;


                    if (!resourceUsageMap[placeId].empty()) {
                        resourceUsageMap[placeId].push_back(resourceUsageInfo);
                    } else {
                        std::vector<ResourceUsageInfo> resourceUsageVector;

                        resourceUsageVector.push_back(resourceUsageInfo);

                        resourceUsageMap[placeId] = resourceUsageVector;
                    }

                    /*if (isVMStatManager == "true" && strArr.size() > 3 && isResourceAllocationRequired == "true") {
                        std::string totalMemory = strArr[6];
                        std::string totalCores = strArr[7];
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

                    string slaPerfSql = "insert into graph_place_sla_performance (graph_sla_id, place_id, memory_usage, load_average, elapsed_time) "
                                        "values ('" + graphSlaId + "','" + placeId + "', '" + memoryUsage + "','" +
                                        loadAverage + "',' " + std::to_string(elapsedTime) + " ')";

                    perfDb.runInsert(slaPerfSql);*/


                }
            }
        }

    }
}

int PerformanceUtil::collectLocalSLAResourceUtilization(std::string placeId, int elapsedTime) {
    StatisticCollector statisticCollector;
    Utils utils;
    statisticCollector.init();
    string graphSlaId;

    //int memoryUsage = statisticCollector.getMemoryUsageByProcess();
    //double cpuUsage = statisticCollector.getCpuUsage();
    double loadAgerage = statisticCollector.getLoadAverage();

    auto executedTime = std::chrono::system_clock::now();
    std::time_t reportTime = std::chrono::system_clock::to_time_t(executedTime);
    std::string reportTimeString(std::ctime(&reportTime));
    reportTimeString = utils.trim_copy(reportTimeString, " \f\n\r\t\v");

    ResourceUsageInfo resourceUsageInfo;
    resourceUsageInfo.elapsedTime = std::to_string(elapsedTime);
    resourceUsageInfo.loadAverage = std::to_string(loadAgerage);
    //resourceUsageInfo.memoryUsage = std::to_string(memoryUsage);

    std::cout << "###PERF### CURRENT LOAD: " + std::to_string(loadAgerage) << std::endl;

    if (!resourceUsageMap[placeId].empty()) {
        resourceUsageMap[placeId].push_back(resourceUsageInfo);
    } else {
        std::vector<ResourceUsageInfo> resourceUsageVector;

        resourceUsageVector.push_back(resourceUsageInfo);

        resourceUsageMap[placeId] = resourceUsageVector;
    }

    /*if (isVMStatManager.find("true") != std::string::npos) {
        std::string vmLevelStatistics = statisticCollector.collectVMStatistics(isVMStatManager, isResourceAllocationRequired);
        std::vector<std::string> strArr = Utils::split(vmLevelStatistics, ',');

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

    string slaPerfSql = "insert into graph_place_sla_performance (graph_sla_id, place_id, memory_usage, load_average, elapsed_time) "
                            "values ('" + graphSlaId + "','" + placeId + "', '" + std::to_string(memoryUsage) + "','" +
                            std::to_string(loadAgerage) + "',' " + std::to_string(elapsedTime) + " ')";

    perfDb.runInsert(slaPerfSql);*/
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

std::vector<long> PerformanceUtil::getResourceAvailableTime(std::vector<std::string> graphIdList, std::string command, std::string category,
                                               std::string masterIP, std::vector<JobRequest> &pendingHPJobList) {
    PerformanceUtil performanceUtil;
    performanceUtil.init();
    std::set<std::string> hostSet;
    std::vector<long> jobScheduleVector;

    processStatusMutex.lock();
    set<ProcessInfo>::iterator processInfoIterator;
    std::vector<double> aggregatedLoadPoints;
    std::map<std::string,std::vector<double>> hostLoadAvgMap;
    std::chrono::milliseconds currentTime = duration_cast<milliseconds>(system_clock::now().time_since_epoch());
    long currentTimestamp = currentTime.count();
    long jobAcceptableTime = 0;

    for (processInfoIterator = processData.begin(); processInfoIterator != processData.end(); ++processInfoIterator) {
        ProcessInfo process = *processInfoIterator;
        std::string currentGraphId = process.graphId;

        if (process.priority == Conts::HIGH_PRIORITY_DEFAULT_VALUE) {
            long processScheduledTime = process.startTimestamp;
            long initialSleepTime = process.sleepTime;
            long elapsedTime = currentTimestamp - (processScheduledTime + initialSleepTime);
            long adjustedElapsedTime;
            long statCollectingGap = Conts::LOAD_AVG_COLLECTING_GAP * 1000;
            long requiredAdjustment = 0;
            long requiredNewPoints = 0;

            if (elapsedTime < 0) {
                adjustedElapsedTime = 0;
                long jobIdleTime = (processScheduledTime + initialSleepTime) - currentTimestamp;
                requiredAdjustment = jobIdleTime % statCollectingGap;
                requiredNewPoints = (jobIdleTime / statCollectingGap) + 1;
            } else if (elapsedTime < statCollectingGap) {
                adjustedElapsedTime = 0;
                requiredAdjustment = elapsedTime;
            } else if (elapsedTime % statCollectingGap == 0) {
                adjustedElapsedTime = elapsedTime;
            } else {
                adjustedElapsedTime = elapsedTime - statCollectingGap;
                requiredAdjustment = elapsedTime % statCollectingGap;
            }

            std::string slaLoadQuery = "select graph_place_sla_performance.place_id,graph_place_sla_performance.load_average,"
                                       "graph_place_sla_performance.elapsed_time from graph_place_sla_performance inner join graph_sla"
                                       "  inner join sla_category where graph_place_sla_performance.graph_sla_id=graph_sla.id"
                                       "  and graph_sla.id_sla_category=sla_category.id and graph_sla.graph_id='" + currentGraphId +
                                       "' and sla_category.command='" + command + "' and sla_category.category='" + category +
                                       "' and graph_place_sla_performance.elapsed_time > '" + std::to_string(adjustedElapsedTime) +
                                       "' order by graph_place_sla_performance.place_id,graph_place_sla_performance.elapsed_time;";

            std::vector<vector<pair<string, string>>> loadAvgResults = perfDb.runSelect(slaLoadQuery);

            std::map<std::string,std::vector<double>> hostTmpLoadAvgMap;

            for (std::vector<vector<pair<string, string>>>::iterator i = loadAvgResults.begin(); i != loadAvgResults.end(); ++i) {
                std::vector<pair<string, string>> rowData = *i;

                std::string placeId = rowData.at(0).second;
                std::string loadAvg = rowData.at(1).second;

                std::vector<double> currentHostLdAvgList = hostTmpLoadAvgMap[placeId];

                if (requiredNewPoints > 0 && currentHostLdAvgList.empty()) {
                    int newPointCount = 0;

                    while (newPointCount < requiredNewPoints) {
                        currentHostLdAvgList.push_back(0);
                        newPointCount++;
                    }
                }

                currentHostLdAvgList.push_back(std::atof(loadAvg.c_str()));

                hostTmpLoadAvgMap[placeId] = currentHostLdAvgList;
            }

            for (std::map<std::string,std::vector<double>>::iterator mapIterator = hostTmpLoadAvgMap.begin();
                mapIterator != hostTmpLoadAvgMap.end(); ++mapIterator) {
                std::string placeId = mapIterator->first;
                std::vector<double> hostLoadVector = mapIterator->second;
                std::vector<double> adjustedHostLoads;
                long xAxisValue = 0;

                for (std::size_t index = 0; index != hostLoadVector.size() - 1; ++index) {
                    double currentLoad = hostLoadVector[index];
                    double nextLoad = hostLoadVector[index + 1];

                    double slope = (nextLoad - currentLoad) / statCollectingGap;

                    double intercept = currentLoad - slope * xAxisValue;

                    double adjustedLoad = slope * (xAxisValue + requiredAdjustment) + (intercept);

                    adjustedHostLoads.push_back(adjustedLoad);

                    xAxisValue += statCollectingGap;
                }

                std::vector<double> existingAggregateLoadVector = hostLoadAvgMap[placeId];

                if (existingAggregateLoadVector.size() > 0) {
                    auto maxVectorSize = std::max(existingAggregateLoadVector.size(),adjustedHostLoads.size());
                    std::vector<double> aggregatedVector = std::vector<double>(maxVectorSize);

                    existingAggregateLoadVector.resize(maxVectorSize);
                    adjustedHostLoads.resize(maxVectorSize);

                    std::transform(existingAggregateLoadVector.begin(), existingAggregateLoadVector.end(),
                            adjustedHostLoads.begin(),aggregatedVector.begin(),std::plus<double>());

                    hostLoadAvgMap[placeId] = aggregatedVector;

                } else {
                    hostLoadAvgMap[placeId] = adjustedHostLoads;
                }
            }
        }
    }

    processStatusMutex.unlock();


    std::vector<std::string>::iterator graphIdListIterator;

    for (graphIdListIterator = graphIdList.begin(); graphIdListIterator != graphIdList.end(); ++graphIdListIterator) {
        std::string graphId = *graphIdListIterator;
        scheduler_logger.log("###PERFORMANCE### New Job Graph ID:" + graphId, "info");
        int loopCount = 0;

        std::string newJobLoadQuery = "select graph_place_sla_performance.place_id,graph_place_sla_performance.load_average,"
                                      "graph_place_sla_performance.elapsed_time from graph_place_sla_performance inner join graph_sla"
                                      "  inner join sla_category where graph_place_sla_performance.graph_sla_id=graph_sla.id"
                                      "  and graph_sla.id_sla_category=sla_category.id and graph_sla.graph_id='" + graphId +
                                      "' and sla_category.command='" + command + "' and sla_category.category='" + category +
                                      "' order by graph_place_sla_performance.place_id,graph_place_sla_performance.elapsed_time;";

        std::vector<vector<pair<string, string>>> newJobLoadAvgResults = perfDb.runSelect(newJobLoadQuery);

        std::map<std::string,std::vector<double>> newJobLoadAvgMap;

        for (std::vector<vector<pair<string, string>>>::iterator i = newJobLoadAvgResults.begin(); i != newJobLoadAvgResults.end(); ++i) {
            std::vector<pair<string, string>> rowData = *i;

            std::string placeId = rowData.at(0).second;
            std::string loadAvg = rowData.at(1).second;

            newJobLoadAvgMap[placeId].push_back(std::atof(loadAvg.c_str()));
        }

        for (std::map<std::string,std::vector<double>>::iterator mapIterator = hostLoadAvgMap.begin();
             mapIterator != hostLoadAvgMap.end(); ++mapIterator) {

            std::string placeId = mapIterator->first;
            std::vector<double> hostLoadVector = mapIterator->second;
            hostLoadVector.push_back(0);

            std::vector<double> newJobHostLoadVector = newJobLoadAvgMap[placeId];

            if (newJobHostLoadVector.size() > 0) {
                double newJobHostMaxLoad = *std::max_element(newJobHostLoadVector.begin(),newJobHostLoadVector.end());
                double currentAggHostMaxLoad = *std::max_element(hostLoadVector.begin(),hostLoadVector.end());

                scheduler_logger.log("###PERFORMANCE### New Job Max Load:" + std::to_string(newJobHostMaxLoad), "info");
                scheduler_logger.log("###PERFORMANCE### Current Aggregated max load:" + std::to_string(currentAggHostMaxLoad), "info");

                if (Conts::LOAD_AVG_THREASHOLD > currentAggHostMaxLoad + newJobHostMaxLoad) {
                    jobAcceptableTime = 0;
                } else {
                    int maxValueIndex = std::max_element(hostLoadVector.begin(),hostLoadVector.end()) - hostLoadVector.begin();
                    bool flag = true;

                    for (int aggIndex = maxValueIndex; aggIndex != hostLoadVector.size(); ++aggIndex) {
                        for (int currentJobIndex = 0; currentJobIndex != newJobHostLoadVector.size(); ++currentJobIndex) {
                            if (aggIndex + currentJobIndex >= hostLoadVector.size()) {
                                break;
                            }

                            if (hostLoadVector[aggIndex + currentJobIndex] + newJobHostLoadVector[currentJobIndex] > Conts::LOAD_AVG_THREASHOLD) {
                                flag = false;
                                break;
                            }
                        }

                        if (flag && aggIndex * Conts::LOAD_AVG_COLLECTING_GAP * 1000 > jobAcceptableTime) {
                            jobAcceptableTime = aggIndex * Conts::LOAD_AVG_COLLECTING_GAP * 1000;
                        } else {
                            flag = true;
                        }

                    }
                }

            }

        }

        scheduler_logger.log("###PERFORMANCE### New Job onboard after:" + std::to_string(jobAcceptableTime), "info");

        JobRequest jobRequest = pendingHPJobList[loopCount];
        std::string slaString = jobRequest.getParameter(Conts::PARAM_KEYS::GRAPH_SLA);
        long graphSla = std::atol(slaString.c_str());

        if (jobAcceptableTime > graphSla * 0.1) {
            jobScheduleVector.push_back(-1);
        } else {
            PerformanceUtil::adjustAggregateLoadMap(hostLoadAvgMap,newJobLoadAvgMap,jobAcceptableTime);
            jobScheduleVector.push_back(jobAcceptableTime);
        }



        loopCount++;
    }


    /*std::string newJobLoadQuery = "select graph_place_sla_performance.place_id,graph_place_sla_performance.load_average,"
                              "graph_place_sla_performance.elapsed_time from graph_place_sla_performance inner join graph_sla"
                              "  inner join sla_category where graph_place_sla_performance.graph_sla_id=graph_sla.id"
                              "  and graph_sla.id_sla_category=sla_category.id and graph_sla.graph_id='" + graphId +
                              "' and sla_category.command='" + command + "' and sla_category.category='" + category +
                              "' order by graph_place_sla_performance.place_id,graph_place_sla_performance.elapsed_time;";

    std::vector<vector<pair<string, string>>> newJobLoadAvgResults = perfDb.runSelect(newJobLoadQuery);

    std::map<std::string,std::vector<double>> newJobLoadAvgMap;

    for (std::vector<vector<pair<string, string>>>::iterator i = newJobLoadAvgResults.begin(); i != newJobLoadAvgResults.end(); ++i) {
        std::vector<pair<string, string>> rowData = *i;

        std::string placeId = rowData.at(0).second;
        std::string loadAvg = rowData.at(1).second;

        newJobLoadAvgMap[placeId].push_back(std::atof(loadAvg.c_str()));
    }

    for (std::map<std::string,std::vector<double>>::iterator mapIterator = hostLoadAvgMap.begin();
         mapIterator != hostLoadAvgMap.end(); ++mapIterator) {

        std::string placeId = mapIterator->first;
        std::vector<double> hostLoadVector = mapIterator->second;

        std::vector<double> newJobHostLoadVector = newJobLoadAvgMap[placeId];

        if (newJobHostLoadVector.size() > 0) {
            auto tmpMaxSize = std::max(hostLoadVector.size(),newJobHostLoadVector.size());

            hostLoadVector.resize(tmpMaxSize);
            newJobHostLoadVector.resize(tmpMaxSize);

            auto tmpAggregate = std::vector<double>(tmpMaxSize);

            std::transform(hostLoadVector.begin(), hostLoadVector.end(),
                           newJobHostLoadVector.begin(),tmpAggregate.begin(),std::plus<double>());

            double aggregatedMaxLoadAverage = *std::max_element(tmpAggregate.begin(), tmpAggregate.end());

            if (Conts::LOAD_AVG_THREASHOLD < aggregatedMaxLoadAverage) {
                auto aggregatedMax = *std::max_element(hostLoadVector.begin(),hostLoadVector.end());
                int maxValueIndex = std::max_element(hostLoadVector.begin(),hostLoadVector.end()) - hostLoadVector.begin();
                bool flag = true;

                for (int aggIndex = maxValueIndex; aggIndex != hostLoadVector.size(); ++aggIndex) {
                    for (int currentJobIndex = 0; currentJobIndex != newJobHostLoadVector.size(); ++currentJobIndex) {
                        if (aggIndex + currentJobIndex >= hostLoadVector.size()) {
                            break;
                        }

                        if (hostLoadVector[aggIndex + currentJobIndex] + newJobHostLoadVector[currentJobIndex] > Conts::LOAD_AVG_THREASHOLD) {
                            flag = false;
                            break;
                        }
                    }

                    if (flag && aggIndex * Conts::LOAD_AVG_COLLECTING_GAP * 1000 > jobAcceptableTime) {
                        jobAcceptableTime = aggIndex * Conts::LOAD_AVG_COLLECTING_GAP * 1000;
                    } else {
                        flag = true;
                    }

                }

            }

        }

    }*/

    std::map<std::string,std::vector<double>>::iterator logIterator;

    for (logIterator = hostLoadAvgMap.begin(); logIterator!=hostLoadAvgMap.end();++logIterator) {
        std::string hostId = logIterator->first;
        std::vector<double> loadVector = logIterator->second;

        std::vector<double>::iterator loadIterator;

        for (loadIterator = loadVector.begin();loadIterator!=loadVector.end();++loadIterator) {
            double load = *loadIterator;
            std::cout << "###PERF### Host ID: " + hostId + "EXPECTED LOAD: " + std::to_string(load) << std::endl;
        }
    }


    return jobScheduleVector;
}

void PerformanceUtil::adjustAggregateLoadMap(std::map<std::string, std::vector<double>> &aggregateLoadAvgMap,
                                             std::map<std::string, std::vector<double>> &newJobLoadAvgMap,
                                             long newJobAcceptanceTime) {
    std::map<std::string, std::vector<double>>::iterator newJobIterator;

    for (newJobIterator = newJobLoadAvgMap.begin();newJobIterator != newJobLoadAvgMap.end();
                ++newJobIterator) {
        std::string hostId = newJobIterator->first;
        std::vector<double> newJobLoadVector = newJobIterator->second;
        std::vector<double> newJobTmpLoadVector;

        if (newJobAcceptanceTime > 0) {
            for (long adjustCount = 0; adjustCount < newJobAcceptanceTime;
                 adjustCount+=Conts::LOAD_AVG_COLLECTING_GAP * 1000) {
                newJobTmpLoadVector.push_back(0);
                newJobTmpLoadVector.insert(newJobTmpLoadVector.end(), newJobLoadVector.begin(),
                                           newJobLoadVector.end());
            }
        } else {
            newJobTmpLoadVector = newJobLoadVector;
        }

        std::vector<double> aggregateLoadVector = aggregateLoadAvgMap[hostId];

        if (aggregateLoadVector.size() > 0) {
            auto maxVectorSize = std::max(newJobTmpLoadVector.size(),aggregateLoadVector.size());
            std::vector<double> aggregatedVector = std::vector<double>(maxVectorSize);

            newJobTmpLoadVector.resize(maxVectorSize);
            aggregateLoadVector.resize(maxVectorSize);

            std::transform(aggregateLoadVector.begin(), aggregateLoadVector.end(),
                           newJobTmpLoadVector.begin(),aggregatedVector.begin(),std::plus<double>());

            aggregateLoadAvgMap[hostId] = aggregatedVector;

        } else {
            aggregateLoadAvgMap[hostId] = newJobTmpLoadVector;
        }

    }
}

void PerformanceUtil::logLoadAverage() {
    StatisticCollector statisticCollector;

    double currentLoadAverage = statisticCollector.getLoadAverage();

    std::cout << "###PERF### CURRENT LOAD: " + std::to_string(currentLoadAverage) << std::endl;
}

void PerformanceUtil::updateResourceConsumption(PerformanceSQLiteDBInterface performanceDb, std::string graphId, int partitionCount, std::vector<Place> placeList,
                                                std::string slaCategoryId) {
    std::vector<Place>::iterator placeListIterator;

    for (placeListIterator = placeList.begin(); placeListIterator != placeList.end(); ++placeListIterator) {
        Place currentPlace = *placeListIterator;
        std::string isHostReporter = currentPlace.isHostReporter;
        std::string hostId = currentPlace.hostId;
        std::string placeId = currentPlace.placeId;
        std::string graphSlaId;

        std::string query = "SELECT id from graph_sla where graph_id='" + graphId +
                            "' and partition_count='" + std::to_string(partitionCount) + "' and id_sla_category='" + slaCategoryId + "';";

        std::vector<vector<pair<string, string>>> results = performanceDb.runSelect(query);

        if (results.size() == 1) {
            graphSlaId = results[0][0].second;
        } else {
            std::string insertQuery = "insert into graph_sla (id_sla_category, graph_id, partition_count, sla_value, attempt) VALUES ('" +
                                      slaCategoryId + "','" + graphId + "'," + std::to_string(partitionCount) + ",0,0);";

            int slaId = performanceDb.runInsert(insertQuery);
            graphSlaId = std::to_string(slaId);
        }

        std::vector<ResourceUsageInfo> resourceUsageVector = resourceUsageMap[placeId];
        string valuesString;
        std::vector<ResourceUsageInfo>::iterator usageIterator;

        if (resourceUsageVector.size() > 0) {
            string slaPerfSql = "insert into graph_place_sla_performance (graph_sla_id, place_id, memory_usage, load_average, elapsed_time) "
                                "values ";

            for (usageIterator = resourceUsageVector.begin(); usageIterator != resourceUsageVector.end(); ++usageIterator) {
                ResourceUsageInfo usageInfo = *usageIterator;

                valuesString += "('" + graphSlaId + "','" + placeId + "', '','" +
                                usageInfo.loadAverage + "','" + usageInfo.elapsedTime + "'),";
            }


            valuesString = valuesString.substr(0, valuesString.length() -1);
            slaPerfSql = slaPerfSql + valuesString;

            performanceDb.runInsert(slaPerfSql);
        }
    }
}

void PerformanceUtil::updateRemoteResourceConsumption(PerformanceSQLiteDBInterface performanceDb, std::string graphId, int partitionCount,
                                                      std::vector<Place> placeList, std::string slaCategoryId, std::string masterIP) {
    std::vector<Place>::iterator placeListIterator;

    for (placeListIterator = placeList.begin(); placeListIterator != placeList.end(); ++placeListIterator) {
        Place place = *placeListIterator;
        std::string host;

        std::string ip = place.ip;
        std::string user = place.user;
        std::string serverPort = place.serverPort;
        std::string isMaster = place.isMaster;
        std::string isHostReporter = place.isHostReporter;
        std::string hostId = place.hostId;
        std::string placeId = place.placeId;
        std::string graphSlaId;

        if (ip.find("localhost") != std::string::npos || ip.compare(masterIP) == 0) {
            host = ip;
        } else {
            host = user + "@" + ip;
        }


        if (isMaster.find("true") != std::string::npos || host == "localhost" || host.compare(masterIP) == 0) {

        } else {
            std::string loadString = requestRemoteLoadAverages(host,atoi(serverPort.c_str()),isHostReporter,"false",placeId,0,masterIP);
            std::vector<std::string> loadVector = Utils::split(loadString, ',');
            std::vector<std::string>::iterator loadVectorIterator;
            string valuesString;
            std::string query = "SELECT id from graph_sla where graph_id='" + graphId +
                                "' and partition_count='" + std::to_string(partitionCount) + "' and id_sla_category='" + slaCategoryId + "';";

            std::vector<vector<pair<string, string>>> results = performanceDb.runSelect(query);

            if (results.size() == 1) {
                graphSlaId = results[0][0].second;
            } else {
                std::string insertQuery = "insert into graph_sla (id_sla_category, graph_id, partition_count, sla_value, attempt) VALUES ('" +
                                          slaCategoryId + "','" + graphId + "'," + std::to_string(partitionCount) + ",0,0);";

                int slaId = performanceDb.runInsert(insertQuery);
                graphSlaId = std::to_string(slaId);
            }

            if (loadVector.size() > 0) {
                int elapsedTime = 0;
                string slaPerfSql = "insert into graph_place_sla_performance (graph_sla_id, place_id, memory_usage, load_average, elapsed_time) "
                                    "values ";

                for (loadVectorIterator = loadVector.begin(); loadVectorIterator != loadVector.end(); ++loadVectorIterator) {
                    std::string loadAverage = *loadVectorIterator;

                    valuesString += "('" + graphSlaId + "','" + placeId + "', '','" +
                            loadAverage + "','" + std::to_string(elapsedTime * 1000) + "'),";
                    elapsedTime+=5;
                }


                valuesString = valuesString.substr(0, valuesString.length() -1);
                slaPerfSql = slaPerfSql + valuesString;

                performanceDb.runInsert(slaPerfSql);
            }
        }
    }

}

std::string PerformanceUtil::getSLACategoryId(std::string command, std::string category) {
    std::string categoryQuery = "SELECT id from sla_category where command='" + command + "' and category='" + category + "'";

    std::vector<vector<pair<string, string>>> categoryResults = perfDb.runSelect(categoryQuery);

    if (categoryResults.size() == 1) {
        std::string slaCategoryId = categoryResults[0][0].second;
        return slaCategoryId;
    } else {
        scheduler_logger.log("Invalid SLA " + category + " for " + command + " command", "error");
        return 0;
    }
}

int PerformanceUtil::initiateCollectingRemoteSLAResourceUtilization(std::string host, int port,
                                                                    std::string isVMStatManager,
                                                                    std::string isResourceAllocationRequired,
                                                                    std::string placeId, int elapsedTime,
                                                                    std::string masterIP) {
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
        write(sockfd, masterIP.c_str(), masterIP.size());
        scheduler_logger.log("Sent : " + masterIP, "info");

        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");
        scheduler_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            write(sockfd, JasmineGraphInstanceProtocol::START_STAT_COLLECTION.c_str(),
                  JasmineGraphInstanceProtocol::START_STAT_COLLECTION.size());
            scheduler_logger.log("Sent : " + JasmineGraphInstanceProtocol::START_STAT_COLLECTION, "info");
            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");
            scheduler_logger.log("Received : " + response, "info");

            if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
                scheduler_logger.log("REQUESTED REMOTE STAT COLLECTION ", "info");
            }
        }

    }
}

std::string PerformanceUtil::requestRemoteLoadAverages(std::string host, int port, std::string isVMStatManager,
                                               std::string isResourceAllocationRequired, std::string placeId,
                                               int elapsedTime, std::string masterIP) {
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
        write(sockfd, masterIP.c_str(), masterIP.size());
        scheduler_logger.log("Sent : " + masterIP, "info");

        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");
        scheduler_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            write(sockfd, JasmineGraphInstanceProtocol::REQUEST_COLLECTED_STATS.c_str(),
                  JasmineGraphInstanceProtocol::REQUEST_COLLECTED_STATS.size());
            scheduler_logger.log("Sent : " + JasmineGraphInstanceProtocol::REQUEST_COLLECTED_STATS, "info");
            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");
            string status = response.substr(response.size() - 5);
            std::string result = response.substr(0, response.size() - 5);

            while (status == "/SEND") {
                write(sockfd, status.c_str(), status.size());

                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);
                response = utils.trim_copy(response, " \f\n\r\t\v");
                status = response.substr(response.size() - 5);
                std::string loadAverageString= response.substr(0, response.size() - 5);
                result = result + loadAverageString;
            }
            response = result;
        }

    }

    return response;
}