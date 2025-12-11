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

#include "../../server/JasmineGraphServer.h"

using namespace std::chrono;
std::map<std::string, std::vector<ResourceUsageInfo>> resourceUsageMap;

static size_t write_callback(void *contents, size_t size, size_t nmemb, std::string *output);
static size_t write_file_callback(void* contents, size_t size, size_t nmemb, void* userp);

Logger scheduler_logger;
SQLiteDBInterface *sqlLiteDB;
PerformanceSQLiteDBInterface *perfDb;

void PerformanceUtil::init() {
    if (sqlLiteDB == nullptr) {
        sqlLiteDB = new SQLiteDBInterface();
        sqlLiteDB->init();
    }

    if (perfDb == nullptr) {
        perfDb = new PerformanceSQLiteDBInterface();
        perfDb->init();
    }
}

int PerformanceUtil::collectPerformanceStatistics() {
    // Host level
    double cpuUsage = StatisticsCollector::getCpuUsage();
    Utils::send_job("", "cpu_usage", std::to_string(cpuUsage));

    long rxBytes = StatisticsCollector::getRXBytes();
    Utils::send_job("", "rx_bytes", std::to_string(rxBytes));

    long txBytes = StatisticsCollector::getTXBytes();
    Utils::send_job("", "tx_bytes", std::to_string(txBytes));

    long totalMemoryUsage = StatisticsCollector::getTotalMemoryUsage();
    Utils::send_job("", "total_memory", std::to_string(totalMemoryUsage));

    double totalMemoryUsagePercentage = StatisticsCollector::getMemoryUsagePercentage();
    Utils::send_job("", "memory_usage_percentage", std::to_string(totalMemoryUsagePercentage));


    long usedSwapSpace = StatisticsCollector::getUsedSwapSpace();
    Utils::send_job("", "used_swap_space", std::to_string(usedSwapSpace));

    double currentLoadAverage = StatisticsCollector::getLoadAverage();
    Utils::send_job("", "load_average", std::to_string(currentLoadAverage));

    double cpuLoadPercentage = StatisticsCollector::getCpuLoadPercentage();
    Utils::send_job("", "cpu_load_percentage", std::to_string(cpuLoadPercentage));

    long runQueue = StatisticsCollector::getRunQueue();
    Utils::send_job("", "run_queue", std::to_string(runQueue));

    std::vector<double> logicalCpuUsages = StatisticsCollector::getLogicalCpuCoreThreadUsage();
    for (size_t i = 0; i < logicalCpuUsages.size(); i++) {
        Utils::send_job("", "cpu_core_" + std::to_string(i) + "_usage", std::to_string(logicalCpuUsages[i]));
    }

    double processSwitchesPerSec = StatisticsCollector::getProcessSwitchesPerSecond();
    Utils::send_job("", "process_switches_per_sec", std::to_string(processSwitchesPerSec));

    double forkCallsPerSec = StatisticsCollector::getForkCallsPerSecond();
    Utils::send_job("", "fork_calls_per_sec", std::to_string(forkCallsPerSec));

    std::unordered_map<std::string, std::pair<double, double>> networkPackets =
                StatisticsCollector::getNetworkPacketsPerSecond();
    for (const auto& [interface, packets] : networkPackets) {
        double rxPacketsPerSec = packets.first;
        double txPacketsPerSec = packets.second;
        Utils::send_job("", "net_" + interface + "_rx_packets_per_sec", std::to_string(rxPacketsPerSec));
        Utils::send_job("", "net_" + interface + "_tx_packets_per_sec", std::to_string(txPacketsPerSec));
    }

    std::unordered_map<std::string, double> diskBusy = StatisticsCollector::getDiskBusyPercentage();
    for (const auto& [device, busyPercentage] : diskBusy) {
        Utils::send_job("", "disk_" + device + "_busy_percentage", std::to_string(busyPercentage));
    }

    std::unordered_map<std::string, std::pair<double, double>> diskRates =
                StatisticsCollector::getDiskReadWriteKBPerSecond();
    for (const auto& [device, rates] : diskRates) {
        double readKBPerSec = rates.first;
        double writeKBPerSec = rates.second;
        Utils::send_job("", "disk_" + device + "_read_kb_per_sec", std::to_string(readKBPerSec));
        Utils::send_job("", "disk_" + device + "_write_kb_per_sec", std::to_string(writeKBPerSec));
    }

    std::unordered_map<std::string, double> diskBlockSizes = StatisticsCollector::getDiskBlockSizeKB();
    for (const auto& [device, blockSizeKB] : diskBlockSizes) {
        Utils::send_job("", "disk_" + device + "_block_size_kb", std::to_string(blockSizeKB));
    }

    std::unordered_map<std::string, double> diskTransferRates = StatisticsCollector::getDiskTransfersPerSecond();
    for (const auto& [device, transfersPerSec] : diskTransferRates) {
        Utils::send_job("", "disk_" + device + "_transfers_per_sec", std::to_string(transfersPerSec));
    }

    long totalSwapSpace = StatisticsCollector::getTotalSwapSpace();
    Utils::send_job("", "total_swap_space", std::to_string(totalSwapSpace));

    // Per process
    long memoryUsage = StatisticsCollector::getMemoryUsageByProcess();
    Utils::send_job("", "memory_usage", std::to_string(memoryUsage));

    int threadCount = StatisticsCollector::getThreadCount();
    Utils::send_job("", "thread_count", std::to_string(threadCount));

    int socketCount = StatisticsCollector::getSocketCount();
    Utils::send_job("", "socket_count", std::to_string(socketCount));
    scheduler_logger.info("Pushed performance metrics");
    return 0;
}




std::vector<Place> PerformanceUtil::getHostReporterList() {
    std::vector<Place> hostReporterList;

    std::string placeLoadQuery =
        "select ip, user, server_port, is_master, is_host_reporter,host_idhost,idplace from place"
        " where is_host_reporter='true'";
    std::vector<vector<pair<string, string>>> placeList = perfDb->runSelect(placeLoadQuery);
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
                                                   std::string command, std::string category, std::string masterIP,
                                                   int elapsedTime, bool autoCalibrate) {
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

        if (isMaster.find("true") != std::string::npos || host == "localhost" || host.compare(masterIP) == 0) {
            collectLocalSLAResourceUtilization(graphId, placeId, command, category, elapsedTime, autoCalibrate);
        }
    }

    return 0;
}

void PerformanceUtil::collectLocalSLAResourceUtilization(std::string graphId, std::string placeId, std::string command,
                                                         std::string category, int elapsedTime, bool autoCalibrate) {
    string graphSlaId;

    double loadAverage = StatisticsCollector::getLoadAverage();

    auto executedTime = std::chrono::system_clock::now();
    std::time_t reportTime = std::chrono::system_clock::to_time_t(executedTime);
    std::string reportTimeString(std::ctime(&reportTime));
    reportTimeString = Utils::trim_copy(reportTimeString);

    ResourceUsageInfo resourceUsageInfo;
    resourceUsageInfo.elapsedTime = std::to_string(elapsedTime);

    if (!autoCalibrate) {
        resourceUsageInfo.loadAverage = std::to_string(loadAverage);
    } else {
        double aggregatedLoadAverage = getAggregatedLoadAverage(graphId, placeId, command, category, elapsedTime);
        resourceUsageInfo.loadAverage = std::to_string(loadAverage - aggregatedLoadAverage);
    }

    if (!resourceUsageMap[placeId].empty()) {
        resourceUsageMap[placeId].push_back(resourceUsageInfo);
    } else {
        std::vector<ResourceUsageInfo> resourceUsageVector;
        resourceUsageVector.push_back(resourceUsageInfo);
        resourceUsageMap[placeId] = resourceUsageVector;
    }
}

std::vector<long> PerformanceUtil::getResourceAvailableTime(std::vector<std::string> graphIdList, std::string command,
                                                            std::string category, std::string masterIP,
                                                            std::vector<JobRequest> &pendingHPJobList) {
    PerformanceUtil performanceUtil;
    performanceUtil.init();
    std::set<std::string> hostSet;
    std::vector<long> jobScheduleVector;

    processStatusMutex.lock();
    set<ProcessInfo>::iterator processInfoIterator;
    std::vector<double> aggregatedLoadPoints;
    std::map<std::string, std::vector<double>> hostLoadAvgMap;
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

            std::string slaLoadQuery =
                "select graph_place_sla_performance.place_id,graph_place_sla_performance.load_average,"
                "graph_place_sla_performance.elapsed_time from graph_place_sla_performance inner join graph_sla"
                "  inner join sla_category where graph_place_sla_performance.graph_sla_id=graph_sla.id"
                "  and graph_sla.id_sla_category=sla_category.id and graph_sla.graph_id='" +
                currentGraphId + "' and sla_category.command='" + command + "' and sla_category.category='" + category +
                "' and graph_place_sla_performance.elapsed_time > '" + std::to_string(adjustedElapsedTime) +
                "' order by graph_place_sla_performance.place_id,graph_place_sla_performance.elapsed_time;";

            std::vector<vector<pair<string, string>>> loadAvgResults = perfDb->runSelect(slaLoadQuery);

            std::map<std::string, std::vector<double>> hostTmpLoadAvgMap;

            for (std::vector<vector<pair<string, string>>>::iterator i = loadAvgResults.begin();
                 i != loadAvgResults.end(); ++i) {
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

            for (std::map<std::string, std::vector<double>>::iterator mapIterator = hostTmpLoadAvgMap.begin();
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
                    auto maxVectorSize = std::max(existingAggregateLoadVector.size(), adjustedHostLoads.size());
                    std::vector<double> aggregatedVector = std::vector<double>(maxVectorSize);

                    existingAggregateLoadVector.resize(maxVectorSize);
                    adjustedHostLoads.resize(maxVectorSize);

                    std::transform(existingAggregateLoadVector.begin(), existingAggregateLoadVector.end(),
                                   adjustedHostLoads.begin(), aggregatedVector.begin(), std::plus<double>());

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

        std::string newJobLoadQuery =
            "select graph_place_sla_performance.place_id,graph_place_sla_performance.load_average,"
            "graph_place_sla_performance.elapsed_time from graph_place_sla_performance inner join graph_sla"
            "  inner join sla_category where graph_place_sla_performance.graph_sla_id=graph_sla.id"
            "  and graph_sla.id_sla_category=sla_category.id and graph_sla.graph_id='" +
            graphId + "' and sla_category.command='" + command + "' and sla_category.category='" + category +
            "' order by graph_place_sla_performance.place_id,graph_place_sla_performance.elapsed_time;";

        std::vector<vector<pair<string, string>>> newJobLoadAvgResults = perfDb->runSelect(newJobLoadQuery);

        std::map<std::string, std::vector<double>> newJobLoadAvgMap;

        for (std::vector<vector<pair<string, string>>>::iterator i = newJobLoadAvgResults.begin();
             i != newJobLoadAvgResults.end(); ++i) {
            std::vector<pair<string, string>> rowData = *i;

            std::string placeId = rowData.at(0).second;
            std::string loadAvg = rowData.at(1).second;

            newJobLoadAvgMap[placeId].push_back(std::atof(loadAvg.c_str()));
        }

        for (std::map<std::string, std::vector<double>>::iterator mapIterator = hostLoadAvgMap.begin();
             mapIterator != hostLoadAvgMap.end(); ++mapIterator) {
            std::string placeId = mapIterator->first;
            std::vector<double> hostLoadVector = mapIterator->second;
            hostLoadVector.push_back(0);

            std::vector<double> newJobHostLoadVector = newJobLoadAvgMap[placeId];

            if (newJobHostLoadVector.size() > 0) {
                double newJobHostMaxLoad = *std::max_element(newJobHostLoadVector.begin(), newJobHostLoadVector.end());
                double currentAggHostMaxLoad = *std::max_element(hostLoadVector.begin(), hostLoadVector.end());

                scheduler_logger.log("###PERFORMANCE### New Job Max Load:" + std::to_string(newJobHostMaxLoad), "info");
                scheduler_logger.log(
                    "###PERFORMANCE### Current Aggregated max load:" + std::to_string(currentAggHostMaxLoad), "info");

                if (Conts::LOAD_AVG_THREASHOLD > currentAggHostMaxLoad + newJobHostMaxLoad) {
                    jobAcceptableTime = 0;
                } else {
                    int maxValueIndex =
                        std::max_element(hostLoadVector.begin(), hostLoadVector.end()) - hostLoadVector.begin();
                    bool flag = true;

                    for (int aggIndex = maxValueIndex; aggIndex != hostLoadVector.size(); ++aggIndex) {
                        for (int currentJobIndex = 0; currentJobIndex != newJobHostLoadVector.size();
                             ++currentJobIndex) {
                            if (aggIndex + currentJobIndex >= hostLoadVector.size()) {
                                break;
                            }

                            if (hostLoadVector[aggIndex + currentJobIndex] + newJobHostLoadVector[currentJobIndex] >
                                Conts::LOAD_AVG_THREASHOLD) {
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
            PerformanceUtil::adjustAggregateLoadMap(hostLoadAvgMap, newJobLoadAvgMap, jobAcceptableTime);
            jobScheduleVector.push_back(jobAcceptableTime);
        }

        loopCount++;
    }

    std::map<std::string, std::vector<double>>::iterator logIterator;

    for (logIterator = hostLoadAvgMap.begin(); logIterator != hostLoadAvgMap.end(); ++logIterator) {
        std::string hostId = logIterator->first;
        std::vector<double> loadVector = logIterator->second;

        std::vector<double>::iterator loadIterator;

        for (loadIterator = loadVector.begin(); loadIterator != loadVector.end(); ++loadIterator) {
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

    for (newJobIterator = newJobLoadAvgMap.begin(); newJobIterator != newJobLoadAvgMap.end(); ++newJobIterator) {
        std::string hostId = newJobIterator->first;
        std::vector<double> newJobLoadVector = newJobIterator->second;
        std::vector<double> newJobTmpLoadVector;

        if (newJobAcceptanceTime > 0) {
            for (long adjustCount = 0; adjustCount < newJobAcceptanceTime;
                 adjustCount += Conts::LOAD_AVG_COLLECTING_GAP * 1000) {
                newJobTmpLoadVector.push_back(0);
                newJobTmpLoadVector.insert(newJobTmpLoadVector.end(), newJobLoadVector.begin(), newJobLoadVector.end());
            }
        } else {
            newJobTmpLoadVector = newJobLoadVector;
        }

        std::vector<double> aggregateLoadVector = aggregateLoadAvgMap[hostId];

        if (aggregateLoadVector.size() > 0) {
            auto maxVectorSize = std::max(newJobTmpLoadVector.size(), aggregateLoadVector.size());
            std::vector<double> aggregatedVector = std::vector<double>(maxVectorSize);

            newJobTmpLoadVector.resize(maxVectorSize);
            aggregateLoadVector.resize(maxVectorSize);

            std::transform(aggregateLoadVector.begin(), aggregateLoadVector.end(), newJobTmpLoadVector.begin(),
                           aggregatedVector.begin(), std::plus<double>());

            aggregateLoadAvgMap[hostId] = aggregatedVector;

        } else {
            aggregateLoadAvgMap[hostId] = newJobTmpLoadVector;
        }
    }
}

void PerformanceUtil::logLoadAverage() {
    double currentLoadAverage = StatisticsCollector::getLoadAverage();
}

void PerformanceUtil::updateResourceConsumption(PerformanceSQLiteDBInterface *performanceDb, std::string graphId,
                                                int partitionCount, std::vector<Place> placeList,
                                                std::string slaCategoryId) {
    std::vector<Place>::iterator placeListIterator;

    for (placeListIterator = placeList.begin(); placeListIterator != placeList.end(); ++placeListIterator) {
        Place currentPlace = *placeListIterator;
        std::string isHostReporter = currentPlace.isHostReporter;
        std::string hostId = currentPlace.hostId;
        std::string placeId = currentPlace.placeId;
        std::string graphSlaId;

        std::string query = "SELECT id from graph_sla where graph_id='" + graphId + "' and partition_count='" +
                            std::to_string(partitionCount) + "' and id_sla_category='" + slaCategoryId + "';";

        std::vector<vector<pair<string, string>>> results = performanceDb->runSelect(query);

        if (results.size() == 1) {
            graphSlaId = results[0][0].second;
        } else {
            std::string insertQuery =
                "insert into graph_sla (id_sla_category, graph_id, partition_count, sla_value, attempt) VALUES ('" +
                slaCategoryId + "','" + graphId + "'," + std::to_string(partitionCount) + ",0,0);";

            int slaId = performanceDb->runInsert(insertQuery);
            graphSlaId = std::to_string(slaId);
        }

        std::vector<ResourceUsageInfo> resourceUsageVector = resourceUsageMap[placeId];
        string valuesString;
        std::vector<ResourceUsageInfo>::iterator usageIterator;

        if (resourceUsageVector.size() > 0) {
            string slaPerfSql =
                "insert into graph_place_sla_performance (graph_sla_id, place_id, memory_usage, load_average, "
                "elapsed_time) "
                "values ";

            for (usageIterator = resourceUsageVector.begin(); usageIterator != resourceUsageVector.end();
                 ++usageIterator) {
                ResourceUsageInfo usageInfo = *usageIterator;

                valuesString += "('" + graphSlaId + "','" + placeId + "', '','" + usageInfo.loadAverage + "','" +
                                usageInfo.elapsedTime + "'),";

                Utils::send_job("loadAverageSLA", "load_average", usageInfo.loadAverage);
            }
            valuesString = valuesString.substr(0, valuesString.length() - 1);
            slaPerfSql = slaPerfSql + valuesString;

            performanceDb->runInsert(slaPerfSql);
        }
    }
}

void PerformanceUtil::updateRemoteResourceConsumption(PerformanceSQLiteDBInterface *performanceDb, std::string graphId,
                                                      int partitionCount, std::vector<Place> placeList,
                                                      std::string slaCategoryId, std::string masterIP) {
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
            std::string loadString = requestRemoteLoadAverages(host, atoi(serverPort.c_str()), isHostReporter, "false",
                                                               placeId, 0, masterIP);
            std::vector<std::string> loadVector = Utils::split(loadString, ',');
            string valuesString;
            std::string query = "SELECT id from graph_sla where graph_id='" + graphId + "' and partition_count='" +
                                std::to_string(partitionCount) + "' and id_sla_category='" + slaCategoryId + "';";

            std::vector<vector<pair<string, string>>> results = performanceDb->runSelect(query);

            if (results.size() == 1) {
                graphSlaId = results[0][0].second;
            } else {
                std::string insertQuery =
                    "insert into graph_sla (id_sla_category, graph_id, partition_count, sla_value, attempt) VALUES ('" +
                    slaCategoryId + "','" + graphId + "'," + std::to_string(partitionCount) + ",0,0);";

                int slaId = performanceDb->runInsert(insertQuery);
                graphSlaId = std::to_string(slaId);
            }

            if (loadVector.size() > 0) {
                int elapsedTime = 0;
                string slaPerfSql =
                    "insert into graph_place_sla_performance (graph_sla_id, place_id, memory_usage, load_average, "
                    "elapsed_time) "
                    "values ";

                for (auto loadVectorIterator = loadVector.begin(); loadVectorIterator != loadVector.end();
                     ++loadVectorIterator) {
                    std::string loadAverage = *loadVectorIterator;

                    valuesString += "('" + graphSlaId + "','" + placeId + "', '','" + loadAverage + "','" +
                                    std::to_string(elapsedTime * 1000) + "'),";
                    elapsedTime += 5;

                    Utils::send_job("loadAverageSLARem", "load_average_rem", loadAverage);
                }

                valuesString = valuesString.substr(0, valuesString.length() - 1);
                slaPerfSql = slaPerfSql + valuesString;

                performanceDb->runInsert(slaPerfSql);
            }
        }
    }
}

std::string PerformanceUtil::getSLACategoryId(std::string command, std::string category) {
    std::string categoryQuery =
        "SELECT id from sla_category where command='" + command + "' and category='" + category + "'";

    std::vector<vector<pair<string, string>>> categoryResults = perfDb->runSelect(categoryQuery);

    if (categoryResults.size() == 1) {
        std::string slaCategoryId = categoryResults[0][0].second;
        return slaCategoryId;
    }
    scheduler_logger.log("Invalid SLA " + category + " for " + command + " command", "error");
    return 0;
}

void PerformanceUtil::initiateCollectingRemoteSLAResourceUtilization(std::string host, int port,
                                                                     std::string isVMStatManager,
                                                                     std::string isResourceAllocationRequired,
                                                                     std::string placeId, int elapsedTime,
                                                                     std::string masterIP) {
    int sockfd;
    char data[301];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;
    std::string graphSlaId;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot create socket" << std::endl;
        return;
    }

    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        scheduler_logger.error("ERROR, no host named " + host);
        return;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
        return;
    }

    bzero(data, 301);
    write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());
    scheduler_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = Utils::trim_copy(response);

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        scheduler_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        write(sockfd, masterIP.c_str(), masterIP.size());
        scheduler_logger.log("Sent : " + masterIP, "info");

        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = Utils::trim_copy(response);
        scheduler_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            write(sockfd, JasmineGraphInstanceProtocol::START_STAT_COLLECTION.c_str(),
                  JasmineGraphInstanceProtocol::START_STAT_COLLECTION.size());
            scheduler_logger.log("Sent : " + JasmineGraphInstanceProtocol::START_STAT_COLLECTION, "info");
            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = Utils::trim_copy(response);
            scheduler_logger.log("Received : " + response, "info");

            if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
                scheduler_logger.log("REQUESTED REMOTE STAT COLLECTION ", "info");
            }
        }
    }
    Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
    close(sockfd);
}

std::string PerformanceUtil::requestRemoteLoadAverages(std::string host, int port, std::string isVMStatManager,
                                                       std::string isResourceAllocationRequired, std::string placeId,
                                                       int elapsedTime, std::string masterIP) {
    // master should not request load averages directly from workers.
    // Instead it should get from Prometheus.
    return "";
}

double PerformanceUtil::getAggregatedLoadAverage(std::string graphId, std::string placeId, std::string command,
                                                 std::string category, int elapsedTime) {
    PerformanceUtil performanceUtil;
    performanceUtil.init();

    set<ProcessInfo>::iterator processInfoIterator;
    std::chrono::milliseconds currentTime = duration_cast<milliseconds>(system_clock::now().time_since_epoch());
    long currentTimestamp = currentTime.count();
    double aggregatedLoadAverage = 0;

    for (processInfoIterator = processData.begin(); processInfoIterator != processData.end(); ++processInfoIterator) {
        ProcessInfo process = *processInfoIterator;
        std::string currentGraphId = process.graphId;

        double previousLoad, nextLoad, currentLoad;
        long xAxisValue;

        if (process.priority == Conts::HIGH_PRIORITY_DEFAULT_VALUE && graphId != currentGraphId) {
            long processScheduledTime = process.startTimestamp;
            long initialSleepTime = process.sleepTime;
            long elapsedTime = currentTimestamp - (processScheduledTime + initialSleepTime);
            long adjustedElapsedTime;
            long statCollectingGap = Conts::LOAD_AVG_COLLECTING_GAP * 1000;
            long requiredAdjustment = 0;

            if (elapsedTime < 0) {
                continue;
            } else if (elapsedTime % statCollectingGap == 0) {
                adjustedElapsedTime = elapsedTime;

                std::string slaLoadQuery =
                    "select graph_place_sla_performance.place_id,graph_place_sla_performance.load_average,"
                    "graph_place_sla_performance.elapsed_time from graph_place_sla_performance inner join graph_sla"
                    "  inner join sla_category where graph_place_sla_performance.graph_sla_id=graph_sla.id"
                    "  and graph_sla.id_sla_category=sla_category.id and graph_sla.graph_id='" +
                    currentGraphId + "' and sla_category.command='" + command +
                    "' and graph_place_sla_performance.place_id='" + placeId + "' and sla_category.category='" +
                    category + "' and graph_place_sla_performance.elapsed_time ='" +
                    std::to_string(adjustedElapsedTime) +
                    "' order by graph_place_sla_performance.place_id,graph_place_sla_performance.elapsed_time;";

                std::vector<vector<pair<string, string>>> loadAvgResults = perfDb->runSelect(slaLoadQuery);
                if (loadAvgResults.empty()) {
                    continue;
                }

                currentLoad = std::atof(loadAvgResults[0][1].second.c_str());
                aggregatedLoadAverage += currentLoad;
                continue;
            } else {
                adjustedElapsedTime = elapsedTime - statCollectingGap;
                requiredAdjustment = elapsedTime % statCollectingGap;

                std::string slaLoadQuery =
                    "select graph_place_sla_performance.place_id,graph_place_sla_performance.load_average,"
                    "graph_place_sla_performance.elapsed_time from graph_place_sla_performance inner join graph_sla"
                    "  inner join sla_category where graph_place_sla_performance.graph_sla_id=graph_sla.id"
                    "  and graph_sla.id_sla_category=sla_category.id and graph_sla.graph_id='" +
                    currentGraphId + "' and sla_category.command='" + command +
                    "' and graph_place_sla_performance.place_id='" + placeId + "' and sla_category.category='" +
                    category + "' and graph_place_sla_performance.elapsed_time > '" +
                    std::to_string(adjustedElapsedTime) +
                    "' order by graph_place_sla_performance.place_id,graph_place_sla_performance.elapsed_time" +
                    " LIMIT 2;";

                std::vector<vector<pair<string, string>>> loadAvgResults = perfDb->runSelect(slaLoadQuery);

                if (loadAvgResults.size() >= 2) {
                    previousLoad = std::atof(loadAvgResults[0][1].second.c_str());
                    xAxisValue = std::atof(loadAvgResults[0][2].second.c_str());
                    nextLoad = std::atof(loadAvgResults[1][1].second.c_str());
                } else if (loadAvgResults.size() == 1) {
                    // TODO(ASHOK12011234) : Handle the case where there is only one row in the result.
                    continue;
                } else {
                    continue;
                }
            }

            double slope = (nextLoad - previousLoad) / statCollectingGap;           // m= (y2-y1)/(x2-x1)
            double intercept = previousLoad - slope * xAxisValue;                   // c = y1 - mx1
            currentLoad = slope * (xAxisValue + requiredAdjustment) + (intercept);  // y = mx + c
            aggregatedLoadAverage += currentLoad;
        }
    }
    return aggregatedLoadAverage;
}
