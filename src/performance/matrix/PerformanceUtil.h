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

#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <string.h>
#include "../../util/Utils.h"
#include "../../util/Conts.h"
#include "../../metadb/SQLiteDBInterface.h"
#include "../../performancedb/PerformanceSQLiteDBInterface.h"
#include "../../util/PlacesToNodeMapper.h"
#include "../../server/JasmineGraphInstanceProtocol.h"
#include "StatisticCollector.h"
#include "../../util/logger/Logger.h"
#include <thread>
#include <pthread.h>
#include <future>
#include <unistd.h>
#include <sys/socket.h>
#include <netdb.h>
#include "../../util/Utils.h"
#include "../../frontend/core/domain/JobRequest.h"
#include <chrono>


#ifndef JASMINEGRAPH_PERFORMANCEUTIL_H
#define JASMINEGRAPH_PERFORMANCEUTIL_H

struct ResourceConsumption {
    int memoryUsage;
    std::string host;
};

struct Place{
    std::string ip;
    std::string user;
    std::string serverPort;
    std::string isMaster;
    std::string isHostReporter;
    std::string hostId;
    std::string placeId;
};


class PerformanceUtil {
public:
    //PerformanceUtil(SQLiteDBInterface sqlLiteDB, PerformanceSQLiteDBInterface perfDb);
    int init();
    static int collectPerformanceStatistics();
    static int collectSLAResourceConsumption(std::vector<Place> placeList, std::string graphId,
                                             std::string masterIP, int elapsedTime);
    static std::vector<ResourceConsumption> retrieveCurrentResourceUtilization(std::string masterIP);
    static std::vector<long> getResourceAvailableTime(std::vector<std::string> graphIdList, std::string command, std::string category,
                                         std::string masterIP, std::vector<JobRequest> &pendingHPJobList);

    static void logLoadAverage();
    static std::vector<Place> getHostReporterList();
    static void updateResourceConsumption(PerformanceSQLiteDBInterface performanceDb, std::string graphId, int partitionCount, std::vector<Place> placeList,
                                          std::string slaCategoryId);
    static void updateRemoteResourceConsumption(PerformanceSQLiteDBInterface performanceDb, std::string graphId, int partitionCount, std::vector<Place> placeList,
                                          std::string slaCategoryId, std::string masterIP);
    static std::string getSLACategoryId(std::string command, std::string category);
    static int initiateCollectingRemoteSLAResourceUtilization(std::string host, int port, std::string isVMStatManager,
                                                              std::string isResourceAllocationRequired, std::string placeId,
                                                              int elapsedTime, std::string masterIP);
    static std::string requestRemoteLoadAverages(std::string host, int port,
            std::string isVMStatManager,
    std::string isResourceAllocationRequired,
            std::string placeId, int elapsedTime,
            std::string masterIP);

private:
    //static SQLiteDBInterface sqlLiteDB;
    //static PerformanceSQLiteDBInterface perfDb;
    static int collectRemotePerformanceData(std::string host, int port, std::string isVMStatManager, std::string isResourceAllocationRequired, std::string hostId, std::string placeId);
    static int collectLocalPerformanceData(std::string isVMStatManager, std::string isResourceAllocationRequired , std::string hostId, std::string placeId);
    static int collectRemoteSLAResourceUtilization(std::string host, int port, std::string isVMStatManager,
                                                   std::string isResourceAllocationRequired, std::string placeId,
                                                   int elapsedTime, std::string masterIP);
    static int collectLocalSLAResourceUtilization(std::string placeId, int elapsedTime);
    static ResourceConsumption retrieveRemoteResourceConsumption(std::string host, int port,
            std::string hostId, std::string placeId);
    static ResourceConsumption retrieveLocalResourceConsumption(std::string hostId, std::string placeId);
    static void adjustAggregateLoadMap (std::map<std::string,std::vector<double>>& aggregateLoadAvgMap,
            std::map<std::string,std::vector<double>>& newJobLoadAvgMap, long newJobAcceptanceTime);

};


#endif //JASMINEGRAPH_PERFORMANCEUTIL_H
