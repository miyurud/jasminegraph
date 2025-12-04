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

#ifndef JASMINEGRAPH_STATISTICSCOLLECTOR_H
#define JASMINEGRAPH_STATISTICSCOLLECTOR_H

#include <dirent.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <algorithm>
#include <climits>
#include <cmath>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <sstream>
#include <thread>
#include <vector>
#include <map>

#include "../../util/Utils.h"
#include "PerformanceUtil.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "sys/times.h"

class StatisticsCollector {
 private:
    static const int BUFFER_SIZE = 128;

 public:
    static int init();
    static long getMemoryUsageByProcess();
    static int getThreadCount();
    static long getUsedSwapSpace();
    static long getTotalSwapSpace();
    static long getRXBytes();
    static long getTXBytes();
    static int getSocketCount();
    static double getCpuUsage();
    static long getTotalMemoryAllocated();
    static int getTotalNumberofCores();
    static double getCpuLoadPercentage();
    static long getTotalMemoryUsage();
    static double getTotalCpuUsage();
    static double getLoadAverage();
    static long getRunQueue();
    static std::vector<double> getLogicalCpuCoreThreadUsage();
    static double getProcessSwitchesPerSecond();
    static double getForkCallsPerSecond();
    static std::map<std::string, std::pair<double, double>, std::less<>> getNetworkPacketsPerSecond();
    static std::map<std::string, double, std::less<>> getDiskBusyPercentage();
    static std::map<std::string, std::pair<double, double>, std::less<>> getDiskReadWriteKBPerSecond();
    static std::map<std::string, double, std::less<>> getDiskBlockSizeKB();
    static std::map<std::string, double, std::less<>> getDiskTransfersPerSecond();
    static void logLoadAverage(std::string name);
    static double getMemoryUsagePercentage();
};

#endif  // JASMINEGRAPH_STATISTICSCOLLECTOR_H
