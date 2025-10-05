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

#include "StatisticCollector.h"
#include <time.h>
#include <unistd.h>

Logger stat_logger;
static int numProcessors;

static long parseLine(char *line);
static long getSwapSpace(const char *type);

#define LINE_BUF_SIZE 128

int StatisticCollector::init() {
    FILE *file;
    struct tms timeSample;
    char line[LINE_BUF_SIZE];

    file = fopen("/proc/cpuinfo", "r");
    if (!file) {
        stat_logger.error("Cannot open /proc/cpuinfo");
        exit(-1);
    }
    numProcessors = 0;
    while (fgets(line, LINE_BUF_SIZE, file) != NULL) {
        if (strncmp(line, "processor", 9) == 0) numProcessors++;
    }
    fclose(file);
    return 0;
}

long StatisticCollector::getMemoryUsageByProcess() {
    FILE *file = fopen("/proc/self/status", "r");
    long result = -1;
    char line[LINE_BUF_SIZE];

    while (fgets(line, LINE_BUF_SIZE, file) != NULL) {
        if (strncmp(line, "VmSize:", 7) == 0) {
            result = parseLine(line);
            break;
        }
    }
    fclose(file);
    return result;
}

int StatisticCollector::getThreadCount() {
    FILE *file = fopen("/proc/self/stat", "r");
    long result;
    char line[LINE_BUF_SIZE];

    for (int i = 0; i < 20; i++) {
        if (fscanf(file, "%127s%*c", line) < 0) {
            fclose(file);
            return -1;
        }
    }
    fclose(file);
    result = strtol(line, NULL, 10);
    if (result <= 0 || result > 0xfffffffffffffffL) return -1;
    return result;
}

static long getSwapSpace(int field) {
    FILE *file = fopen("/proc/swaps", "r");
    long result = -1;
    char line[LINE_BUF_SIZE];

    fgets(line, LINE_BUF_SIZE, file);

    while (fgets(line, LINE_BUF_SIZE, file) != NULL) {
        char *value;
        char *save = NULL;
        for (int i = 0; i < field; i++) {
            if (i == 0) {
                value = strtok_r(line, " ", &save);
            } else {
                value = strtok_r(NULL, "\t", &save);
            }
        }
        long used = strtol(value, NULL, 10);
        if (used < 0 || used > 0xfffffffffffffffL) {
            continue;
        }
        if (result >= 0) {
            result += used;
        } else {
            result = used;
        }
    }
    fclose(file);

    return result;
}

long StatisticCollector::getUsedSwapSpace() {
    long result = getSwapSpace(4);
    return result;
}

long StatisticCollector::getTotalSwapSpace() {
    long result = getSwapSpace(3);
    return result;
}

long StatisticCollector::getRXBytes() {
    FILE *file = fopen("/sys/class/net/eth0/statistics/rx_bytes", "r");
    long result = -1;
    fscanf(file, "%li", &result);
    fclose(file);
    return result;
}

long StatisticCollector::getTXBytes() {
    FILE *file = fopen("/sys/class/net/eth0/statistics/tx_bytes", "r");
    long result = -1;
    fscanf(file, "%li", &result);
    fclose(file);
    return result;
}

int StatisticCollector::getSocketCount() {
    DIR *d = opendir("/proc/self/fd");
    if (!d) {
        puts("Error opening directory /proc/self/fd");
        return -1;
    }
    const struct dirent *dir;
    char path[64];
    char link_buf[1024];
    int count = 0;
    while ((dir = readdir(d)) != NULL) {
        const char *filename = dir->d_name;
        if (filename[0] < '0' || '9' < filename[0]) continue;
        sprintf(path, "/proc/self/fd/%s", filename);
        size_t len = readlink(path, link_buf, sizeof(link_buf) - 1);
        link_buf[len] = 0;
        if (len > 0 && strncmp("socket:", link_buf, 7) == 0) {
            count++;
        }
    }
    (void)closedir(d);
    return count;
}

static long parseLine(char *line) {
    int i = strlen(line);
    const char *p = line;
    while (*p < '0' || *p > '9') p++;
    line[i - 3] = '\0';
    long val = strtol(p, NULL, 10);
    if (val < 0 || val > 0xfffffffffffffffL) return -1;
    return val;
}

static void getCpuCycles(long long *totalp, long long *idlep) {
    *totalp = 0;
    *idlep = 0;
    FILE *fp = fopen("/proc/stat", "r");
    if (!fp) return;
    char line[1024];
    fscanf(fp, "%[^\r\n]%*c", line);
    fclose(fp);

    char *p = line;
    while (*p < '0' || *p > '9') p++;
    long long total = 0;
    long long idle = 0;
    char *end_ptr = p;
    for (int field = 1; field <= 10; field++) {
        while (*p < '0' || *p > '9') {
            if (!(*p)) break;
            p++;
        }
        if (!(*p)) break;
        long long value = strtoll(p, &end_ptr, 10);
        p = end_ptr;
        if (value < 0) {
            stat_logger.error("Value is " + to_string(value) + " for line " + string(line));
        }
        if (field == 4) {
            idle += value;
        }
        total += value;
    }
    *totalp = total;
    *idlep = idle;
}

double StatisticCollector::getCpuUsage() {
    long long total1;
    long long idle1;
    getCpuCycles(&total1, &idle1);
    sleep(5);
    long long total2;
    long long idle2;
    getCpuCycles(&total2, &idle2);

    long long diffTotal = total2 - total1;
    long long diffIdle = idle2 - idle1;

    return (diffTotal - diffIdle) / (double)diffTotal;
}

long StatisticCollector::getTotalMemoryAllocated() {
    std::string token;
    std::ifstream file("/proc/meminfo");
    while (file >> token) {
        if (token == "MemTotal:") {
            unsigned long mem;
            if (file >> mem) {
                return mem;
            }
            return 0;
        }
        file.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
    }
    return 0;
}

int StatisticCollector::getTotalNumberofCores() {
    unsigned concurentThreadsSupported = std::thread::hardware_concurrency();
    return concurentThreadsSupported;
}

long StatisticCollector::getTotalMemoryUsage() {
    std::string token;
    std::ifstream file("/proc/meminfo");
    unsigned long memTotal;
    unsigned long memFree;
    unsigned long buffers;
    unsigned long cached;
    unsigned long sReclaimable;
    unsigned long memUsage;

    while (file >> token) {
        if (token == "MemTotal:") {
            file >> memTotal;
        } else if (token == "MemFree:") {
            file >> memFree;
        } else if (token == "Buffers:") {
            file >> buffers;
        } else if (token == "Cached:") {
            file >> cached;
        } else if (token == "SReclaimable:") {
            file >> sReclaimable;
        }
        file.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
    }
    memUsage = memTotal - (memFree + buffers + cached + sReclaimable);

    return memUsage;
}

double StatisticCollector::getTotalCpuUsage() {
    std::string mpstatCommand = "mpstat";
    char buffer[BUFFER_SIZE];
    std::string result = "";
    std::vector<std::string>::iterator paramNameIterator;
    int count = 0;
    double totalCPUUsage = 0;

    FILE *input = popen(mpstatCommand.c_str(), "r");

    if (input) {
        // read the input
        while (!feof(input)) {
            if (fgets(buffer, BUFFER_SIZE, input) != NULL) {
                result.append(buffer);
            }
        }
        if (!result.empty()) {
            std::vector<std::string> splittedStats = Utils::split(result, '\n');
            int length = splittedStats.size();
            std::string parameterNames = splittedStats[length - 2];
            std::string parameterValues = splittedStats[length - 1];
            std::vector<std::string> splittedParamNames = Utils::split(parameterNames, ' ');
            splittedParamNames.erase(std::remove(splittedParamNames.begin(), splittedParamNames.end(), ""),
                                     splittedParamNames.end());
            std::vector<std::string> splittedParamValues = Utils::split(parameterValues, ' ');
            splittedParamValues.erase(std::remove(splittedParamValues.begin(), splittedParamValues.end(), ""),
                                      splittedParamValues.end());

            for (paramNameIterator = splittedParamNames.begin(); paramNameIterator != splittedParamNames.end();
                 ++paramNameIterator) {
                std::string paramName = *paramNameIterator;

                if (paramName.find("%") != std::string::npos && paramName.find("idle") == std::string::npos) {
                    std::string paramValue = splittedParamValues[count];
                    double paramCPUUsage = std::stof(paramValue.c_str());
                    paramCPUUsage = round(paramCPUUsage * 100) / 100;
                    totalCPUUsage = totalCPUUsage + paramCPUUsage;
                }

                count++;
            }
        }
        pclose(input);
    }

    return totalCPUUsage;
}

double StatisticCollector::getLoadAverage() {
    double loadAvg;
    getloadavg(&loadAvg, 1);
    return loadAvg;
}

long StatisticCollector::getRunQueue() {
    FILE *file = fopen("/proc/stat", "r");
    if (!file) {
        stat_logger.error("Cannot open /proc/stat");
        return -1;
    }
    
    char line[LINE_BUF_SIZE];
    long runQueue = -1;
    
    // Read lines until we find procs_running
    while (fgets(line, LINE_BUF_SIZE, file) != NULL) {
        if (strncmp(line, "procs_running", 13) == 0) {
            // Parse the number after "procs_running "
            char *p = line;
            while (*p && (*p < '0' || *p > '9')) p++;  // Skip to first digit
            if (*p) {
                runQueue = strtol(p, NULL, 10);
                if (runQueue < 0 || runQueue > 0xfffffffffffffffL) {
                    runQueue = -1;  // Invalid value
                }
            }
            break;
        }
    }
    
    fclose(file);
    return runQueue;
}

std::vector<double> StatisticCollector::getLogicalCpuCoreThreadUsage() {
    std::vector<double> cpuUsages;
    
    // First reading
    std::vector<std::vector<long long>> firstReading;
    FILE *file1 = fopen("/proc/stat", "r");
    if (!file1) {
        stat_logger.error("Cannot open /proc/stat for first reading");
        return cpuUsages;
    }
    
    char line[1024];
    // Skip the first line (total cpu stats)
    fgets(line, sizeof(line), file1);
    
    // Read per-CPU stats
    while (fgets(line, sizeof(line), file1) != NULL) {
        if (strncmp(line, "cpu", 3) == 0 && line[3] >= '0' && line[3] <= '9') {
            std::vector<long long> cpuStats;
            char *p = line;
            // Skip "cpu" and cpu number
            while (*p && *p != ' ') p++;
            while (*p == ' ') p++;
            
            // Parse CPU time values: user, nice, system, idle, iowait, irq, softirq, steal
            for (int i = 0; i < 8; i++) {
                long long value = 0;
                if (*p && (*p >= '0' && *p <= '9')) {
                    value = strtoll(p, &p, 10);
                    while (*p == ' ') p++;
                }
                cpuStats.push_back(value);
            }
            firstReading.push_back(cpuStats);
        } else {
            break;  // No more CPU lines
        }
    }
    fclose(file1);
    
    // Sleep for a short interval to get meaningful difference
    usleep(100000);  // 100ms
    
    // Second reading
    std::vector<std::vector<long long>> secondReading;
    FILE *file2 = fopen("/proc/stat", "r");
    if (!file2) {
        stat_logger.error("Cannot open /proc/stat for second reading");
        return cpuUsages;
    }
    
    // Skip the first line (total cpu stats)
    fgets(line, sizeof(line), file2);
    
    // Read per-CPU stats
    while (fgets(line, sizeof(line), file2) != NULL) {
        if (strncmp(line, "cpu", 3) == 0 && line[3] >= '0' && line[3] <= '9') {
            std::vector<long long> cpuStats;
            char *p = line;
            // Skip "cpu" and cpu number
            while (*p && *p != ' ') p++;
            while (*p == ' ') p++;
            
            // Parse CPU time values
            for (int i = 0; i < 8; i++) {
                long long value = 0;
                if (*p && (*p >= '0' && *p <= '9')) {
                    value = strtoll(p, &p, 10);
                    while (*p == ' ') p++;
                }
                cpuStats.push_back(value);
            }
            secondReading.push_back(cpuStats);
        } else {
            break;  // No more CPU lines
        }
    }
    fclose(file2);
    
    // Calculate usage for each CPU
    size_t numCpus = std::min(firstReading.size(), secondReading.size());
    for (size_t i = 0; i < numCpus; i++) {
        if (firstReading[i].size() >= 4 && secondReading[i].size() >= 4) {
            // Calculate total time difference
            long long totalDiff = 0;
            long long idleDiff = 0;
            
            for (int j = 0; j < 8 && j < (int)firstReading[i].size() && j < (int)secondReading[i].size(); j++) {
                long long diff = secondReading[i][j] - firstReading[i][j];
                totalDiff += diff;
                if (j == 3) {  // idle time is the 4th field (index 3)
                    idleDiff = diff;
                }
            }
            
            // Calculate CPU usage percentage
            double usage = 0.0;
            if (totalDiff > 0) {
                usage = ((double)(totalDiff - idleDiff) / totalDiff) * 100.0;
                if (usage < 0.0) usage = 0.0;
                if (usage > 100.0) usage = 100.0;
            }
            cpuUsages.push_back(usage);
        }
    }
    
    return cpuUsages;
}

double StatisticCollector::getProcessSwitchesPerSecond() {
    // First reading of context switches
    FILE *file1 = fopen("/proc/stat", "r");
    if (!file1) {
        stat_logger.error("Cannot open /proc/stat for first reading");
        return -1.0;
    }
    
    char line[LINE_BUF_SIZE];
    long long firstCtxt = -1;
    
    // Find context switches line
    while (fgets(line, LINE_BUF_SIZE, file1) != NULL) {
        if (strncmp(line, "ctxt", 4) == 0) {
            char *p = line;
            while (*p && (*p < '0' || *p > '9')) p++;  // Skip to first digit
            if (*p) {
                firstCtxt = strtoll(p, NULL, 10);
                if (firstCtxt < 0) {
                    firstCtxt = -1;  // Invalid value
                }
            }
            break;
        }
    }
    fclose(file1);
    
    if (firstCtxt == -1) {
        stat_logger.error("Could not read initial context switches");
        return -1.0;
    }
    
    // Record start time
    struct timespec startTime, endTime;
    clock_gettime(CLOCK_MONOTONIC, &startTime);
    
    // Sleep for measurement interval (1 second)
    sleep(1);
    
    // Second reading of context switches
    FILE *file2 = fopen("/proc/stat", "r");
    if (!file2) {
        stat_logger.error("Cannot open /proc/stat for second reading");
        return -1.0;
    }
    
    long long secondCtxt = -1;
    
    // Find context switches line
    while (fgets(line, LINE_BUF_SIZE, file2) != NULL) {
        if (strncmp(line, "ctxt", 4) == 0) {
            char *p = line;
            while (*p && (*p < '0' || *p > '9')) p++;  // Skip to first digit
            if (*p) {
                secondCtxt = strtoll(p, NULL, 10);
                if (secondCtxt < 0) {
                    secondCtxt = -1;  // Invalid value
                }
            }
            break;
        }
    }
    fclose(file2);
    
    if (secondCtxt == -1) {
        stat_logger.error("Could not read final context switches");
        return -1.0;
    }
    
    // Record end time
    clock_gettime(CLOCK_MONOTONIC, &endTime);
    
    // Calculate elapsed time in seconds
    double elapsedTime = (endTime.tv_sec - startTime.tv_sec) + 
                        (endTime.tv_nsec - startTime.tv_nsec) / 1000000000.0;
    
    if (elapsedTime <= 0.0) {
        stat_logger.error("Invalid elapsed time for context switch calculation");
        return -1.0;
    }
    
    // Calculate context switches per second
    long long ctxtDiff = secondCtxt - firstCtxt;
    if (ctxtDiff < 0) {
        stat_logger.error("Context switches counter wrapped or invalid");
        return -1.0;
    }
    
    double switchesPerSecond = (double)ctxtDiff / elapsedTime;
    return switchesPerSecond;
}

double StatisticCollector::getForkCallsPerSecond() {
    // First reading of processes created
    FILE *file1 = fopen("/proc/stat", "r");
    if (!file1) {
        stat_logger.error("Cannot open /proc/stat for first reading");
        return -1.0;
    }
    
    char line[LINE_BUF_SIZE];
    long long firstProcs = -1;
    
    // Find processes line
    while (fgets(line, LINE_BUF_SIZE, file1) != NULL) {
        if (strncmp(line, "processes", 9) == 0) {
            char *p = line;
            while (*p && (*p < '0' || *p > '9')) p++;  // Skip to first digit
            if (*p) {
                firstProcs = strtoll(p, NULL, 10);
                if (firstProcs < 0) {
                    firstProcs = -1;  // Invalid value
                }
            }
            break;
        }
    }
    fclose(file1);
    
    if (firstProcs == -1) {
        stat_logger.error("Could not read initial process count");
        return -1.0;
    }
    
    // Record start time
    struct timespec startTime, endTime;
    clock_gettime(CLOCK_MONOTONIC, &startTime);
    
    // Sleep for measurement interval (1 second)
    sleep(1);
    
    // Second reading of processes created
    FILE *file2 = fopen("/proc/stat", "r");
    if (!file2) {
        stat_logger.error("Cannot open /proc/stat for second reading");
        return -1.0;
    }
    
    long long secondProcs = -1;
    
    // Find processes line
    while (fgets(line, LINE_BUF_SIZE, file2) != NULL) {
        if (strncmp(line, "processes", 9) == 0) {
            char *p = line;
            while (*p && (*p < '0' || *p > '9')) p++;  // Skip to first digit
            if (*p) {
                secondProcs = strtoll(p, NULL, 10);
                if (secondProcs < 0) {
                    secondProcs = -1;  // Invalid value
                }
            }
            break;
        }
    }
    fclose(file2);
    
    if (secondProcs == -1) {
        stat_logger.error("Could not read final process count");
        return -1.0;
    }
    
    // Record end time
    clock_gettime(CLOCK_MONOTONIC, &endTime);
    
    // Calculate elapsed time in seconds
    double elapsedTime = (endTime.tv_sec - startTime.tv_sec) + 
                        (endTime.tv_nsec - startTime.tv_nsec) / 1000000000.0;
    
    if (elapsedTime <= 0.0) {
        stat_logger.error("Invalid elapsed time for fork calculation");
        return -1.0;
    }
    
    // Calculate fork calls per second
    long long procsDiff = secondProcs - firstProcs;
    if (procsDiff < 0) {
        stat_logger.error("Processes counter wrapped or invalid");
        return -1.0;
    }
    
    double forksPerSecond = (double)procsDiff / elapsedTime;
    return forksPerSecond;
}

std::map<std::string, std::pair<double, double>> StatisticCollector::getNetworkPacketsPerSecond() {
    std::map<std::string, std::pair<double, double>> packetRates; // <interface, <input_pps, output_pps>>
    std::map<std::string, std::pair<unsigned long long, unsigned long long>> firstReading;
    
    // First reading of network statistics
    FILE *file1 = fopen("/proc/net/dev", "r");
    if (!file1) {
        stat_logger.error("Cannot open /proc/net/dev for first reading");
        return packetRates;
    }
    
    char line[1024];
    // Skip header lines
    if (fgets(line, sizeof(line), file1) == NULL || fgets(line, sizeof(line), file1) == NULL) {
        stat_logger.error("Cannot read header lines from /proc/net/dev");
        fclose(file1);
        return packetRates;
    }
    
    // Read network interface statistics
    while (fgets(line, sizeof(line), file1) != NULL) {
        char interface[32];
        unsigned long long rx_bytes, rx_packets, rx_errs, rx_drop, rx_fifo, rx_frame, rx_compressed, rx_multicast;
        unsigned long long tx_bytes, tx_packets, tx_errs, tx_drop, tx_fifo, tx_colls, tx_carrier, tx_compressed;
        
        // Strip spaces and parse the line
        char *p = line;
        while (*p == ' ' || *p == '\t') p++;  // Skip leading whitespace
        
        int ret = sscanf(p, "%31[^:]: %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu",
            interface,
            &rx_bytes, &rx_packets, &rx_errs, &rx_drop, &rx_fifo, &rx_frame, &rx_compressed, &rx_multicast,
            &tx_bytes, &tx_packets, &tx_errs, &tx_drop, &tx_fifo, &tx_colls, &tx_carrier, &tx_compressed);
        
        if (ret == 17) {
            std::string ifName(interface);
            firstReading[ifName] = std::make_pair(rx_packets, tx_packets);
        }
    }
    fclose(file1);
    
    if (firstReading.empty()) {
        stat_logger.error("No network interfaces found in first reading");
        return packetRates;
    }
    
    // Record start time
    struct timespec startTime, endTime;
    clock_gettime(CLOCK_MONOTONIC, &startTime);
    
    // Sleep for measurement interval (1 second)
    sleep(1);
    
    // Second reading of network statistics
    FILE *file2 = fopen("/proc/net/dev", "r");
    if (!file2) {
        stat_logger.error("Cannot open /proc/net/dev for second reading");
        return packetRates;
    }
    
    // Skip header lines
    if (fgets(line, sizeof(line), file2) == NULL || fgets(line, sizeof(line), file2) == NULL) {
        stat_logger.error("Cannot read header lines from /proc/net/dev in second reading");
        fclose(file2);
        return packetRates;
    }
    
    // Read network interface statistics again
    while (fgets(line, sizeof(line), file2) != NULL) {
        char interface[32];
        unsigned long long rx_bytes, rx_packets, rx_errs, rx_drop, rx_fifo, rx_frame, rx_compressed, rx_multicast;
        unsigned long long tx_bytes, tx_packets, tx_errs, tx_drop, tx_fifo, tx_colls, tx_carrier, tx_compressed;
        
        // Strip spaces and parse the line
        char *p = line;
        while (*p == ' ' || *p == '\t') p++;  // Skip leading whitespace
        
        int ret = sscanf(p, "%31[^:]: %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu",
            interface,
            &rx_bytes, &rx_packets, &rx_errs, &rx_drop, &rx_fifo, &rx_frame, &rx_compressed, &rx_multicast,
            &tx_bytes, &tx_packets, &tx_errs, &tx_drop, &tx_fifo, &tx_colls, &tx_carrier, &tx_compressed);
        
        if (ret == 17) {
            std::string ifName(interface);
            
            // Check if we have first reading for this interface
            if (firstReading.find(ifName) != firstReading.end()) {
                unsigned long long firstRxPackets = firstReading[ifName].first;
                unsigned long long firstTxPackets = firstReading[ifName].second;
                
                // Calculate packet differences (handle counter wraparound)
                long long rxDiff = (rx_packets >= firstRxPackets) ? (rx_packets - firstRxPackets) : 0;
                long long txDiff = (tx_packets >= firstTxPackets) ? (tx_packets - firstTxPackets) : 0;
                
                // Store the rates
                packetRates[ifName] = std::make_pair((double)rxDiff, (double)txDiff);
            }
        }
    }
    fclose(file2);
    
    // Record end time
    clock_gettime(CLOCK_MONOTONIC, &endTime);
    
    // Calculate elapsed time in seconds
    double elapsedTime = (endTime.tv_sec - startTime.tv_sec) + 
                        (endTime.tv_nsec - startTime.tv_nsec) / 1000000000.0;
    
    if (elapsedTime <= 0.0) {
        stat_logger.error("Invalid elapsed time for network packet calculation");
        return packetRates;
    }
    
    // Convert to per-second rates
    for (auto& entry : packetRates) {
        entry.second.first /= elapsedTime;   // RX packets per second
        entry.second.second /= elapsedTime;  // TX packets per second
    }
    
    return packetRates;
}

std::map<std::string, double> StatisticCollector::getDiskBusyPercentage() {
    std::map<std::string, double> diskBusyRates;
    Logger stat_logger("StatisticCollector", "logs/main", "w");
    
    struct timespec startTime, endTime;
    
    // Record start time
    clock_gettime(CLOCK_MONOTONIC, &startTime);
    
    // First reading
    std::map<std::string, unsigned long long> firstReading;
    FILE *file1 = fopen("/proc/diskstats", "r");
    if (!file1) {
        stat_logger.error("Cannot open /proc/diskstats for first reading");
        return diskBusyRates;
    }
    
    char line[256];
    while (fgets(line, sizeof(line), file1) != NULL) {
        int major, minor;
        char device[32];
        unsigned long long reads_completed, reads_merged, sectors_read, time_reading;
        unsigned long long writes_completed, writes_merged, sectors_written, time_writing;
        unsigned long long ios_in_progress, io_time, weighted_io_time;
        
        // Parse the diskstats line (11 or more fields)
        int ret = sscanf(line, "%d %d %31s %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu",
                        &major, &minor, device,
                        &reads_completed, &reads_merged, &sectors_read, &time_reading,
                        &writes_completed, &writes_merged, &sectors_written, &time_writing,
                        &ios_in_progress, &io_time, &weighted_io_time);
        
        if (ret >= 14) {  // We need at least 14 fields to get io_time
            // Skip loop devices and ram devices
            if (strncmp(device, "loop", 4) != 0 && strncmp(device, "ram", 3) != 0) {
                firstReading[device] = io_time;  // io_time is in milliseconds
            }
        }
    }
    fclose(file1);
    
    if (firstReading.empty()) {
        stat_logger.error("No valid disk devices found in first reading");
        return diskBusyRates;
    }
    
    // Sleep for a short interval to get meaningful difference
    usleep(1000000);  // 1 second
    
    // Second reading
    std::map<std::string, unsigned long long> secondReading;
    FILE *file2 = fopen("/proc/diskstats", "r");
    if (!file2) {
        stat_logger.error("Cannot open /proc/diskstats for second reading");
        return diskBusyRates;
    }
    
    while (fgets(line, sizeof(line), file2) != NULL) {
        int major, minor;
        char device[32];
        unsigned long long reads_completed, reads_merged, sectors_read, time_reading;
        unsigned long long writes_completed, writes_merged, sectors_written, time_writing;
        unsigned long long ios_in_progress, io_time, weighted_io_time;
        
        // Parse the diskstats line (11 or more fields)
        int ret = sscanf(line, "%d %d %31s %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu",
                        &major, &minor, device,
                        &reads_completed, &reads_merged, &sectors_read, &time_reading,
                        &writes_completed, &writes_merged, &sectors_written, &time_writing,
                        &ios_in_progress, &io_time, &weighted_io_time);
        
        if (ret >= 14) {  // We need at least 14 fields to get io_time
            // Skip loop devices and ram devices
            if (strncmp(device, "loop", 4) != 0 && strncmp(device, "ram", 3) != 0) {
                secondReading[device] = io_time;  // io_time is in milliseconds
            }
        }
    }
    fclose(file2);
    
    // Record end time
    clock_gettime(CLOCK_MONOTONIC, &endTime);
    
    // Calculate elapsed time in milliseconds (to match io_time units)
    double elapsedTimeMs = ((endTime.tv_sec - startTime.tv_sec) * 1000.0) + 
                           ((endTime.tv_nsec - startTime.tv_nsec) / 1000000.0);
    
    if (elapsedTimeMs <= 0.0) {
        stat_logger.error("Invalid elapsed time for disk busy calculation");
        return diskBusyRates;
    }
    
    // Calculate disk busy percentage for each device
    for (const auto& entry : firstReading) {
        const std::string& device = entry.first;
        unsigned long long firstTime = entry.second;
        
        if (secondReading.find(device) != secondReading.end()) {
            unsigned long long secondTime = secondReading[device];
            
            // Calculate the delta (handling potential counter wraparound)
            unsigned long long deltaTime;
            if (secondTime >= firstTime) {
                deltaTime = secondTime - firstTime;
            } else {
                // Counter wrapped around, assume it's a 64-bit counter
                deltaTime = (ULLONG_MAX - firstTime) + secondTime + 1;
            }
            
            // Calculate busy percentage: (delta_io_time / elapsed_time) * 100
            // Both times are in milliseconds
            double busyPercentage = (static_cast<double>(deltaTime) / elapsedTimeMs) * 100.0;
            
            // Cap at 100% to handle any calculation anomalies
            if (busyPercentage > 100.0) {
                busyPercentage = 100.0;
            }
            
            diskBusyRates[device] = busyPercentage;
        }
    }
    
    return diskBusyRates;
}

std::map<std::string, std::pair<double, double>> StatisticCollector::getDiskReadWriteKBPerSecond() {
    std::map<std::string, std::pair<double, double>> diskRates;
    Logger stat_logger("StatisticCollector", "logs/main", "w");
    
    struct timespec startTime, endTime;
    
    // Record start time
    clock_gettime(CLOCK_MONOTONIC, &startTime);
    
    // First reading - store sectors_read and sectors_written for each device
    std::map<std::string, std::pair<unsigned long long, unsigned long long>> firstReading;
    FILE *file1 = fopen("/proc/diskstats", "r");
    if (!file1) {
        stat_logger.error("Cannot open /proc/diskstats for first reading");
        return diskRates;
    }
    
    char line[256];
    while (fgets(line, sizeof(line), file1) != NULL) {
        int major, minor;
        char device[32];
        unsigned long long reads_completed, reads_merged, sectors_read, time_reading;
        unsigned long long writes_completed, writes_merged, sectors_written, time_writing;
        unsigned long long ios_in_progress, io_time, weighted_io_time;
        
        // Parse the diskstats line (14 fields)
        int ret = sscanf(line, "%d %d %31s %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu",
                        &major, &minor, device,
                        &reads_completed, &reads_merged, &sectors_read, &time_reading,
                        &writes_completed, &writes_merged, &sectors_written, &time_writing,
                        &ios_in_progress, &io_time, &weighted_io_time);
        
        if (ret >= 14) {  // We need all 14 fields for full disk devices
            // Skip loop devices and ram devices
            if (strncmp(device, "loop", 4) != 0 && strncmp(device, "ram", 3) != 0) {
                firstReading[device] = std::make_pair(sectors_read, sectors_written);
            }
        }
    }
    fclose(file1);
    
    if (firstReading.empty()) {
        stat_logger.error("No valid disk devices found in first reading");
        return diskRates;
    }
    
    // Sleep for a short interval to get meaningful difference
    usleep(1000000);  // 1 second
    
    // Second reading
    std::map<std::string, std::pair<unsigned long long, unsigned long long>> secondReading;
    FILE *file2 = fopen("/proc/diskstats", "r");
    if (!file2) {
        stat_logger.error("Cannot open /proc/diskstats for second reading");
        return diskRates;
    }
    
    while (fgets(line, sizeof(line), file2) != NULL) {
        int major, minor;
        char device[32];
        unsigned long long reads_completed, reads_merged, sectors_read, time_reading;
        unsigned long long writes_completed, writes_merged, sectors_written, time_writing;
        unsigned long long ios_in_progress, io_time, weighted_io_time;
        
        // Parse the diskstats line (14 fields)
        int ret = sscanf(line, "%d %d %31s %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu",
                        &major, &minor, device,
                        &reads_completed, &reads_merged, &sectors_read, &time_reading,
                        &writes_completed, &writes_merged, &sectors_written, &time_writing,
                        &ios_in_progress, &io_time, &weighted_io_time);
        
        if (ret >= 14) {  // We need all 14 fields for full disk devices
            // Skip loop devices and ram devices
            if (strncmp(device, "loop", 4) != 0 && strncmp(device, "ram", 3) != 0) {
                secondReading[device] = std::make_pair(sectors_read, sectors_written);
            }
        }
    }
    fclose(file2);
    
    // Record end time
    clock_gettime(CLOCK_MONOTONIC, &endTime);
    
    // Calculate elapsed time in seconds
    double elapsedTime = (endTime.tv_sec - startTime.tv_sec) + 
                        (endTime.tv_nsec - startTime.tv_nsec) / 1000000000.0;
    
    if (elapsedTime <= 0.0) {
        stat_logger.error("Invalid elapsed time for disk read/write calculation");
        return diskRates;
    }
    
    // Calculate read/write KB per second for each device
    for (const auto& entry : firstReading) {
        const std::string& device = entry.first;
        unsigned long long firstSectorsRead = entry.second.first;
        unsigned long long firstSectorsWritten = entry.second.second;
        
        if (secondReading.find(device) != secondReading.end()) {
            unsigned long long secondSectorsRead = secondReading[device].first;
            unsigned long long secondSectorsWritten = secondReading[device].second;
            
            // Calculate the deltas (handling potential counter wraparound)
            unsigned long long deltaSectorsRead, deltaSectorsWritten;
            
            if (secondSectorsRead >= firstSectorsRead) {
                deltaSectorsRead = secondSectorsRead - firstSectorsRead;
            } else {
                // Counter wrapped around, assume it's a 64-bit counter
                deltaSectorsRead = (ULLONG_MAX - firstSectorsRead) + secondSectorsRead + 1;
            }
            
            if (secondSectorsWritten >= firstSectorsWritten) {
                deltaSectorsWritten = secondSectorsWritten - firstSectorsWritten;
            } else {
                // Counter wrapped around, assume it's a 64-bit counter
                deltaSectorsWritten = (ULLONG_MAX - firstSectorsWritten) + secondSectorsWritten + 1;
            }
            
            // Convert sectors to KB: sectors are 512 bytes, so divide by 2 to get KB
            // Then divide by elapsed time to get KB per second
            double readKBPerSecond = (static_cast<double>(deltaSectorsRead) / 2.0) / elapsedTime;
            double writeKBPerSecond = (static_cast<double>(deltaSectorsWritten) / 2.0) / elapsedTime;
            
            diskRates[device] = std::make_pair(readKBPerSecond, writeKBPerSecond);
        }
    }
    
    return diskRates;
}

void StatisticCollector::logLoadAverage(std::string name) {
    PerformanceUtil::logLoadAverage();

    int elapsedTime = 0;
    time_t start;
    time_t end;
    PerformanceUtil performanceUtil;
    performanceUtil.init();

    start = time(0);
    while (true) {
        if (isStatCollect) {
            std::this_thread::sleep_for(std::chrono::seconds(60));
            continue;
        }

        time_t elapsed = time(0) - start;
        if (elapsed >= Conts::LOAD_AVG_COLLECTING_GAP) {
            elapsedTime += Conts::LOAD_AVG_COLLECTING_GAP * 1000;
            PerformanceUtil::logLoadAverage();
            start = start + Conts::LOAD_AVG_COLLECTING_GAP;
        } else {
            sleep(Conts::LOAD_AVG_COLLECTING_GAP - elapsed);
        }
    }
}
