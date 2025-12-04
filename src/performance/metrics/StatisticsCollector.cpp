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

#include "StatisticsCollector.h"
#include <time.h>
#include <unistd.h>

constexpr std::size_t LINE_BUF_SIZE = 128;
constexpr std::size_t LINE_BUF_SIZE_LONG = 256;

// Global variables
Logger stat_logger;
static int numProcessors;

// Data structures for system statistics
struct DiskStats {
    unsigned long reads_completed = 0;
    unsigned long reads_merged = 0;
    unsigned long sectors_read = 0;
    unsigned long time_reading = 0;
    unsigned long writes_completed = 0;
    unsigned long writes_merged = 0;
    unsigned long sectors_written = 0;
    unsigned long time_writing = 0;
    unsigned long ios_in_progress = 0;
    unsigned long io_time = 0;
    unsigned long weighted_io_time = 0;
};

struct NetworkStats {
    unsigned long rx_packets = 0;
    unsigned long tx_packets = 0;
};

// Forward declarations for internal helper functions
static long parseLine(char *line);
static long getSwapSpace(const char *type);
static void getCpuCycles(long *totalp, long *idlep);

// Time calculation utilities
static double calculateElapsedTime(const struct timespec& startTime, const struct timespec& endTime) {
    return (endTime.tv_sec - startTime.tv_sec) +
           (endTime.tv_nsec - startTime.tv_nsec) / 1000000000.0;
}

static double calculateElapsedTimeMs(const struct timespec& startTime, const struct timespec& endTime) {
    return ((endTime.tv_sec - startTime.tv_sec) * 1000.0) +
           ((endTime.tv_nsec - startTime.tv_nsec) / 1000000.0);
}

// /proc/stat operations
static long readProcStatValue(const char* prefix, int prefixLen) {
    FILE *file = fopen("/proc/stat", "r");
    if (!file) {
        stat_logger.error("Cannot open /proc/stat");
        return -1;
    }

    std::string line;
    std::string buffer(LINE_BUF_SIZE, '\0');
    long value = -1;

    while (fgets(&buffer[0], LINE_BUF_SIZE, file) != nullptr) {
        line = buffer.c_str();
        std::string_view lineView(line);
        std::string_view prefixView(prefix, prefixLen);
        if (lineView.substr(0, prefixLen) != prefixView) {
            continue;
        }
        const char *p = line.c_str();
        while (*p && (*p < '0' || *p > '9')) p++;  // Skip to first digit
        if (*p) {
            value = strtoll(p, nullptr, 10);
            if (value < 0) {
                value = -1;  // Invalid value
            }
        }
        break;
    }

    fclose(file);
    return value;
}

static double measureProcStatRate(const char* prefix, int prefixLen, int sleepSeconds) {
    // First reading
    long firstValue = readProcStatValue(prefix, prefixLen);
    if (firstValue == -1) {
        stat_logger.error(std::string("Could not read initial ") + prefix + " value");
        return -1.0;
    }

    // Record start time
    struct timespec startTime, endTime;
    clock_gettime(CLOCK_MONOTONIC, &startTime);

    // Sleep for measurement interval
    sleep(sleepSeconds);

    // Second reading
    long secondValue = readProcStatValue(prefix, prefixLen);
    if (secondValue == -1) {
        stat_logger.error(std::string("Could not read final ") + prefix + " value");
        return -1.0;
    }

    // Record end time
    clock_gettime(CLOCK_MONOTONIC, &endTime);

    // Calculate elapsed time in seconds
    double elapsedTime = calculateElapsedTime(startTime, endTime);

    if (elapsedTime <= 0.0) {
        stat_logger.error(std::string("Invalid elapsed time for ") + prefix + " calculation");
        return -1.0;
    }

    // Calculate rate per second
    long valueDiff = secondValue - firstValue;
    if (valueDiff < 0) {
        stat_logger.error(std::string(prefix) + " counter wrapped or invalid");
        return -1.0;
    }

    return (double)valueDiff / elapsedTime;
}

// CPU statistics operations
static bool readCpuStats(std::vector<std::vector<long>> &readings, const std::string& errorContext = "") {
    std::ifstream file("/proc/stat");
    if (!file.is_open()) {
        std::string msg = "Cannot open /proc/stat";
        if (!errorContext.empty()) {
            msg += " for " + errorContext;
        }
        stat_logger.error(msg);
        return false;
    }

    std::string line;

    // Skip the first "cpu" aggregate line
    if (!std::getline(file, line)) {
        return true;  // Nothing else to process
    }

    // Read per-CPU lines: cpu0, cpu1, cpu2, ...
    while (std::getline(file, line)) {
        if (line.rfind("cpu", 0) != 0 || !isdigit(line[3])) {
            break;  // No more cpuN lines
        }

        std::istringstream iss(line);

        std::string cpuLabel;
        iss >> cpuLabel;  // e.g. "cpu0"

        std::vector<long> cpuStats;
        cpuStats.reserve(8);

        long value;
        for (int i = 0; i < 8; ++i) {
            if (!(iss >> value)) {
                value = 0;  // Default fallback if parsing fails
            }
            cpuStats.push_back(value);
        }

        readings.push_back(std::move(cpuStats));
    }

    return true;
}

static void getCpuCycles(long *totalp, long *idlep) {
    *totalp = 0;
    *idlep = 0;
    FILE *fp = fopen("/proc/stat", "r");
    if (!fp) return;
    char line[1024];
    fscanf(fp, "%[^\r\n]%*c", line);
    fclose(fp);

    char *p = line;
    while (*p < '0' || *p > '9') p++;
    long total = 0;
    long idle = 0;
    char *end_ptr = p;
    for (int field = 1; field <= 10; field++) {
        while (*p < '0' || *p > '9') {
            if (!(*p)) break;
            p++;
        }
        if (!(*p)) break;
        long value = strtoll(p, &end_ptr, 10);
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

// Network statistics operations
static bool readNetworkStats(std::map<std::string, NetworkStats, std::less<>> &out,
                const std::string& errorContext = "") {
    FILE *file = fopen("/proc/net/dev", "r");
    if (!file) {
        std::string msg = "Cannot open /proc/net/dev";
        if (!errorContext.empty()) {
            msg += " for " + errorContext;
        }
        stat_logger.error(msg);
        return false;
    }

    std::string line;
    std::string buffer(LINE_BUF_SIZE_LONG, '\0');
    // Skip header lines
    if (fgets(&buffer[0], LINE_BUF_SIZE_LONG, file) == nullptr || fgets(&buffer[0], LINE_BUF_SIZE_LONG, file) == nullptr) {
        std::string msg = "Cannot read header lines from /proc/net/dev";
        if (!errorContext.empty()) {
            msg += " in " + errorContext;
        }
        stat_logger.error(msg);
        fclose(file);
        return false;
    }

    // Read network interface statistics
    while (fgets(&buffer[0], LINE_BUF_SIZE_LONG, file) != nullptr) {
        line = buffer.c_str();
        std::string interface;
        unsigned long rx_bytes;
        unsigned long rx_packets;
        unsigned long rx_errs;
        unsigned long rx_drop;
        unsigned long rx_fifo;
        unsigned long rx_frame;
        unsigned long rx_compressed;
        unsigned long rx_multicast;
        unsigned long tx_bytes;
        unsigned long tx_packets;
        unsigned long tx_errs;
        unsigned long tx_drop;
        unsigned long tx_fifo;
        unsigned long tx_colls;
        unsigned long tx_carrier;
        unsigned long tx_compressed;

        // Strip spaces and parse the line
        const char *p = line.c_str();
        while (*p == ' ' || *p == '\t') p++;  // Skip leading whitespace

        std::string interfaceBuffer(32, '\0');
        int ret = sscanf(p, "%31[^:]: %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu",
            &interfaceBuffer[0],
            &rx_bytes, &rx_packets, &rx_errs, &rx_drop, &rx_fifo, &rx_frame, &rx_compressed, &rx_multicast,
            &tx_bytes, &tx_packets, &tx_errs, &tx_drop, &tx_fifo, &tx_colls, &tx_carrier, &tx_compressed);

        if (ret == 17) {
            interface = interfaceBuffer.c_str();
            NetworkStats ns;
            ns.rx_packets = rx_packets;
            ns.tx_packets = tx_packets;
            out[interface] = ns;
        }
    }

    fclose(file);
    return true;
}

// Disk statistics operations
static bool readDiskStats(std::map<std::string, DiskStats, std::less<>> &out, const std::string& errorContext = "") {
    FILE *file = fopen("/proc/diskstats", "r");
    if (!file) {
        std::string msg = "Cannot open /proc/diskstats";
        if (!errorContext.empty()) {
            msg += " for " + errorContext;
        }
        stat_logger.error(msg);
        return false;
    }

    std::string line;
    std::string buffer(LINE_BUF_SIZE_LONG, '\0');
    while (fgets(&buffer[0], LINE_BUF_SIZE_LONG, file) != nullptr) {
        line = buffer.c_str();
        int major = 0;
        int minor = 0;
        std::string device;
        DiskStats ds;

        // Parse up to 14 fields; sscanf will fill available values.
        std::string deviceBuffer(32, '\0');
        int ret = sscanf(line.c_str(), "%d %d %31s %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu",
                         &major, &minor, &deviceBuffer[0],
                         &ds.reads_completed, &ds.reads_merged, &ds.sectors_read, &ds.time_reading,
                         &ds.writes_completed, &ds.writes_merged, &ds.sectors_written, &ds.time_writing,
                         &ds.ios_in_progress, &ds.io_time, &ds.weighted_io_time);

        if (ret >= 3) {
            device = deviceBuffer.c_str();
            // Skip loop and ram devices
            if (device.compare(0, 4, "loop") != 0 && device.compare(0, 3, "ram") != 0) {
                out[device] = ds;
            }
        }
    }

    fclose(file);
    return true;
}

// Memory and process utilities
static long parseLine(char *line) {
    int i = strlen(line);
    const char *p = line;
    while (*p < '0' || *p > '9') p++;
    line[i - 3] = '\0';
    long val = strtol(p, nullptr, 10);
    if (val < 0 || val > 0xfffffffffffffffL) return -1;
    return val;
}

static long getSwapSpace(int field) {
    FILE *file = fopen("/proc/swaps", "r");
    long result = -1;
    char line[LINE_BUF_SIZE];

    fgets(line, LINE_BUF_SIZE, file);

    while (fgets(line, LINE_BUF_SIZE, file) != nullptr) {
        char *value = nullptr;
        char *save = nullptr;
        for (int i = 0; i < field; i++) {
            if (i == 0) {
                value = strtok_r(line, " ", &save);
            } else {
                value = strtok_r(nullptr, "\t", &save);
            }
        }
        long used = strtol(value, nullptr, 10);
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

int StatisticsCollector::init() {
    FILE *file;
    struct tms timeSample;
    char line[LINE_BUF_SIZE];

    file = fopen("/proc/cpuinfo", "r");
    if (!file) {
        stat_logger.error("Cannot open /proc/cpuinfo");
        exit(-1);
    }
    numProcessors = 0;
    while (fgets(line, LINE_BUF_SIZE, file) != nullptr) {
        if (strncmp(line, "processor", 9) == 0) numProcessors++;
    }
    fclose(file);
    return 0;
}

long StatisticsCollector::getMemoryUsageByProcess() {
    FILE *file = fopen("/proc/self/status", "r");
    if (!file) {
        stat_logger.error("Cannot open /proc/self/status");
        return -1;
    }
    long result = -1;
    char line[LINE_BUF_SIZE];

    while (fgets(line, LINE_BUF_SIZE, file) != nullptr) {
        if (strncmp(line, "VmSize:", 7) == 0) {
            result = parseLine(line);
            break;
        }
    }
    fclose(file);
    return result;
}

long StatisticsCollector::getUsedSwapSpace() {
    long result = getSwapSpace(4);
    return result;
}

long StatisticsCollector::getTotalSwapSpace() {
    long result = getSwapSpace(3);
    return result;
}

long StatisticsCollector::getRXBytes() {
    FILE *file = fopen("/sys/class/net/eth0/statistics/rx_bytes", "r");
    long result = -1;
    fscanf(file, "%li", &result);
    fclose(file);
    return result;
}

long StatisticsCollector::getTXBytes() {
    FILE *file = fopen("/sys/class/net/eth0/statistics/tx_bytes", "r");
    long result = -1;
    fscanf(file, "%li", &result);
    fclose(file);
    return result;
}

// Memory operations
long StatisticsCollector::getTotalMemoryAllocated() {
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
double StatisticsCollector::getMemoryUsagePercentage() {
    long totalMem = getTotalMemoryAllocated();   // in KB
    long usedMem = getTotalMemoryUsage();        // in KB

    if (totalMem <= 0) return -1.0;             // avoid division by zero

    return (usedMem / (double)totalMem);
}


int StatisticsCollector::getTotalNumberofCores() {
    unsigned concurentThreadsSupported = std::thread::hardware_concurrency();
    return concurentThreadsSupported;
}

double StatisticsCollector::getCpuLoadPercentage() {
    int cores = getTotalNumberofCores();       // total CPU cores
    double load = getLoadAverage();            // 1-min load average

    if (cores <= 0) return -1.0;              // avoid division by zero

    return (load / cores);
}
long StatisticsCollector::getTotalMemoryUsage() {
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

// Process and thread operations
int StatisticsCollector::getThreadCount() {
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
    result = strtol(line, nullptr, 10);
    if (result <= 0 || result > 0xfffffffffffffffL) return -1;
    return result;
}

int StatisticsCollector::getSocketCount() {
    DIR *d = opendir("/proc/self/fd");
    if (!d) {
        puts("Error opening directory /proc/self/fd");
        return -1;
    }
    const struct dirent *dir;
    char path[64];
    char link_buf[1024];
    int count = 0;
    while ((dir = readdir(d)) != nullptr) {
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

// CPU statistics
double StatisticsCollector::getCpuUsage() {
    long total1;
    long idle1;
    getCpuCycles(&total1, &idle1);
    sleep(5);
    long total2;
    long idle2;
    getCpuCycles(&total2, &idle2);

    long diffTotal = total2 - total1;
    long diffIdle = idle2 - idle1;

    return (diffTotal - diffIdle) / (double)diffTotal;
}

double StatisticsCollector::getTotalCpuUsage() {
    std::string mpstatCommand = "mpstat";
    std::string buffer(BUFFER_SIZE, '\0');
    std::string result = "";
    std::vector<std::string>::iterator paramNameIterator;
    int count = 0;
    double totalCPUUsage = 0;

    FILE *input = popen(mpstatCommand.c_str(), "r");

    if (input) {
        // read the input
        while (!feof(input)) {
            if (fgets(&buffer[0], BUFFER_SIZE, input) != nullptr) {
                result.append(buffer.c_str());
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

double StatisticsCollector::getLoadAverage() {
    double loadAvg;
    getloadavg(&loadAvg, 1);
    return loadAvg;
}

long StatisticsCollector::getRunQueue() {
    return readProcStatValue("procs_running", 13);
}

// Helper function to calculate CPU usage for a single core
static double calculateSingleCoreUsage(const std::vector<long>& firstReading, const std::vector<long>& secondReading) {
    if (firstReading.size() < 4 || secondReading.size() < 4) {
        return 0.0;
    }

    long totalDiff = 0;
    long idleDiff = 0;

    int maxFields = std::min({8, (int)firstReading.size(), (int)secondReading.size()});
    for (int j = 0; j < maxFields; j++) {
        long diff = secondReading[j] - firstReading[j];
        totalDiff += diff;
        if (j == 3) {  // idle time is the 4th field (index 3)
            idleDiff = diff;
        }
    }

    if (totalDiff <= 0) {
        return 0.0;
    }

    double usage = ((double)(totalDiff - idleDiff) / totalDiff) * 100.0;
    return std::max(0.0, std::min(100.0, usage));
}

std::vector<double> StatisticsCollector::getLogicalCpuCoreThreadUsage() {
    std::vector<double> cpuUsages;

    // First reading
    std::vector<std::vector<long>> firstReading;
    if (!readCpuStats(firstReading, "first reading")) {
        return cpuUsages;
    }

    // Sleep for a short interval to get meaningful difference
    struct timespec sleepTime = {0, 100000000};  // 100ms
    nanosleep(&sleepTime, nullptr);

    // Second reading
    std::vector<std::vector<long>> secondReading;
    if (!readCpuStats(secondReading, "second reading")) {
        return cpuUsages;
    }

    // Calculate usage for each CPU
    size_t numCpus = std::min(firstReading.size(), secondReading.size());
    cpuUsages.reserve(numCpus);

    for (size_t i = 0; i < numCpus; i++) {
        double usage = calculateSingleCoreUsage(firstReading[i], secondReading[i]);
        cpuUsages.push_back(usage);
    }

    return cpuUsages;
}

double StatisticsCollector::getProcessSwitchesPerSecond() {
    return measureProcStatRate("ctxt", 4, 1);
}

double StatisticsCollector::getForkCallsPerSecond() {
    return measureProcStatRate("processes", 9, 1);
}

// Network operations
std::map<std::string, std::pair<double, double>, std::less<>> StatisticsCollector::getNetworkPacketsPerSecond() {
    std::map<std::string, std::pair<double, double>, std::less<>> packetRates;

    // First reading of network statistics
    std::map<std::string, NetworkStats, std::less<>> firstReading;
    if (!readNetworkStats(firstReading, "first reading")) {
        return packetRates;
    }

    if (firstReading.empty()) {
        stat_logger.error("No network interfaces found in first reading");
        return packetRates;
    }

    // Record start time
    struct timespec startTime;
    struct timespec endTime;
    clock_gettime(CLOCK_MONOTONIC, &startTime);

    // Sleep for measurement interval (1 second)
    sleep(1);

    // Second reading of network statistics
    std::map<std::string, NetworkStats, std::less<>> secondReading;
    if (!readNetworkStats(secondReading, "second reading")) {
        return packetRates;
    }

    // Record end time
    clock_gettime(CLOCK_MONOTONIC, &endTime);

    // Calculate elapsed time in seconds
    double elapsedTime = calculateElapsedTime(startTime, endTime);

    if (elapsedTime <= 0.0) {
        stat_logger.error("Invalid elapsed time for network packet calculation");
        return packetRates;
    }

    // Calculate packet differences and rates for each interface
    for (const auto& [ifName, firstStats] : firstReading) {
        // Check if we have second reading for this interface
        if (secondReading.find(ifName) != secondReading.end()) {
            const NetworkStats& secondStats = secondReading[ifName];

            // Calculate packet differences (handle counter wraparound)
            long rxDiff = (secondStats.rx_packets >= firstStats.rx_packets) ?
                              (secondStats.rx_packets - firstStats.rx_packets) : 0;
            long txDiff = (secondStats.tx_packets >= firstStats.tx_packets) ?
                              (secondStats.tx_packets - firstStats.tx_packets) : 0;

            // Calculate rates per second
            double rxPacketsPerSecond = (double)rxDiff / elapsedTime;
            double txPacketsPerSecond = (double)txDiff / elapsedTime;

            packetRates[ifName] = std::make_pair(rxPacketsPerSecond, txPacketsPerSecond);
        }
    }

    return packetRates;
}

// Disk operations
std::map<std::string, double, std::less<>> StatisticsCollector::getDiskBusyPercentage() {
    std::map<std::string, double, std::less<>> diskBusyRates;

    struct timespec startTime;
    struct timespec endTime;

    // Record start time
    clock_gettime(CLOCK_MONOTONIC, &startTime);

    // First reading
    std::map<std::string, DiskStats, std::less<>> firstReading;
    if (!readDiskStats(firstReading, "first reading")) {
        return diskBusyRates;
    }

    if (firstReading.empty()) {
        stat_logger.error("No valid disk devices found in first reading");
        return diskBusyRates;
    }

    // Sleep for a short interval to get meaningful difference
    struct timespec sleepTime = {1, 0};  // 1 second
    nanosleep(&sleepTime, nullptr);

    // Second reading
    std::map<std::string, DiskStats, std::less<>> secondReading;
    if (!readDiskStats(secondReading, "second reading")) {
        return diskBusyRates;
    }

    // Record end time
    clock_gettime(CLOCK_MONOTONIC, &endTime);

    // Calculate elapsed time in milliseconds (to match io_time units)
    double elapsedTimeMs = calculateElapsedTimeMs(startTime, endTime);

    if (elapsedTimeMs <= 0.0) {
        stat_logger.error("Invalid elapsed time for disk busy calculation");
        return diskBusyRates;
    }

    // Calculate disk busy percentage for each device
    for (const auto& [device, diskStats] : firstReading) {
        unsigned long firstTime = diskStats.io_time;

        if (secondReading.find(device) != secondReading.end()) {
            unsigned long secondTime = secondReading[device].io_time;

            // Calculate the delta (handling potential counter wraparound)
            unsigned long deltaTime;
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

std::map<std::string, std::pair<double, double>, std::less<>> StatisticsCollector::getDiskReadWriteKBPerSecond() {
    std::map<std::string, std::pair<double, double>, std::less<>> diskRates;

    struct timespec startTime;
    struct timespec endTime;

    // Record start time
    clock_gettime(CLOCK_MONOTONIC, &startTime);

    // First reading - store sectors_read and sectors_written for each device
    std::map<std::string, DiskStats, std::less<>> firstReading;
    if (!readDiskStats(firstReading, "first reading")) {
        return diskRates;
    }

    if (firstReading.empty()) {
        stat_logger.error("No valid disk devices found in first reading");
        return diskRates;
    }

    // Sleep for a short interval to get meaningful difference
    struct timespec sleepTime = {1, 0};  // 1 second
    nanosleep(&sleepTime, nullptr);

    // Second reading
    std::map<std::string, DiskStats, std::less<>> secondReading;
    if (!readDiskStats(secondReading, "second reading")) {
        return diskRates;
    }

    // Record end time
    clock_gettime(CLOCK_MONOTONIC, &endTime);

    // Calculate elapsed time in seconds
    double elapsedTime = calculateElapsedTime(startTime, endTime);

    if (elapsedTime <= 0.0) {
        stat_logger.error("Invalid elapsed time for disk read/write calculation");
        return diskRates;
    }

    // Calculate read/write KB per second for each device
    for (const auto& [device, diskStats] : firstReading) {
        unsigned long firstSectorsRead = diskStats.sectors_read;
        unsigned long firstSectorsWritten = diskStats.sectors_written;

        if (secondReading.find(device) != secondReading.end()) {
            unsigned long secondSectorsRead = secondReading[device].sectors_read;
            unsigned long secondSectorsWritten = secondReading[device].sectors_written;

            // Calculate the deltas (handling potential counter wraparound)
            unsigned long deltaSectorsRead;
            unsigned long deltaSectorsWritten;

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

std::map<std::string, double, std::less<>> StatisticsCollector::getDiskBlockSizeKB() {
    std::map<std::string, double, std::less<>> diskBlockSizes;

    std::map<std::string, DiskStats, std::less<>> allStats;
    if (!readDiskStats(allStats, "disk block size reading")) {
        return diskBlockSizes;
    }

    for (const auto& [device, ds] : allStats) {
        // dk_bsize = ((dk_rkb + dk_wkb) / dk_xfers) * 1024
        // where dk_rkb = sectors_read / 2, dk_wkb = sectors_written / 2
        // and dk_xfers = reads_completed + writes_completed

        unsigned long total_transfers = ds.reads_completed + ds.writes_completed;
        if (total_transfers > 0) {
            double read_kb = static_cast<double>(ds.sectors_read) / 2.0;
            double write_kb = static_cast<double>(ds.sectors_written) / 2.0;
            double total_kb = read_kb + write_kb;
            double block_size_kb = total_kb / static_cast<double>(total_transfers);
            diskBlockSizes[device] = block_size_kb;
        } else {
            diskBlockSizes[device] = 0.0;
        }
    }

    return diskBlockSizes;
}

std::map<std::string, double, std::less<>> StatisticsCollector::getDiskTransfersPerSecond() {
    std::map<std::string, double, std::less<>> diskTransferRates;

    struct timespec startTime;
    struct timespec endTime;

    // Record start time
    clock_gettime(CLOCK_MONOTONIC, &startTime);

    // First reading - store total transfers (dk_xfers = dk_reads + dk_writes) for each device
    std::map<std::string, DiskStats, std::less<>> firstReading;
    if (!readDiskStats(firstReading, "first reading")) {
        return diskTransferRates;
    }

    if (firstReading.empty()) {
        stat_logger.error("No valid disk devices found in first reading");
        return diskTransferRates;
    }

    // Sleep for a short interval to get meaningful difference
    struct timespec sleepTime = {1, 0};  // 1 second
    nanosleep(&sleepTime, nullptr);

    // Second reading
    std::map<std::string, DiskStats, std::less<>> secondReading;
    if (!readDiskStats(secondReading, "second reading")) {
        return diskTransferRates;
    }

    // Record end time
    clock_gettime(CLOCK_MONOTONIC, &endTime);

    // Calculate elapsed time in seconds
    double elapsedTime = calculateElapsedTime(startTime, endTime);

    if (elapsedTime <= 0.0) {
        stat_logger.error("Invalid elapsed time for disk transfers calculation");
        return diskTransferRates;
    }

    // Calculate transfers per second for each device
    for (const auto& [device, diskStats] : firstReading) {
        unsigned long firstTransfers = diskStats.reads_completed + diskStats.writes_completed;

        if (secondReading.find(device) != secondReading.end()) {
            unsigned long secondTransfers = secondReading[device].reads_completed +
                secondReading[device].writes_completed;

            // Calculate the delta (handling potential counter wraparound)
            unsigned long deltaTransfers;
            if (secondTransfers >= firstTransfers) {
                deltaTransfers = secondTransfers - firstTransfers;
            } else {
                // Counter wrapped around, assume it's a 64-bit counter
                deltaTransfers = (ULLONG_MAX - firstTransfers) + secondTransfers + 1;
            }

            // Calculate transfers per second: DKDELTA(dk_xfers) / elapsed
            double transfersPerSecond = static_cast<double>(deltaTransfers) / elapsedTime;

            diskTransferRates[device] = transfersPerSecond;
        }
    }

    return diskTransferRates;
}

void StatisticsCollector::logLoadAverage(std::string name) {
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
