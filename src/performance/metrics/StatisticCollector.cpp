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

static int numProcessors;

static long parseLine(char *line);
static long getSwapSpace(const char *type);

int StatisticCollector::init() {
    FILE *file;
    struct tms timeSample;
    char line[128];

    file = fopen("/proc/cpuinfo", "r");
    if (!file) {
        std::cout << "Cannot open /proc/cpuinfo" << std::endl;
        exit(-1);
    }
    numProcessors = 0;
    while (fgets(line, 128, file) != NULL) {
        if (strncmp(line, "processor", 9) == 0) numProcessors++;
    }
    fclose(file);
    return 0;
}

long StatisticCollector::getMemoryUsageByProcess() {
    FILE *file = fopen("/proc/self/status", "r");
    long result = -1;
    char line[128];

    while (fgets(line, 128, file) != NULL) {
        if (strncmp(line, "VmSize:", 7) == 0) {
            result = parseLine(line);
            break;
        }
    }
    fclose(file);
    std::cout << "Memory Usage: " + std::to_string(result) << std::endl;
    return result;
}

int StatisticCollector::getThreadCount() {
    FILE *file = fopen("/proc/self/stat", "r");
    long result;
    char line[128];

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
    char line[128];

    fgets(line, 128, file);

    while (fgets(line, 128, file) != NULL) {
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
    std::cout << "Used swap space: " + std::to_string(result) << std::endl;
    return result;
}

long StatisticCollector::getTotalSwapSpace() {
    long result = getSwapSpace(3);
    std::cout << "Total swap space: " + std::to_string(result) << std::endl;
    return result;
}

long StatisticCollector::getRXBytes() {
    FILE *file = fopen("/sys/class/net/eth0/statistics/rx_bytes", "r");
    long result = -1;
    fscanf(file, "%li", &result);
    fclose(file);
    std::cout << "Total read bytes: " + std::to_string(result) << std::endl;
    return result;
}

long StatisticCollector::getTXBytes() {
    FILE *file = fopen("/sys/class/net/eth0/statistics/tx_bytes", "r");
    long result = -1;
    fscanf(file, "%li", &result);
    fclose(file);
    std::cout << "Total sent bytes: " + std::to_string(result) << std::endl;

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

    std::cout << "Total sockets: " + std::to_string(count) << std::endl;
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

double StatisticCollector::getCpuUsage() {
    static long long lastTotal = 0, lastIdle = 0;

    FILE *fp = fopen("/proc/stat", "r");
    if (!fp) return -1;
    char line[1024];
    fscanf(fp, "%[^\r\n]%*c", line);
    fclose(fp);
    std::cout << line << std::endl;
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
            std::cerr << "Value is " << value << " for line " << line << std::endl;
        }
        if (field == 4) {
            idle += value;
        }
        total += value;
    }
    long long diffTotal = lastTotal - total;
    long long diffIdle = lastIdle - idle;
    lastTotal = total;
    lastIdle = idle;

    return (diffTotal - diffIdle) / (double)diffTotal;
}

std::string StatisticCollector::collectVMStatistics(std::string isVMStatManager,
                                                    std::string isTotalAllocationRequired) {
    std::string vmLevelStatistics;

    if (isVMStatManager == "true") {
        long totalMemoryUsed = getTotalMemoryUsage();
        double totalCPUUsage = getTotalCpuUsage();

        std::stringstream stream;
        stream << std::fixed << std::setprecision(2) << totalCPUUsage;
        std::string cpuUsageString = stream.str();

        vmLevelStatistics = std::to_string(totalMemoryUsed) + "," + cpuUsageString + ",";
    }

    if (isTotalAllocationRequired == "true") {
        long totalMemory = getTotalMemoryAllocated();
        int totalCoresAvailable = getTotalNumberofCores();

        vmLevelStatistics = vmLevelStatistics + std::to_string(totalMemory) + "," + std::to_string(totalCoresAvailable);
    }

    return vmLevelStatistics;
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
