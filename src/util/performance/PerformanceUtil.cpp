//
// Created by chinthaka on 8/24/19.
//

#include "PerformanceUtil.h"

int PerformanceUtil::reportPerformanceStatistics() {
    int memoryUsage = getVirtualMemoryUsage();

    return 0;
}

int PerformanceUtil::getVirtualMemoryUsage() {
    FILE* file = fopen("/proc/self/status", "r");
    int result = -1;
    char line[128];

    while (fgets(line, 128, file) != NULL){
        if (strncmp(line, "VmSize:", 7) == 0){
            result = parseLine(line);
            break;
        }
    }
    fclose(file);
    std::cout << "Memory Usage: " + std::to_string(result) << std::endl;
    return result;


}

int PerformanceUtil::parseLine(char* line){
    int i = strlen(line);
    const char* p = line;
    while (*p <'0' || *p > '9') p++;
    line[i-3] = '\0';
    i = atoi(p);
    return i;
}