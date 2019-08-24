//
// Created by chinthaka on 8/24/19.
//

#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <string.h>
#include <iostream>

#ifndef JASMINEGRAPH_PERFORMANCEUTIL_H
#define JASMINEGRAPH_PERFORMANCEUTIL_H


class PerformanceUtil {
public:
    static int reportPerformanceStatistics();
    static int getVirtualMemoryUsage();
    static int parseLine(char* line);
};


#endif //JASMINEGRAPH_PERFORMANCEUTIL_H
