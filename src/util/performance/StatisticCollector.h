//
// Created by chinthaka on 8/27/19.
//

#ifndef JASMINEGRAPH_STATISTICCOLLECTOR_H
#define JASMINEGRAPH_STATISTICCOLLECTOR_H

#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <string.h>
#include <iostream>


class StatisticCollector {
public:
    static int getVirtualMemoryUsage();
    static int parseLine(char* line);
};


#endif //JASMINEGRAPH_STATISTICCOLLECTOR_H
