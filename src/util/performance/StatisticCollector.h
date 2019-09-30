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

#ifndef JASMINEGRAPH_STATISTICCOLLECTOR_H
#define JASMINEGRAPH_STATISTICCOLLECTOR_H

#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <string.h>
#include <iostream>
#include "stdlib.h"
#include "stdio.h"
#include "string.h"
#include "sys/times.h"
#include "sys/vtimes.h"
#include <limits>
#include <fstream>
#include <thread>


class StatisticCollector {
public:
    int init();
    static int getVirtualMemoryUsage();
    static int parseLine(char* line);
    static double getCpuUsage();
    static std::string collectVMStatistics(std::string isVMStatManager);
    static long getTotalMemory();
    static int getTotalNumberofCores();
};


#endif //JASMINEGRAPH_STATISTICCOLLECTOR_H
