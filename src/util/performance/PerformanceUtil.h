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
#include "../Utils.h"
#include "../Conts.h"
#include "../../metadb/SQLiteDBInterface.h"
#include "../../performancedb/PerformanceSQLiteDBInterface.h"
#include "../PlacesToNodeMapper.h"
#include "../../server/JasmineGraphInstanceProtocol.h"
#include "../logger/Logger.h"
#include <thread>
#include <pthread.h>
#include <future>


#ifndef JASMINEGRAPH_PERFORMANCEUTIL_H
#define JASMINEGRAPH_PERFORMANCEUTIL_H


class PerformanceUtil {
public:
    //PerformanceUtil(SQLiteDBInterface sqlLiteDB, PerformanceSQLiteDBInterface perfDb);
    int init();
    static int collectPerformanceStatistics();


private:
    //static SQLiteDBInterface sqlLiteDB;
    //static PerformanceSQLiteDBInterface perfDb;
    static int requestPerformanceData(std::string host, int port, std::string isVMStatManager);
};


#endif //JASMINEGRAPH_PERFORMANCEUTIL_H
