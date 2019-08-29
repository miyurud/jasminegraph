//
// Created by chinthaka on 8/24/19.
//

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
