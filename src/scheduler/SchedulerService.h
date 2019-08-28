//
// Created by chinthaka on 8/24/19.
//

#ifndef JASMINEGRAPH_SCHEDULERSERVICE_H
#define JASMINEGRAPH_SCHEDULERSERVICE_H

#include "../util/performance/PerformanceUtil.h"
#include "../util/Utils.h"
#include "../util/logger/Logger.h"
#include "../performancedb/PerformanceSQLiteDBInterface.h"
#include <thread>


class SchedulerService {
public:
    static void startScheduler();
    static void startPerformanceScheduler();

    static SQLiteDBInterface sqlite;
    static PerformanceSQLiteDBInterface perfSqlite;
};


#endif //JASMINEGRAPH_SCHEDULERSERVICE_H
