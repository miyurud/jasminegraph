/**
Copyright 2019 JasminGraph Team
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

#ifndef JASMINEGRAPH_JOBSCHEDULER_H
#define JASMINEGRAPH_JOBSCHEDULER_H

#include "../domain/JobRequest.h"
#include "../domain/JobResponse.h"
#include "../../../metadb/SQLiteDBInterface.h"
#include "../../../performancedb/PerformanceSQLiteDBInterface.h"
#include "../CoreConstants.h"
#include <thread>
#include <chrono>
#include <future>

class JobScheduler {
public:
    JobScheduler(SQLiteDBInterface sqlite, PerformanceSQLiteDBInterface perfDB);

    JobScheduler();

    void init();

    static void processJob(JobRequest request, SQLiteDBInterface sqlite, PerformanceSQLiteDBInterface perfDB);

    static void executeJob(JobRequest request, SQLiteDBInterface sqlite, PerformanceSQLiteDBInterface perfDB);

    void pushJob(JobRequest jobDetails);

    JobResponse getResult(JobRequest jobRequest);

    SQLiteDBInterface sqlite;
    PerformanceSQLiteDBInterface perfSqlite;
    static std::vector<std::future<void>> intermRes;
};

inline bool operator<(const JobRequest& lhs, const JobRequest& rhs)
{
    return lhs.priority < rhs.priority;
}


#endif //JASMINEGRAPH_JOBSCHEDULER_H
