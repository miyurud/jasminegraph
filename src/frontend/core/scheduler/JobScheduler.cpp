/**
Copyright 2021 JasmineGraph Team
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

#include "JobScheduler.h"
#include "../../../util/logger/Logger.h"
#include "../../../util/Conts.h"
#include "../factory/ExecutorFactory.h"
#include "../executor/AbstractExecutor.h"

Logger jobScheduler_Logger;
std::priority_queue<JobRequest> jobQueue;
std::vector<JobResponse> responseVector;
std::map<std::string, JobResponse> responseMap;
std::vector<std::future<void>> JobScheduler::intermRes;
bool workerResponded;
std::vector<std::string> highPriorityGraphList;

JobScheduler::JobScheduler(SQLiteDBInterface sqlite, PerformanceSQLiteDBInterface perfDB) {
    this->sqlite = sqlite;
    this->perfSqlite = perfDB;
}

JobScheduler::JobScheduler() {

}

void *startScheduler(void *dummyPt) {
    JobScheduler *refToScheduler = (JobScheduler *) dummyPt;
    PerformanceUtil performanceUtil;
    performanceUtil.init();
    while (true) {
        if (jobQueue.size() > 0) {
            jobScheduler_Logger.log("##JOB SCHEDULER## Picked up Job", "info");
            JobRequest request = jobQueue.top();
            int priority = request.priority;
            std::string masterIP = request.getMasterIP();

            if (priority == Conts::HIGH_PRIORITY_DEFAULT_VALUE) {
                string perfCategory = request.getParameter(Conts::PARAM_KEYS::CATEGORY);
                string command = request.getJobType();
                string graphId = request.getParameter(Conts::PARAM_KEYS::GRAPH_ID);
                bool isResourcesSufficient = performanceUtil.isResourcesSufficient(graphId,command,perfCategory,masterIP);

                if (!isResourcesSufficient) {
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                }
            }

            jobQueue.pop();
            JobScheduler::processJob(request, refToScheduler->sqlite, refToScheduler->perfSqlite);
            jobScheduler_Logger.log("##JOB SCHEDULER## Scheduled the job", "info");
        } else {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }
}

void JobScheduler::init() {
    pthread_t schedulerThread;
    pthread_create(&schedulerThread, NULL, startScheduler, this);
}

void JobScheduler::processJob(JobRequest request, SQLiteDBInterface sqlite, PerformanceSQLiteDBInterface perfDB) {
    intermRes.push_back(
            std::async(std::launch::async, JobScheduler::executeJob, request, sqlite, perfDB));
}

void JobScheduler::executeJob(JobRequest request, SQLiteDBInterface sqlite, PerformanceSQLiteDBInterface perfDB) {
    ExecutorFactory *executorFactory = new ExecutorFactory(sqlite, perfDB);
    AbstractExecutor *abstractExecutor = executorFactory->getExecutor(request);
    abstractExecutor->execute();
}

void JobScheduler::pushJob(JobRequest jobDetails) {
    jobQueue.push(jobDetails);
}

JobResponse JobScheduler::getResult(JobRequest jobRequest) {
    JobResponse jobResponse;
    bool responseFound = false;

    while (!responseFound) {
        if (responseMap.find(jobRequest.getJobId()) != responseMap.end()) {
            jobResponse = responseMap[jobRequest.getJobId()];
            responseFound = true;
        }
    }

    return jobResponse;
}