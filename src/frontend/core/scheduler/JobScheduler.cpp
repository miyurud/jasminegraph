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
            jobScheduler_Logger.log("##JOB SCHEDULER## Jobs Available for Scheduling", "info");

            std::vector<JobRequest> pendingHPJobList;
            std::vector<std::string> highPriorityGraphList;

            while (!jobQueue.empty()) {
                JobRequest request = jobQueue.top();

                if (request.getPriority() == Conts::HIGH_PRIORITY_DEFAULT_VALUE && request.getJobType() == TRIANGLES) {
                    pendingHPJobList.push_back(request);
                    highPriorityGraphList.push_back(request.getParameter(Conts::PARAM_KEYS::GRAPH_ID));
                    jobQueue.pop();
                } else {
                    jobQueue.pop();
                    JobScheduler::processJob(request, refToScheduler->sqlite, refToScheduler->perfSqlite);
                }
            }


            if (pendingHPJobList.size() > 0) {
                jobScheduler_Logger.log("##JOB SCHEDULER## High Priority Jobs in Queue: " + std::to_string(pendingHPJobList.size()), "info");
                std::string masterIP = pendingHPJobList[0].getMasterIP();
                std::string jobType = pendingHPJobList[0].getJobType();
                std::string category = pendingHPJobList[0].getParameter(Conts::PARAM_KEYS::CATEGORY);

                std::vector<long> scheduleTimeVector = performanceUtil.getResourceAvailableTime(highPriorityGraphList,
                        jobType, category, masterIP, pendingHPJobList);

                for (int index = 0; index != pendingHPJobList.size(); ++index) {
                    JobRequest hpRequest = pendingHPJobList[index];
                    long queueTime = scheduleTimeVector[index];

                    if (queueTime  < 0) {
                        JobResponse failedJobResponse;
                        failedJobResponse.setJobId(hpRequest.getJobId());
                        failedJobResponse.addParameter(Conts::PARAM_KEYS::ERROR_MESSAGE, "Rejecting the job request because "
                                                                                         "SLA cannot be maintained");
                        responseVectorMutex.lock();
                        responseMap[hpRequest.getJobId()] = failedJobResponse;
                        responseVectorMutex.unlock();
                        continue;
                    }

                    hpRequest.addParameter(Conts::PARAM_KEYS::QUEUE_TIME, std::to_string(queueTime));
                    JobScheduler::processJob(hpRequest, refToScheduler->sqlite, refToScheduler->perfSqlite);
                }
            }
        } else {
            std::this_thread::sleep_for(std::chrono::seconds(2));
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
        responseVectorMutex.lock();
        if (responseMap.find(jobRequest.getJobId()) != responseMap.end()) {
            jobResponse = responseMap[jobRequest.getJobId()];
            responseFound = true;
        }
        responseVectorMutex.unlock();
    }

    return jobResponse;
}