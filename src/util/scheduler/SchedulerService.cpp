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

#include "SchedulerService.h"
#include "Scheduler.h"

using namespace Bosma;

Logger schedulerservice_logger;


void SchedulerService::startScheduler() {

    Utils utils;

    std::string schedulerEnabled = utils.getJasmineGraphProperty("org.jasminegraph.scheduler.enabled");

    if (schedulerEnabled == "true") {

        schedulerservice_logger.log("#######SCHEDULER ENABLED#####","info");

        startPerformanceScheduler();
    }


}

void SchedulerService::startPerformanceScheduler() {
    unsigned int max_n_threads = 12;

    Utils utils;
    PerformanceUtil util;
    util.init();

    Bosma::Scheduler scheduler(max_n_threads);

    std::string performanceSchedulerTiming = utils.getJasmineGraphProperty("org.jasminegraph.scheduler.performancecollector.timing");

    scheduler.every(std::chrono::seconds(atoi(performanceSchedulerTiming.c_str())), util.collectPerformanceStatistics);

    std::this_thread::sleep_for(std::chrono::minutes(1440));
}