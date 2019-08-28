//
// Created by chinthaka on 8/24/19.
//

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

    PerformanceUtil util;

    Bosma::Scheduler s(max_n_threads);

    s.every(std::chrono::seconds(5), util.collectPerformanceStatistics);

    std::this_thread::sleep_for(std::chrono::minutes(10));
}