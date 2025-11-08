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

#include "main.h"

#include <unistd.h>

#include <future>
#include <iostream>

#include "globals.h"
#include "src/k8s/K8sWorkerController.h"
#include "src/server/JasmineGraphInstance.h"
#include "src/util/logger/Logger.h"
#include "src/util/scheduler/SchedulerService.h"

unsigned int microseconds = 10000000;
JasmineGraphServer *server;
JasmineGraphInstance *instance;
SchedulerService schedulerService;
Logger main_logger;

#ifndef UNIT_TEST
int jasminegraph_profile = PROFILE_DOCKER;
#endif

enum args {
    PROFILE = 1,
    MODE = 2
};

enum master_mode_args {
    MASTER_IP = 3,
    NUMBER_OF_WORKERS = 4,
    WORKER_IPS = 5,
    ENABLE_NMON = 6
};

enum worker_mode_args {
    HOST_NAME = 3,
    MASTER_HOST = 4,
    SERVER_PORT = 5,
    SERVER_DATA_PORT = 6,
    WORKER_ENABLE_NMON = 7
};

void fnExit3(void) { delete (server); }

int main(int argc, char *argv[]) {
    atexit(fnExit3);

    if (argc <= 1) {
        main_logger.error(
            "\"Use argument 1 to start JasmineGraph in Master mode. Use 2 "
            "<hostName> <serverPort> <serverDataPort> to start as worker");
        return -1;
    }
    std::cout << argc << std::endl;

    int mode = atoi(argv[args::MODE]);
    std::string JASMINEGRAPH_HOME = Utils::getJasmineGraphHome();
    jasminegraph_profile = strcmp(argv[args::PROFILE], "docker") == 0 ? PROFILE_DOCKER : PROFILE_K8S;
    std::string enableNmon = "false";
    main_logger.info("Using JASMINE_GRAPH_HOME=" + JASMINEGRAPH_HOME);

    if (mode == Conts::JASMINEGRAPH_RUNTIME_PROFILE_WORKER){
        setenv("HOST_NAME", argv[worker_mode_args::HOST_NAME], 1);

    }else {
        setenv("HOST_NAME", argv[master_mode_args::MASTER_IP], 1);

    }
    setenv("HOST_NAME", argv[worker_mode_args::HOST_NAME], 1);

    StatisticCollector::init();
    thread schedulerThread(SchedulerService::startScheduler);

    if (mode == Conts::JASMINEGRAPH_RUNTIME_PROFILE_MASTER) {
        std::string masterIp = argv[master_mode_args::MASTER_IP];
        int numberOfWorkers = atoi(argv[master_mode_args::NUMBER_OF_WORKERS]);
        std::string workerIps = argv[master_mode_args::WORKER_IPS];
        enableNmon = argv[master_mode_args::ENABLE_NMON];
        server = JasmineGraphServer::getInstance();


        if (jasminegraph_profile == PROFILE_K8S) {
            std::unique_ptr<K8sInterface> k8sInterface(new K8sInterface());
            masterIp = k8sInterface->getMasterIp();
            if (masterIp.empty()) {
                masterIp = k8sInterface->createJasmineGraphMasterService()->spec->cluster_ip;
            }
        }
        server->run(masterIp, numberOfWorkers, workerIps, enableNmon);

        schedulerThread.join();
        delete server;
    } else if (mode == Conts::JASMINEGRAPH_RUNTIME_PROFILE_WORKER) {
        main_logger.info(to_string(argc));

        if (argc < 8) {
            main_logger.info(
                "Need 7 arguments. Use <mode> 2 <hostName> <masterIP> <serverPort> <serverDataPort> <enable-nmon> to "
                "start as worker");
            return -1;
        }

        string hostName;
        hostName = argv[worker_mode_args::HOST_NAME];
        setenv("HOST_NAME", argv[worker_mode_args::HOST_NAME], 1);
        std::string masterHost = argv[worker_mode_args::MASTER_HOST];
        int serverPort = atoi(argv[worker_mode_args::SERVER_PORT]);
        setenv("PORT", argv[worker_mode_args::SERVER_PORT], 1);
        int serverDataPort = atoi(argv[worker_mode_args::SERVER_DATA_PORT]);
        enableNmon = argv[worker_mode_args::WORKER_ENABLE_NMON];

        std::cout << "In worker mode" << std::endl;
        instance = new JasmineGraphInstance();
        instance->start_running(hostName, masterHost, serverPort, serverDataPort, enableNmon);

        delete instance;
    }

    return 0;
}
