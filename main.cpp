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

void fnExit3(void) {
    delete (server);
    puts("Shutting down the server.");
}

int main(int argc, char *argv[]) {
    atexit(fnExit3);

    if (argc <= 1) {
        main_logger.log(
            "\"Use argument 1 to start JasmineGraph in Master mode. Use 2 "
            "<hostName> <serverPort> <serverDataPort> to start as worker",
            "error");
        return -1;
    }
    std::cout << argc << std::endl;

    int mode = atoi(argv[2]);
    std::string JASMINEGRAPH_HOME = Utils::getJasmineGraphHome();
    jasminegraph_profile = strcmp(argv[1], "docker") == 0 ? PROFILE_DOCKER : PROFILE_K8S;
    std::string enableNmon = "false";

    main_logger.log("Using JASMINE_GRAPH_HOME", "info");
    std::cout << JASMINEGRAPH_HOME << std::endl;

    StatisticCollector::init();
    thread schedulerThread(SchedulerService::startScheduler);

    if (mode == Conts::JASMINEGRAPH_RUNTIME_PROFILE_MASTER) {
        std::string masterIp = argv[3];
        int numberOfWorkers = atoi(argv[4]);
        std::string workerIps = argv[5];
        enableNmon = argv[6];
        server = JasmineGraphServer::getInstance();

        if (jasminegraph_profile == PROFILE_K8S) {
            K8sInterface *interface = new K8sInterface();
            masterIp = interface->getMasterIp();
            if (masterIp.empty()) {
                masterIp = interface->createJasmineGraphMasterService()->spec->cluster_ip;
            }
        }
        server->run(masterIp, numberOfWorkers, workerIps, enableNmon);

        schedulerThread.join();
        delete server;
    } else if (mode == Conts::JASMINEGRAPH_RUNTIME_PROFILE_WORKER) {
        main_logger.log(to_string(argc), "info");

        if (argc < 8) {
            main_logger.info(
                "Need 7 arguments. Use <mode> 2 <hostName> <masterIP> <serverPort> <serverDataPort> <enable-nmon> to "
                "start as worker");
            return -1;
        }

        string hostName;
        hostName = argv[3];
        setenv("HOST_NAME", argv[3], 1);
        std::string masterHost = argv[4];
        int serverPort = atoi(argv[5]);
        setenv("PORT", argv[5], 1);
        int serverDataPort = atoi(argv[6]);
        enableNmon = argv[7];

        std::cout << "In worker mode" << std::endl;
        instance = new JasmineGraphInstance();
        instance->start_running(hostName, masterHost, serverPort, serverDataPort, enableNmon);

        delete instance;
    }

    return 0;
}
