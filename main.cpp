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


#include <iostream>
#include <unistd.h>
#include "main.h"
#include "src/util/Conts.h"
#include "src/server/JasmineGraphInstance.h"
#include "src/util/logger/Logger.h"

unsigned int microseconds = 10000000;
JasmineGraphServer *server;
JasmineGraphInstance *instance;
Logger main_logger;

void fnExit3(void) {
    delete (server);
    puts("Shutting down the server.");
}


int main(int argc, char *argv[]) {
    atexit(fnExit3);
    Utils utils;

    if (argc < 1) {
    main_logger.log("\"Use argument 1 to start JasmineGraph in Master mode. Use 2 "
                    "<hostName> <serverPort> <serverDataPort> to start as worker","error");
        return -1;
    }


    int mode = atoi(argv[1]);
    std::string JASMINEGRAPH_HOME = utils.getJasmineGraphHome();

    std::cout << "Using JASMINE_GRAPH_HOME" << std::endl;
    std::cout << JASMINEGRAPH_HOME << std::endl;

    if (mode == Conts::JASMINEGRAPH_RUNTIME_PROFILE_MASTER) {
        server = new JasmineGraphServer();
        server->run();

        while (server->isRunning()) {
            usleep(microseconds);
        }

        delete server;
    } else if (mode == Conts::JASMINEGRAPH_RUNTIME_PROFILE_WORKER){
        main_logger.log(to_string(argc),"info");
        main_logger.log((argv[0]),"info");
        main_logger.log((argv[1]),"info");
        main_logger.log((argv[2]),"info");
        main_logger.log((argv[3]),"info");
        main_logger.log((argv[4]),"info");
        if (argc < 5) {
//            std::cout << "Need three arguments. Use 2 <serverPort> <serverDataPort> to "
//                      << "start as worker ." << std::endl;
        main_logger.log("Need Four arguments. Use 2 <hostName> <serverPort> <serverDataPort> to start as worker","info");
            return -1;
        }
        string hostName;
        hostName = argv[2];
        int serverPort = atoi(argv[3]);
        int serverDataPort = atoi(argv[4]);

        std::cout << "In worker mode" << std::endl;
        instance = new JasmineGraphInstance();
        instance->start_running(hostName, serverPort,serverDataPort);

        while (instance->isRunning()) {
            usleep(microseconds);
        }

        delete instance;
    }

    return 0;
}
