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

#include "JasmineGraphInstance.h"

#include "../util/Utils.h"
#include "../util/logger/Logger.h"
#include "../util/telemetry/TelemetryInitializer.h"
#include "../util/telemetry/OpenTelemetryUtil.h"

Logger graphInstance_logger;

void *runInstanceService(void *dummyPt) {
    JasmineGraphInstance *refToInstance = (JasmineGraphInstance *)dummyPt;
    refToInstance->instanceService = new JasmineGraphInstanceService();
    refToInstance->instanceService->run(refToInstance->masterHostName, refToInstance->hostName,
                                        refToInstance->serverPort, refToInstance->serverDataPort);
    return NULL;
}

void *runFileTransferService(void *dummyPt) {
    JasmineGraphInstance *refToInstance = (JasmineGraphInstance *)dummyPt;
    refToInstance->ftpService = new JasmineGraphInstanceFileTransferService();
    refToInstance->ftpService->run(refToInstance->serverDataPort);
    return NULL;
}

int JasmineGraphInstance::start_running(string hostName, string masterHost, int serverPort, int serverDataPort,
                                        string enableNmon) {
    graphInstance_logger.info("Worker started on port " + std::to_string(serverPort));
    
    // Initialize OpenTelemetry for this worker process with worker-specific service name
    try {
        std::string workerServiceName = "jasminegraph-worker-" + std::to_string(serverPort);
        OpenTelemetryUtil::initialize(
            workerServiceName,                   // Worker-specific service name (e.g., jasminegraph-worker-7780)
            "http://pushgateway:9091",          // Pushgateway for metrics
            "http://tempo:4318/v1/traces"       // Tempo OTLP HTTP endpoint for traces
        );
        graphInstance_logger.info("OpenTelemetry initialized successfully for worker " + workerServiceName);
    } catch (const std::exception& e) {
        graphInstance_logger.error("Failed to initialize OpenTelemetry for worker: " + std::string(e.what()));
        graphInstance_logger.info("Worker will continue without telemetry...");
    }

    this->hostName = hostName;
    this->masterHostName = masterHost;
    this->serverPort = serverPort;
    this->serverDataPort = serverDataPort;
    this->enableNmon = enableNmon;

    if (Utils::createDirectory(Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder"))) {
        graphInstance_logger.error("Could not create directory: " +
                                   Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder"));
    }
    if (Utils::createDirectory(Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.aggregatefolder"))) {
        graphInstance_logger.error("Could not create directory: " +
                                   Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.aggregatefolder"));
    }

    startNmonAnalyzer(enableNmon, serverPort);

    pthread_t instanceCommunicatorThread;
    pthread_t instanceFileTransferThread;
    pthread_create(&instanceCommunicatorThread, NULL, runInstanceService, this);
    pthread_create(&instanceFileTransferThread, NULL, runFileTransferService, this);

    std::thread *myThreads = new std::thread[1];
    myThreads[0] = std::thread(StatisticCollector::logLoadAverage, "worker");

    pthread_join(instanceCommunicatorThread, NULL);
    pthread_join(instanceFileTransferThread, NULL);
    
    // Shutdown OpenTelemetry gracefully
    try {
        OpenTelemetryUtil::shutdown();
        graphInstance_logger.info("OpenTelemetry shutdown completed for worker on port " + std::to_string(serverPort));
    } catch (const std::exception& e) {
        graphInstance_logger.error("Error during OpenTelemetry shutdown: " + std::string(e.what()));
    }
    
    return 0;
}

void JasmineGraphInstance::startNmonAnalyzer(string enableNmon, int serverPort) {
    if (enableNmon == "true") {
        std::string nmonFileLocation = Utils::getJasmineGraphProperty("org.jasminegraph.server.nmon.file.location");
        std::string numberOfSnapshots = Utils::getJasmineGraphProperty("org.jasminegraph.server.nmon.snapshots");
        std::string snapshotGap = Utils::getJasmineGraphProperty("org.jasminegraph.server.nmon.snapshot.gap");
        std::string nmonFileName = nmonFileLocation + "nmon.log." + std::to_string(serverPort);
        std::string nmonStartupCommand =
            "nmon -c " + numberOfSnapshots + " -s " + snapshotGap + " -T -F " + nmonFileName;

        char buffer[BUFFER_SIZE];
        std::string result = "";

        FILE *input = popen(nmonStartupCommand.c_str(), "r");

        if (input) {
            // read the input
            while (!feof(input)) {
                if (fgets(buffer, BUFFER_SIZE, input) != NULL) {
                    result.append(buffer);
                }
            }
            if (!result.empty()) {
                graphInstance_logger.error("Error in performance database backup process");
            }

            pclose(input);
        }
    }
}
