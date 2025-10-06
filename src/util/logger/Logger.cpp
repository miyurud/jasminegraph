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

#include "Logger.h"

#include <pthread.h>
#include <spdlog/sinks/daily_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>

#include <iostream>

using namespace std;

static string get_worker_name();

auto logger = spdlog::stdout_color_mt("logger");
auto daily_logger = spdlog::daily_logger_mt("JasmineGraph", "logs/server_logs.log", 00, 01);
string worker_name = get_worker_name();

static string get_worker_name() {
    char *worker_id = getenv("WORKER_ID");
    if (worker_id) {
        return string("WORKER ") + string(worker_id);
    }
    return string("MASTER");
}

void Logger::log(std::string message, const std::string log_type) {
    pthread_t tid = pthread_self();

    struct timeval tv;
    gettimeofday(&tv, NULL);
    long millis = tv.tv_sec * 1000 + tv.tv_usec / 1000;
    // TODO: This temporarily fixes spdlog hanging after forking. This will prevent using the actual logger and simulate
    // the behavior using cout instead. But it will not write logs to the log file.
    // if (log_type.compare("debug") != 0) {
        cout << " [" << millis << "] [" << log_type << "] [" << worker_name << " : " << getpid() << ":" << tid << "] "
             << message << endl;
    // }
    return;

    if (log_type.compare("info") == 0) {
        daily_logger->info(message);
        logger->info(message);
    } else if (log_type.compare("warn") == 0) {
        daily_logger->warn(message);
        logger->warn(message);
    } else if (log_type.compare("trace") == 0) {
        daily_logger->trace(message);
        logger->trace(message);
    } else if (log_type.compare("error") == 0) {
        daily_logger->error(message);
        logger->error(message);
    } else if (log_type.compare("debug") == 0) {
        daily_logger->debug(message);
        logger->debug(message);
    }
    spdlog::flush_every(std::chrono::seconds(5));
}
