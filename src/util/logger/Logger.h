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

#ifndef JASMINEGRAPH_SPDLOGGER_H
#define JASMINEGRAPH_SPDLOGGER_H

#include <string>

class Logger {
public:
    void log(std::string message, const std::string log_type);
    void info(std::string message){log(message,"info");};
    void warn(std::string message){log(message,"warn");};
    void debug(std::string message){log(message,"debug");};
    void error(std::string message){log(message,"error");};

};


#endif //JASMINEGRAPH_SPDLOGGER_H
