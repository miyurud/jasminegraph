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

#ifndef JASMINEGRAPH_JOBREQUEST_H
#define JASMINEGRAPH_JOBREQUEST_H

#include <string>
#include <map>

class JobRequest {
private:
    std::string jobId;
    std::string jobType;
    std::map<std::string, std::string> requestParams;

public:
    int priority;

    std::string getJobId();
    void setJobId(std::string inputJobId);
    std::string getJobType();
    void setJobType(std::string inputJobType);
    void addParameter(std::string key, std::string value);
    std::string getParameter(std::string key);
    void setPriority(int priority);
    int getPriority();
};


#endif //JASMINEGRAPH_JOBREQUEST_H
