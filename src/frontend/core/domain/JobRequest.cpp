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

#include "JobRequest.h"

std::string JobRequest::getJobId() {
    return jobId;
}

void JobRequest::setJobId(std::string inputJobId) {
    jobId = inputJobId;
}

std::string JobRequest::getJobType() {
    return jobType;
}

void JobRequest::setJobType(std::string inputJobType) {
    jobType = inputJobType;
}

void JobRequest::addParameter(std::string key, std::string value) {
    requestParams[key] = value;
}

std::string JobRequest::getParameter(std::string key) {
    return requestParams[key];
}

void JobRequest::setPriority(int priority) {
    this->priority = priority;
}

int JobRequest::getPriority() {
    return priority;
}

void JobRequest::setMasterIP(std::string masterip) {
    this->masterIP = masterip;
}

std::string JobRequest::getMasterIP() {
    return masterIP;
}
