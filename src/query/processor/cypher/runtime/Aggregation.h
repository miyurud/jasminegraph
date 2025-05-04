/**
Copyright 2025 JasmineGraph Team
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

#ifndef JASMINEGRAPH_AGGREGATION_H
#define JASMINEGRAPH_AGGREGATION_H
#include <iostream>
#include <string>
#include <unistd.h>

#include "../util/SharedBuffer.h"
using namespace std;

class Aggregation {
 public:
    virtual void getResult(int connFd) = 0;
    virtual void insert(string data) = 0;
};

class AverageAggregation : public Aggregation {
 public:
    string data;
    int numberOfData = 0;
    float average = 0.0f;
    AverageAggregation()= default;
    void getResult(int connFd) override;
    void insert(string data) override;
};

class AscAggregation : public Aggregation {
private:
    SharedBuffer resultBuffer;
public:
    explicit AscAggregation(size_t bufferSize = 1000) : resultBuffer(bufferSize) {};
    void getResult(int connFd) override;
    void insert(string data) override;
};


#endif //JASMINEGRAPH_AGGREGATION_H
