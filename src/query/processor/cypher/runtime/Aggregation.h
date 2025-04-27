//
// Created by kumarawansha on 3/31/25.
//

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
