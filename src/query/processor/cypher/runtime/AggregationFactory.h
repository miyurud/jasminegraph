//
// Created by kumarawansha on 3/31/25.
//

#ifndef JASMINEGRAPH_AGGREGATIONFACTORY_H
#define JASMINEGRAPH_AGGREGATIONFACTORY_H


#include "Aggregation.h"

class AggregationFactory {
 public:
    static const string AVERAGE;
    static Aggregation* getAggregationMethod(string type);
};


#endif //JASMINEGRAPH_AGGREGATIONFACTORY_H
