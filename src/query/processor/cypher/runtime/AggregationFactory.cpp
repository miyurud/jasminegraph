//
// Created by kumarawansha on 3/31/25.
//

#include "AggregationFactory.h"

const string AggregationFactory::AVERAGE = "Average";

Aggregation* AggregationFactory::getAggregationMethod(std::string type) {
    if (type == AVERAGE) {
        return new AverageAggregation();
    }
    return nullptr;
}