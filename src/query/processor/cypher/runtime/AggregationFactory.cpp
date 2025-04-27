//
// Created by kumarawansha on 3/31/25.
//

#include "AggregationFactory.h"

const string AggregationFactory::AVERAGE = "Average";
const string AggregationFactory::DESC = "DESC";
const string AggregationFactory::ASC = "ASC";

Aggregation* AggregationFactory::getAggregationMethod(std::string type) {
    if (type == AVERAGE) {
        return new AverageAggregation();
    }else if (type == ASC)
    {
        return new AscAggregation();
    }


    return nullptr;
}