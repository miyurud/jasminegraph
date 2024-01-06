#ifndef KAFKACONNECTOR_CLASS
#define KAFKACONNECTOR_CLASS

#include <cppkafka/cppkafka.h>

#include "../../nativestore/DataPublisher.h"

class KafkaConnector {
 public:
    cppkafka::Consumer consumer;
    KafkaConnector(cppkafka::Configuration configs) : consumer(configs) { _configs = configs; };
    void Subscribe(std::string topic);
    void Unsubscribe();

 private:
    cppkafka::Configuration _configs;
};

#endif
