#ifndef KAFKACONNECTOR_CLASS
#define KAFKACONNECTOR_CLASS

#include <cppkafka/cppkafka.h>

class KafkaConnector {
public:
  cppkafka::Consumer consumer;
  KafkaConnector(cppkafka::Configuration configs) : consumer(configs) {
    _configs = configs;
  };
  void Subscribe(std::string topic);

private:
  cppkafka::Configuration _configs;
};

#endif