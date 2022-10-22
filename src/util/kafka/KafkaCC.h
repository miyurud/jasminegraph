#ifndef KAFKACONNECTOR_CLASS
#define KAFKACONNECTOR_CLASS

#include <cppkafka/cppkafka.h>
#include "../../centralstore/incremental/DataPublisher.h"

class KafkaConnector {
public:
  cppkafka::Consumer consumer;
  KafkaConnector(cppkafka::Configuration configs) : consumer(configs) {_configs = configs;};
  void Subscribe(std::string topic);
  void static *startStream(std::string topicName, std::vector<DataPublisher*> workerClients, std::map<std::string, std::atomic<bool>> *streamsState);

private:
  cppkafka::Configuration _configs;
};

#endif