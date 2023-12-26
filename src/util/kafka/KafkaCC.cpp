#include "./KafkaCC.h"
#include <nlohmann/json.hpp>
#include "../logger/Logger.h"

using json = nlohmann::json;

using namespace std;
using namespace cppkafka;

void KafkaConnector::Subscribe(string topic) { consumer.subscribe({topic}); }
void KafkaConnector::Unsubscribe() { consumer.unsubscribe(); }