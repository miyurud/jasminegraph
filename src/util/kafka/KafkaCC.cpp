#include "./KafkaCC.h"
#include <cppkafka/cppkafka.h>

using namespace std;
using namespace cppkafka;

void KafkaConnector::Subscribe(string topic) { consumer.subscribe({topic}); }