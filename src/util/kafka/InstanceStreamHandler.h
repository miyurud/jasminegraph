#ifndef INSTANCESTREAMHANDLER_H
#define INSTANCESTREAMHANDLER_H

#include <mutex>
#include <thread>
#include <condition_variable>
#include <queue>
#include <map>
#include <string>
#include <atomic>
#include "../../localstore/incremental/JasmineGraphIncrementalLocalStore.h"

class InstanceStreamHandler {
 public:
    InstanceStreamHandler(std::map<std::string, JasmineGraphIncrementalLocalStore*>& incrementalLocalStoreMap);
    ~InstanceStreamHandler();

    void handleRequest(const std::string& nodeString);

 private:
    std::map<std::string, JasmineGraphIncrementalLocalStore*>& incrementalLocalStoreMap;
    std::map<std::string, std::thread> threads;
    std::map<std::string, std::queue<std::string>> queues;
    std::map<std::string, std::condition_variable> cond_vars;
    std::map<std::string, std::mutex> queue_mutexes;
    std::atomic<bool> terminateThreads{false};

        void threadFunction(const std::string& nodeString);
        static std::string extractGraphIdentifier(const std::string& nodeString);
        static JasmineGraphIncrementalLocalStore *loadStreamingStore(
                std::string graphId, std::string partitionId, std::map<std::string,
                JasmineGraphIncrementalLocalStore *> &graphDBMapStreamingStores);
};
#endif  // INSTANCESTREAMHANDLER_H
