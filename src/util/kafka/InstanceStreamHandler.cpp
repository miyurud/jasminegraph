#include "InstanceStreamHandler.h"
#include "../../localstore/incremental/JasmineGraphIncrementalLocalStore.h"
#include "../Utils.h"
#include "../logger/Logger.h"

Logger InstanceStreamHandlerLogger;
InstanceStreamHandler::InstanceStreamHandler(std::map<std::string,
                                             JasmineGraphIncrementalLocalStore*>& incrementalLocalStoreMap)
        : incrementalLocalStoreMap(incrementalLocalStoreMap) { }

InstanceStreamHandler::~InstanceStreamHandler() { }

void InstanceStreamHandler::handleRequest(const std::string& nodeString) {
    if (nodeString == "-1") {
        terminateThreads = true;

        for (auto& cv : cond_vars) {
            cv.second.notify_all();
        }

         for (auto& thread : threads) {
             if (thread.second.joinable()) {
                 thread.second.join();
             }
         }

        return;
    }
    std::string graphIdentifier = extractGraphIdentifier(nodeString);

    std::unique_lock<std::mutex> lock(queue_mutexes[graphIdentifier]);  // Use specific mutex for the queue
    if (threads.find(graphIdentifier) == threads.end()) {
        threads[graphIdentifier] = std::thread(&InstanceStreamHandler::threadFunction, this, nodeString);
        queues[graphIdentifier] = std::queue<std::string>();
    }

    queues[graphIdentifier].push(nodeString);
    cond_vars[graphIdentifier].notify_one();
    InstanceStreamHandlerLogger.info("Pushed into the Queue");
}

void InstanceStreamHandler::threadFunction(const std::string& nodeString) {
    std::string graphIdentifier = extractGraphIdentifier(nodeString);
    if (incrementalLocalStoreMap.find(graphIdentifier) == incrementalLocalStoreMap.end()) {
        auto graphIdPartitionId = JasmineGraphIncrementalLocalStore::getIDs(nodeString);
        std::string graphId = graphIdPartitionId.first;
        std::string partitionId = std::to_string(graphIdPartitionId.second);
        loadStreamingStore(graphId, partitionId, incrementalLocalStoreMap);
    }
    JasmineGraphIncrementalLocalStore* localStore = incrementalLocalStoreMap[graphIdentifier];
    InstanceStreamHandlerLogger.info("Thread Function");

    while (!terminateThreads) {
        std::string nodeString;
        {
            std::unique_lock<std::mutex> lock(queue_mutexes[graphIdentifier]);
            cond_vars[graphIdentifier].wait(lock, [&]{
                return !queues[graphIdentifier].empty() || terminateThreads;
            });

            if (terminateThreads) {
                break;
            }
            nodeString = queues[graphIdentifier].front();
            queues[graphIdentifier].pop();
        }
        localStore->addEdgeFromString(nodeString);
    }
}

std::string InstanceStreamHandler::extractGraphIdentifier(const std::string& nodeString) {
    auto graphIdPartitionId = JasmineGraphIncrementalLocalStore::getIDs(nodeString);
    std::string graphId = graphIdPartitionId.first;
    std::string partitionId = std::to_string(graphIdPartitionId.second);
    std::string graphIdentifier = graphId + "_" + partitionId;
    return graphIdentifier;
}

JasmineGraphIncrementalLocalStore *
InstanceStreamHandler::loadStreamingStore(std::string graphId, std::string partitionId,
                                          map<std::string, JasmineGraphIncrementalLocalStore *> &graphDBMapStreamingStores) {
    std::string graphIdentifier = graphId + "_" + partitionId;
    InstanceStreamHandlerLogger.log("###INSTANCE### Loading streaming Store for" + graphIdentifier
                                    + " : Started", "info");
    std::string folderLocation = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    auto *jasmineGraphStreamingLocalStore = new JasmineGraphIncrementalLocalStore(
                                     stoi(graphId), stoi(partitionId));
    graphDBMapStreamingStores.insert(std::make_pair(graphIdentifier, jasmineGraphStreamingLocalStore));
    InstanceStreamHandlerLogger.log("###INSTANCE### Loading Local Store : Completed", "info");
    return jasmineGraphStreamingLocalStore;
}
