//
// Created by sajeenthiran on 2025-08-18.
//

#include "SemanticBeamSearch.h"

#include <iostream>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "../../../nativestore/NodeManager.h"
#include "../../../server/JasmineGraphInstanceProtocol.h"
#include "../../../server/JasmineGraphServer.h"
#include "../../../vectorStore/FaissStore.h"
#include "../cypher/runtime/Helpers.h"

Logger semantic_beam_search_logger;

SemanticBeamSearch::SemanticBeamSearch( FaissStore* faissStore,
                                        std::vector<float> emb, int k ,GraphConfig gc , vector<JasmineGraphServer::worker> workerList)
    : faissStore(faissStore), emb(std::move(emb)), k(k) , gc(gc), workerList(workerList)
{
    // Constructor implementation
    this->nodeManager = new NodeManager(gc);
    semantic_beam_search_logger.info("SemanticBeamSearch initialized with k: " + std::to_string(k) +
                                     ", embedding size: " + std::to_string(emb.size())
                                     + ", graph ID: " + std::to_string(gc.graphID) +
                                     ", partition ID: " + std::to_string(gc.partitionID));

}




std::vector<ScoredPath> SemanticBeamSearch::getSeedNodes()
{
    std::cout<<"getSeedNodes"<<std::endl;

    // std::cout<< "Searching for top "  << " nodes using FAISS...\n";
    std::cout<<"28"<<std::endl;
    // check the emb
    std::vector<ScoredPath> paths;

    try
    {
        auto results = faissStore->search(emb, 5);
        std::cout << "Top " << results.size() << " nodes found:\n";
        for (auto& [id, dist] : results) {
        std::cout << "ID: " << id << ", Distance: " << dist << "\n";
        // NodeBlock* node = nodeManager.get( std::to_string(id));


            NodeBlock* seedNode = nodeManager->get( faissStore->getNodeIdFromEmbeddingId((id)));
            if (!seedNode) continue;

            json initialPath;
            initialPath["pathNodes"] = json::array();
            json nodeData;
            nodeData["partitionID"] = std::string(seedNode->getMetaPropertyHead()->value);
            auto properties = seedNode->getAllProperties();
            for (const auto& [key, value] : properties) {
                nodeData[key] = value;
            }
            nodeData["id"] = std::to_string(seedNode->nodeId);
            initialPath["pathNodes"].push_back(nodeData);
            initialPath["pathRels"] = json::array();

            paths.push_back({initialPath, dist});
            break;
        ////
        // std::string value(node->getMetaPropertyHead()->value);
        //     std::cout<< value;
        //     if (value == to_string(gc.partitionID)) {
        // nodeData["partitionID"] = value;
        // std::map<std::string, char*> properties = node->getAllProperties();
        //         for (auto property : properties) {
        //             nodeData[property.first] = property.second;
        //         }
        //         for (auto& [key, value] : properties) {
        //             delete[] value;  // Free each allocated char* array
        //         }
        //         properties.clear();
        //
        //         std::cout << "Node ID: " << node->id << ", Properties: " << nodeData.dump() << "\n";
        //
        //     }

        }
    } catch ( std::exception& e )
    {
        std::cout << e.what() << "\n";
    }

    return  paths;
}

void SemanticBeamSearch::semanticMultiHopBeamSearch(SharedBuffer &buffer,
                                                     int numHops,
                                                     int beamWidth) {

    semantic_beam_search_logger.info("Starting semanticMultiHopBeamSearch with numHops: " + std::to_string(numHops) + ", beamWidth: " + std::to_string(beamWidth));

    // 1. Get seed nodes using FAISS
    std::vector<ScoredPath> paths = getSeedNodes();
    semantic_beam_search_logger.info("Seed nodes retrieved: " + std::to_string(paths.size()));

    // Debug: Print all seed paths
    for (size_t i = 0; i < paths.size(); ++i) {
        semantic_beam_search_logger.debug("Seed path " + std::to_string(i) + ": " + paths[i].pathObj.dump() + ", score: " + std::to_string(paths[i].score));
    }

    // Initialize paths

    // 2. Multi-hop beam search
    for (int hop = 1; hop <= numHops; ++hop) {
        semantic_beam_search_logger.info("Hop " + std::to_string(hop) + " started. Current paths: " + std::to_string(paths.size()));
        std::vector<ScoredPath> expandedPaths;
        std::unordered_map<std::string, json> remoteFrontier;


        for (size_t spIdx = 0; spIdx < paths.size(); ++spIdx) {
            auto &sp = paths[spIdx];
           semantic_beam_search_logger.debug("Expanding path index " + std::to_string(spIdx) + ": " + sp.pathObj.dump());
           auto& currentPath = sp.pathObj;
           semantic_beam_search_logger.debug("Current path object: " + currentPath.dump());
           json lastNodeJson = currentPath["pathNodes"].back();
           semantic_beam_search_logger.debug("Last node JSON: " + lastNodeJson.dump());
           string lastNodeId = lastNodeJson["id"].get<std::string>();
           semantic_beam_search_logger.debug("Last node ID: " + lastNodeId);
           std::string destPartitionId = lastNodeJson["partitionID"].get<std::string>();
           semantic_beam_search_logger.debug("Destination partition ID: " + destPartitionId);
            if (destPartitionId != std::to_string(gc.partitionID)) {
                semantic_beam_search_logger.info(
                    "Queueing remote node " + lastNodeId+
                    " for partition " + destPartitionId);

                remoteFrontier[destPartitionId].push_back(currentPath);

                // relation = relation->nextCentralDestination();
                continue; // skip local expansion
            }
            NodeBlock* lastNode = nodeManager->get(lastNodeId);

            semantic_beam_search_logger.debug("130");

            if (!lastNode) {
                semantic_beam_search_logger.info("Last node not found for path, skipping.");
                continue;
            }
            semantic_beam_search_logger.debug("Last node ID: " + std::to_string(lastNode->nodeId));

            // Expand local + central relations
            auto expandRelations = [&](RelationBlock* relation) {
                int relCount = 0;
                while (relation) {
                    relCount++;
                    semantic_beam_search_logger.debug("Expanding relation #" + std::to_string(relCount));
                    NodeBlock* destNode = relation->getDestination();
                    if (!destNode) {
                        semantic_beam_search_logger.info("Destination node not found for relation, skipping.");
                        relation = relation->nextLocalDestination(); // or nextCentralDestination
                        continue;
                    }
                    semantic_beam_search_logger.debug("Destination node ID: " + std::to_string(destNode->nodeId));

                    // Create new path
                    json newPath = currentPath;
                    json relData;
                    auto relProps = relation->getAllProperties();
                    for (auto& [k, v] : relProps) relData[k] = v;
                    newPath["pathRels"].push_back(relData);

                    semantic_beam_search_logger.debug("Relation properties: " + relData.dump());

                    json nodeData;
                    semantic_beam_search_logger.debug("Destination 168 node ID: " + std::to_string(destNode->nodeId));

                    auto nodeProps = destNode->getAllProperties();
                    nodeData["partitionID"] = std::string(destNode->getMetaPropertyHead()->value);
                    for (auto& [k, v] : nodeProps) nodeData[k] = v;
                    nodeData["id"] = std::to_string(destNode->nodeId);

                    newPath["pathNodes"].push_back(nodeData);

                    semantic_beam_search_logger.debug("Node properties: " + nodeData.dump());

                    // Compute cumulative score
                    float score = 0.0f;
                    for (auto& nodeJson : newPath["pathNodes"]) {
                        NodeBlock* n = nodeManager->get(nodeJson["id"].get<std::string>());
                        if (!n) {
                            semantic_beam_search_logger.debug("Node not found for scoring: " + nodeJson.dump());
                            continue;
                        }
                        vector<float> emb_ = faissStore->getEmbeddingById(std::to_string(n->nodeId));
                        semantic_beam_search_logger.debug("Scoring node ID: " + std::to_string(n->nodeId));
                        score += Utils::cosineSimilarity(emb, emb_);
                    }
                    // Relation contributions
                    // for (auto& relJson : newPath["pathRels"]) {
                    //     RelationBlock* r = relation; // if you can map relJson to RelationBlock
                    //     if (r) score += cosineSimilarity(emb, r->getEmbeddingVector()); // assumes RelationBlock has getEmbeddingVector()
                    // }
                    expandedPaths.push_back({newPath, score});

                    semantic_beam_search_logger.info("Expanded path to node " + std::to_string(destNode->nodeId) + " with score " + std::to_string(score));
                    semantic_beam_search_logger.debug("Expanded path JSON: " + newPath.dump());

                    relation = relation->nextLocalSource(); // or nextCentralDestination
                }
                semantic_beam_search_logger.debug("Total relations expanded: " + std::to_string(relCount));
            };

            RelationBlock* localRel = RelationBlock::getLocalRelation(lastNode->edgeRef);
            if (localRel) {
                semantic_beam_search_logger.info("Expanding local relations for node " + std::to_string(lastNode->nodeId));
                expandRelations(localRel);
            } else {
                semantic_beam_search_logger.debug("No local relations for node " + std::to_string(lastNode->nodeId));
            }

            RelationBlock* centralRel = RelationBlock::getCentralRelation(lastNode->centralEdgeRef);
            if (centralRel) {
                semantic_beam_search_logger.info("Expanding central relations for node " + std::to_string(lastNode->nodeId));
                expandRelations(centralRel);
            } else {
                semantic_beam_search_logger.debug("No central relations for node " + std::to_string(lastNode->nodeId));
            }
        }



        std::vector<std::thread> expansionThreads;
        for (auto& [partitionId, currentPaths] : remoteFrontier) {
            expansionThreads.emplace_back([&, partitionId, currentPaths]() {
                callRemoteExpansion(stoi(partitionId), currentPaths,  expandedPaths);
            });
        }

        // Join all threads
        for (auto& t : expansionThreads) {
            if (t.joinable()) t.join();
        }

        // block this thread until all threads are done
        semantic_beam_search_logger.info("All remote expansion threads joined.");
        semantic_beam_search_logger.info("Waiting for all remote expansion threads to complete.");


        //check all threads completed

        // Keep top beamWidth paths
        semantic_beam_search_logger.debug("Sorting expanded paths by score.");
        std::sort(expandedPaths.begin(), expandedPaths.end(),
                  [](const ScoredPath &a, const ScoredPath &b) { return a.score > b.score; });
        if ((int)expandedPaths.size() > beamWidth)
            expandedPaths.resize(beamWidth);

        semantic_beam_search_logger.info("Hop " + std::to_string(hop) + " finished. Expanded paths: " + std::to_string(expandedPaths.size()));
        for (size_t i = 0; i < expandedPaths.size(); ++i) {
            semantic_beam_search_logger.debug("Expanded path " + std::to_string(i) + ": " + expandedPaths[i].pathObj.dump() + ", score: " + std::to_string(expandedPaths[i].score));
        }

        paths = expandedPaths;
    }

    // 3. Add final paths to buffer
    semantic_beam_search_logger.info("Adding final paths to buffer. Total: " + std::to_string(paths.size()));
    for (size_t i = 0; i < paths.size(); ++i) {
        semantic_beam_search_logger.debug("272 Buffering path " + std::to_string(i) + ": " + paths[i].pathObj.dump());
        buffer.add(paths[i].pathObj.dump());
    }
    semantic_beam_search_logger.info("semanticMultiHopBeamSearch completed.");
}

json SemanticBeamSearch::callRemoteExpansion(
    int partitionId,
    const std::vector<json>& currentPaths,
    std::vector<ScoredPath>& expandedPaths) {

    semantic_beam_search_logger.info(
        "Starting remote expansion for partition " + std::to_string(partitionId));

    // check worklist

    for (const auto& worker : workerList) {
        semantic_beam_search_logger.debug("Worker: " + worker.hostname + ":" + std::to_string(worker.port));
    }

    std::string host = this->workerList[partitionId].hostname;
    int port = workerList[partitionId].port;
    char data[FED_DATA_LENGTH + 1];
    struct sockaddr_in serv_addr;
    struct hostent* server;

    semantic_beam_search_logger.info("Preparing to create socket for host: " + host + ", port: " + std::to_string(port));
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        semantic_beam_search_logger.error("Cannot create socket for remote expansion");
        return json();
    }

    if (host.find('@') != std::string::npos) {
        semantic_beam_search_logger.info("Host contains '@', splitting...");
        host = Utils::split(host, '@')[1];
        semantic_beam_search_logger.info("Host after split: " + host);
    }

    semantic_beam_search_logger.info("Resolving host: " + host);
    server = gethostbyname(host.c_str());
    if (!server) {
        semantic_beam_search_logger.error("ERROR, no host named " + host);
        return json();
    }

    semantic_beam_search_logger.info("Setting up server address struct");
    bzero((char*)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char*)server->h_addr, (char*)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);

    semantic_beam_search_logger.info("Attempting to connect to " + host + ":" + std::to_string(port));
    if (Utils::connect_wrapper(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        semantic_beam_search_logger.error("Failed to connect to " + host + ":" + std::to_string(port));
        return json();
    }

    semantic_beam_search_logger.info("Connected to " + host + ":" + std::to_string(port));

    // 2. Send EXPAND_REMOTE_BATCH command
    semantic_beam_search_logger.info("Sending EXPAND_REMOTE_BATCH command");
    if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH,
                                   JasmineGraphInstanceProtocol::EXPAND_NODE_BATCH,
                                   JasmineGraphInstanceProtocol::OK)) {
        semantic_beam_search_logger.error("Remote expansion init failed");
        close(sockfd);
        return json();
    }
    semantic_beam_search_logger.info("Remote expansion command sent successfully");

    // 3. Send graphID
    semantic_beam_search_logger.info("Sending graphID: " + std::to_string(gc.graphID));
    Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH,
                              std::to_string(gc.graphID), JasmineGraphInstanceProtocol ::OK);

    // 4. Send nodeIds batch as JSON
    semantic_beam_search_logger.info("Preparing nodeIds batch for remote expansion");
    json request;
    request["currentPaths"] = currentPaths;
    request["fromPartition"] = std::to_string(gc.partitionID);
    request["toPartition"] = partitionId;
    request["queryEmbedding"] =this->emb;
    std::string requestStr = request.dump();
    semantic_beam_search_logger.info("Sending request JSON: " + requestStr);

    int requestLen = htonl(requestStr.size());
    send(sockfd, &requestLen, sizeof(int), 0);
    send(sockfd, requestStr.c_str(), requestStr.size(), 0);


    // send embedding
    // semantic_beam_search_logger.info("Sending query embedding of size: " + std::to_string(emb.size()));
    // // std::vector<float> vec = {0.6083, 0.4139, -0.3170};
    // int32_t size = emb.size();
    // send(sockfd, &size, sizeof(size), 0);
    //
    // send(sockfd, emb.data(), emb.size() * sizeof(float), 0);
    // 5. Receive response with expanded paths
    semantic_beam_search_logger.info("Waiting for response length from remote server");
    int respLen;
    recv(sockfd, &respLen, sizeof(int), 0);
    respLen = ntohl(respLen);
    semantic_beam_search_logger.info("Response length received: " + std::to_string(respLen));
    std::string respStr(respLen, 0);
    semantic_beam_search_logger.info("Receiving response data from remote server");
ssize_t totalReceived = 0;
    while (totalReceived < respLen) {
        ssize_t bytes = recv(sockfd, &respStr[totalReceived], respLen - totalReceived, 0);
        if (bytes <= 0) {
            semantic_beam_search_logger.error("Error or connection closed while receiving response data");
            break;
        }
        totalReceived += bytes;
    }
    semantic_beam_search_logger.info("Parsing response JSON");
    semantic_beam_search_logger.info("Sending response JSON: " + respStr);
    json response = json::parse(respStr);
    semantic_beam_search_logger.info("Remote expansion response parsed successfully");

    for (auto& expanded : response["expandedPaths"]) {
        semantic_beam_search_logger.info("Processing expanded path: " + expanded.dump());
        float score = expanded["score"].get<float>();
        expandedPaths.push_back({expanded["pathObj"], score});
    }

    semantic_beam_search_logger.info("Sending CLOSE command to remote server");
    Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
    close(sockfd);
    semantic_beam_search_logger.info("Socket closed, returning response");
    return response;
}