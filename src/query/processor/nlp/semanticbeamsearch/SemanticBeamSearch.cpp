/**
Copyright 2025 JasmineGraph Team
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */
#include "SemanticBeamSearch.h"

#include <float.h>

#include <iostream>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "../../../../nativestore/NodeManager.h"
#include "../../../../server/JasmineGraphInstanceProtocol.h"
#include "../../../../server/JasmineGraphServer.h"
#include "../../../../vectorstore/FaissIndex.h"
#include "../../cypher/runtime/Helpers.h"

Logger semantic_beam_search_logger;

SemanticBeamSearch::SemanticBeamSearch(
    FaissIndex* faissStore, FaissIndex* faissEdgeStore, TextEmbedder* textEmbedder, std::vector<float> emb,
    int k, GraphConfig gc, vector<JasmineGraphServer::worker> workerList, NodeManager* nodeManager)
    : faissStore(faissStore), faissEdgeStore(faissEdgeStore),
      textEmbedder(textEmbedder),
      emb(std::move(emb)),
      k(k),
      gc(gc),
      workerList(workerList), nodeManager(nodeManager) {
  // Constructor implementation
  semantic_beam_search_logger.debug(
      "SemanticBeamSearch initialized with k: " + std::to_string(k) +
      ", embedding size: " + std::to_string(emb.size()) +
      ", graph ID: " + std::to_string(gc.graphID) +
      ", partition ID: " + std::to_string(gc.partitionID));
}

std::vector<ScoredPath> SemanticBeamSearch::getSeedNodes() {
  // check the emb
  std::vector<ScoredPath> paths;
  try {
    auto results = faissStore->search(emb, 5);
    semantic_beam_search_logger.debug("Top " + to_string(results.size())+ " nodes found");
      int pathId = 0;
    for (auto& [id, dist] : results) {
        semantic_beam_search_logger.debug("Retrieved Node from FAISS index: " +faissStore->getNodeIdFromEmbeddingId(
            (id)));

      NodeBlock* seedNode =
          nodeManager->get(faissStore->getNodeIdFromEmbeddingId((id)));
      if (!seedNode) continue;

      json initialPath;
      initialPath["pathNodes"] = json::array();
        set<string> pathNodeIds;
      json nodeData;
      nodeData["partitionID"] =
          std::string(seedNode->getMetaPropertyHead()->value);

      auto properties = seedNode->getAllProperties();
      for (const auto& [key, value] : properties) {
        nodeData[key] = value;
      }

      initialPath["pathNodes"].push_back(nodeData);
        pathNodeIds.insert(nodeData["id"].get<std::string>());
      initialPath["pathRels"] = json::array();
        set<string>  pathRelIds;

      float score = Utils::cosineSimilarity(
          emb, faissStore->getEmbeddingById(nodeData["id"]));

        if (score < 0.55) {
            continue;
        }
        HopTrace seedTrace;
        seedTrace.hop = 0;
        seedTrace.expandedFromNode = "QUERY";
        seedTrace.viaRelationType = "SEED";
        seedTrace.toNode = nodeData["id"];
        seedTrace.nodeScore = score;
        seedTrace.relationScore = 0.0f;
        seedTrace.cumulativeScore = score;
      paths.push_back({pathId, initialPath, pathNodeIds, pathRelIds, score , {seedTrace}});
        pathId++;
        string displayName;
        if (nodeData.contains("name")) {
            displayName = nodeData["name"].get<std::string>();
        } else {
            displayName = nodeData["id"].get<std::string>();
        }
      semantic_beam_search_logger.debug("Seed node: " + nodeData["name"].get<std::string>() +
          ", Distance: " +to_string(dist));
    }
  } catch (std::exception& e) {
      semantic_beam_search_logger.error(std::string("getSeedNodes exception: ") + e.what());
    }
  return paths;
}

void SemanticBeamSearch::semanticMultiHopBeamSearch(SharedBuffer& buffer,
                                                    int numHops,
                                                    int beamWidth) {
  semantic_beam_search_logger.debug(
      "Starting semantic Multi-Hop Beam Search with following number of hops : " +
      std::to_string(numHops) + ", beamWidth: " + std::to_string(beamWidth));
    json report;
    report["numHops"] = numHops;
    report["beamWidth"] = k;
    report["results"] = json::array();
  // 1. Get seed nodes using FAISS
  std::vector<ScoredPath> paths = getSeedNodes();
  semantic_beam_search_logger.debug("Seed nodes retrieved: " +
                                   std::to_string(paths.size()));

  // Debug: Print all seed paths
  for (size_t i = 0; i < paths.size(); ++i) {
    semantic_beam_search_logger.debug(
        "Seed path " + std::to_string(i) + ": " + paths[i].pathObj.dump() +
        ", score: " + std::to_string(paths[i].score));
  }

    // set<string> visitedNodes;
    set<string>visitedRelations;


  // Initialize paths

  // 2. Multi-hop beam search
  for (int hop = 1; hop <= numHops; ++hop) {
    semantic_beam_search_logger.debug(
        "Hop " + std::to_string(hop) +
        " started. Current paths: " + std::to_string(paths.size()));
    std::vector<ScoredPath> expandedPaths;
    std::unordered_map<std::string, std::vector<ScoredPath>> remoteFrontier;
    std::vector<string> embeddingRequestsForNewlyExploredEdges;

    for (size_t spIdx = 0; spIdx < paths.size(); ++spIdx) {
        bool isPathExpanded = false;
      auto& sp = paths[spIdx];
      semantic_beam_search_logger.debug("Expanding path index " +
                                        std::to_string(spIdx) + ": " +
                                        sp.pathObj.dump());
      auto currentPath = sp.pathObj;
        int pathId = sp.pathId;
        auto pathNodeIds = sp.pathNodeIds;
        auto pathRelIds = sp.pathRelIds;
      float score = sp.score;
       vector<HopTrace> pathTrace = sp.hopTraces;
      semantic_beam_search_logger.debug("Current path object: " +
                                        currentPath.dump());
      json lastNodeJson = currentPath["pathNodes"].back();
      string lastRelationId = "";
      if (!currentPath["pathRels"].empty()) {
        json lastRelationJson = currentPath["pathRels"].back();
        lastRelationId = lastRelationJson["id"].get<std::string>();
      }
      semantic_beam_search_logger.debug("Last node JSON: " +
                                        lastNodeJson.dump());
      if (lastNodeJson["id"].empty()) continue;
      string lastNodeId = lastNodeJson["id"].get<std::string>();
        // visitedNodes.insert(lastNodeId);
      semantic_beam_search_logger.debug("Last node ID: " + lastNodeId);
      std::string destPartitionId =
          lastNodeJson["partitionID"].get<std::string>();
      semantic_beam_search_logger.debug("Destination partition ID: " +
                                        destPartitionId);
      if (destPartitionId != std::to_string(gc.partitionID)) {
        semantic_beam_search_logger.debug("Queueing remote node " + lastNodeId +
                                         " for partition " + destPartitionId);

        remoteFrontier[destPartitionId].push_back(sp);

        continue;  // skip local expansion
      }
      NodeBlock* lastNode = nodeManager->get(lastNodeJson["id"].get<std::string>());


      if (!lastNode) {
        semantic_beam_search_logger.debug(
            "Last node not found for path, skipping.");
        continue;
      }
        lastNode->nodeId = stoi(lastNodeJson["id"].get<std::string>());
      semantic_beam_search_logger.debug("Last node ID: " +
                                        lastNodeJson["id"].get<std::string>());

      // Expand local + central relations
      auto expandRelations = [&](RelationBlock* relation) {
        int relCount = 0;
          string direction;
        while (relation) {
          auto newPathNodeIds = pathNodeIds;
          auto newPathRelIds  = pathRelIds;
          relCount++;
          semantic_beam_search_logger.debug("Expanding relation #" +
                                            std::to_string(relCount));
            NodeBlock* expandedNode;

            if (lastNode->addr == relation->source.address) {
                expandedNode =  relation->getDestination();
                direction = "right";
            } else {
                expandedNode = relation->getSource();
                direction = "left";
            }
          if (!expandedNode) {
            semantic_beam_search_logger.debug(
                "Exapnded node not found for relation, skipping.");
              if (lastNode->addr == relation->source.address) {
                  relation =  relation->nextLocalSource();
              } else {
                  relation =  relation->nextLocalDestination();
              }
            if (relation) {
              continue;
            } else {
              break;
            }
          }
          semantic_beam_search_logger.debug("Expanded node ID: " +
                                            std::to_string(expandedNode->nodeId));

          // Create new path
          json newPath = currentPath;
          json relData;
            relData["direction"] = direction;
          auto relProps = relation->getAllProperties();
          for (auto& [k, v] : relProps) relData[k] = v;
          if (relProps.empty()) {
            relation = relation->nextLocalSource();
            continue;
          }

          if (relData.contains("id") &&
              visitedRelations.find(relData["id"].get<std::string>())!= visitedRelations.end()
              || newPathNodeIds.find(to_string(expandedNode->nodeId))!=  newPathNodeIds.end()) {
            semantic_beam_search_logger.debug("Skipping visited relation");
              if (lastNode->addr == relation->source.address) {
                  relation =  relation->nextLocalSource();
              } else {
                  relation =  relation->nextLocalDestination();
              }

            continue;
          }
          newPath["pathRels"].push_back(relData);
            newPathRelIds.insert(relData["id"].get<std::string>());
            visitedRelations.insert(relData["id"].get<std::string>());

          semantic_beam_search_logger.debug("Relation properties: " +
                                            relData.dump());
          std::string edgeText;
          auto it = relProps.find("type");
          if (it != relProps.end()) {
            edgeText = std::string(it->second);
          }

          // push into array
          if (typeEmbeddingCache.find(edgeText) == typeEmbeddingCache.end()) {
            // not cached → request an embedding
            embeddingRequestsForNewlyExploredEdges.push_back(edgeText);
          }

          semantic_beam_search_logger.debug("Relation properties: " +
                                            relData.dump());

          json nodeData;
          semantic_beam_search_logger.debug("Destination 168 node ID: " +
                                            std::to_string(expandedNode->nodeId));
          auto nodeProps = expandedNode->getAllProperties();
          nodeData["partitionID"] =
              std::string(expandedNode->getMetaPropertyHead()->value);
          for (auto& [k, v] : nodeProps) nodeData[k] = v;
            expandedNode->nodeId = stoi(nodeData["id"].get<std::string>());
          vector<float> emb_ =
              faissStore->getEmbeddingById(std::to_string(expandedNode->nodeId));
          semantic_beam_search_logger.debug("Scoring node ID: " +
                                            std::to_string(expandedNode->nodeId));
          newPath["pathNodes"].push_back(nodeData);
            newPathNodeIds.insert(std::to_string(expandedNode->nodeId));

          semantic_beam_search_logger.debug(
              "Expanded to node ID: " + std::to_string(expandedNode->nodeId) +
              ", interim score: " + std::to_string(score));
          semantic_beam_search_logger.debug("Node properties: " +
                                            nodeData.dump());
            float newScore = score;

            HopTrace trace;
            trace.hop = hop;
            trace.expandedFromNode = lastNodeId;
            trace.viaRelationType = relData.contains("type")
                                        ? relData["type"].get<std::string>()
                                        : "UNKNOWN";
            trace.toNode = std::to_string(expandedNode->nodeId);
            trace.nodeScore = 0.0f;
            trace.relationScore = 0.0f;  // added later
            trace.cumulativeScore = newScore;

            auto newHopTraces = sp.hopTraces;
            newHopTraces.push_back(trace);

            expandedPaths.push_back({
                sp.pathId,
                newPath,
                newPathNodeIds,
                newPathRelIds,
                newScore,
                newHopTraces
            });
            isPathExpanded = true;

          semantic_beam_search_logger.debug(
              "Expanded path to node " + std::to_string(expandedNode->nodeId) +
              " with score " + std::to_string(score));
          semantic_beam_search_logger.debug("Expanded path JSON: " +
                                            newPath.dump());
            semantic_beam_search_logger.debug(to_string(newHopTraces[0].hop));


            // visitedNodes.insert(nodeData["id"].get<std::string>());
            visitedRelations.insert(relData["id"].get<std::string>());
            if (lastNode->addr == relation->source.address) {
                relation =  relation->nextLocalSource();
            } else {
                relation =  relation->nextLocalDestination();
            }
        }
        semantic_beam_search_logger.debug("Total relations expanded: " +
                                          std::to_string(relCount));
      };
      auto expandCentralRelations = [&](RelationBlock* relation) {
        int relCount = 0;
          string direction;

        while (relation) {
            auto newPathNodeIds = pathNodeIds;
            auto newPathRelIds  = pathRelIds;
            relCount++;
            semantic_beam_search_logger.debug("Expanding relation #" +
                                            std::to_string(relCount));
            NodeBlock* expandedNode;

            if (lastNode->addr == relation->source.address) {
                expandedNode =  relation->getDestination();
                direction = "right";

            } else {
                expandedNode = relation->getSource();
            }

            if (!expandedNode) {
            semantic_beam_search_logger.debug(
                "Destination node not found for relation, skipping.");
                if (lastNode->addr == relation->source.address) {
                    relation =  relation->nextLocalSource();
                    direction = "right";

                } else {
                    relation =  relation->nextLocalDestination();
                    direction = "left";
                }
            continue;
          }
          semantic_beam_search_logger.debug("Destination node ID: " +
                                            std::to_string(expandedNode->nodeId));
          // Create new path
          json newPath = currentPath;
          json relData;
            relData["direction"] = direction;
          auto relProps = relation->getAllProperties();
          for (auto& [k, v] : relProps) relData[k] = v;
            if (relData.contains("id") &&
              visitedRelations.find(relData["id"].get<std::string>())!= visitedRelations.end()
              ||  newPathNodeIds.find(to_string(expandedNode->nodeId))!=  newPathNodeIds.end()) {
            semantic_beam_search_logger.debug("Skipping parent relation");
                if (lastNode->addr == relation->source.address) {
                    relation =  relation->nextLocalSource();
                } else {
                    relation =  relation->nextLocalDestination();
                }
            continue;
          }
          newPath["pathRels"].push_back(relData);
          newPathRelIds.insert(relData["id"].get<std::string>());
          std::string edgeText;
          auto it = relProps.find("type");
          if (it != relProps.end()) {
            edgeText = std::string(it->second);
          }

          if (typeEmbeddingCache.find(edgeText) == typeEmbeddingCache.end()) {
            // not cached → request an embedding
            embeddingRequestsForNewlyExploredEdges.push_back(edgeText);
          }

          semantic_beam_search_logger.debug("Relation properties: " +
                                            relData.dump());

          json nodeData;
          semantic_beam_search_logger.debug("Destination  node ID: " +
                                            std::to_string(expandedNode->nodeId));

          auto nodeProps = expandedNode->getAllProperties();
          nodeData["partitionID"] =
              std::string(expandedNode->getMetaPropertyHead()->value);
          for (auto& [k, v] : nodeProps) nodeData[k] = v;

            expandedNode->nodeId = stoi(nodeData["id"].get<std::string>());
          vector<float> emb_ =
              faissStore->getEmbeddingById(std::to_string(expandedNode->nodeId));
          semantic_beam_search_logger.debug("Scoring node ID: " +
                                            std::to_string(expandedNode->nodeId));
          newPath["pathNodes"].push_back(nodeData);
          semantic_beam_search_logger.debug(
              "Expanded to node ID: " + std::to_string(expandedNode->nodeId) +
              ", interim score: " + std::to_string(score));

          semantic_beam_search_logger.debug("Node properties: " +
                                            nodeData.dump());

            float newScore = score;

            HopTrace trace;
            trace.hop = hop;
            trace.expandedFromNode = lastNodeId;
            trace.viaRelationType = relData.contains("type")
                                        ? relData["type"].get<std::string>()
                                        : "UNKNOWN";
            trace.toNode = std::to_string(expandedNode->nodeId);
            trace.nodeScore = 0.0f;
            trace.relationScore = 0.0f;  // added later
            trace.cumulativeScore = newScore;

            auto newHopTraces = sp.hopTraces;
            newHopTraces.push_back(trace);

            expandedPaths.push_back({
                sp.pathId,
                newPath,
                newPathNodeIds,
                newPathRelIds,
                newScore,
                newHopTraces
            });
            isPathExpanded = true;

          semantic_beam_search_logger.debug(
              "Expanded path to node " + std::to_string(expandedNode->nodeId) +
              " with score " + std::to_string(score));
          semantic_beam_search_logger.debug("Expanded path JSON: " +
                                            newPath.dump());
          semantic_beam_search_logger.debug(to_string(newHopTraces[0].hop));
            if (lastNode->addr == relation->source.address) {
                relation =  relation->nextLocalSource();
            } else {
                relation =  relation->nextLocalDestination();
            }
        }

        semantic_beam_search_logger.debug("Total relations expanded: " +
                                          std::to_string(relCount));
      };

      RelationBlock* localRel =
          RelationBlock::getLocalRelation(lastNode->edgeRef);
      if (localRel) {
        semantic_beam_search_logger.debug("Expanding local relations for node " +
                                         std::to_string(lastNode->nodeId));
        expandRelations(localRel);
      } else {
        semantic_beam_search_logger.debug("No local relations for node " +
                                          std::to_string(lastNode->nodeId));
      }

      RelationBlock* centralRel =
          RelationBlock::getCentralRelation(lastNode->centralEdgeRef);
      if (centralRel) {
        semantic_beam_search_logger.debug(
            "Expanding central relations for node " +
            std::to_string(lastNode->nodeId));
        expandCentralRelations(centralRel);
      } else {
        semantic_beam_search_logger.debug("No central relations for node " +
                                          std::to_string(lastNode->nodeId));
      }
      if (expandedPaths.empty() || !isPathExpanded) {
        json spJson;
        float total = hop;
        spJson["score"] = sp.score/total;
        spJson["pathObj"] = sp.pathObj;
          spJson["hop"] = hop;
          semantic_beam_search_logger.debug("Adding terminal:" + spJson.dump());
        buffer.add(spJson.dump());
          json entry;
          entry["rank"] = FLT_MAX;
          entry["finalScore"] = sp.score;
          entry["path"] = sp.pathObj;

          json hops = json::array();
          for (const auto& h : sp.hopTraces) {
              json hopJson;
              hopJson["hop"] = h.hop;
              hopJson["from"] = h.expandedFromNode;
              hopJson["relation"] = h.viaRelationType;
              hopJson["to"] = h.toNode;
              hopJson["nodeScore"] = h.nodeScore;
              hopJson["relationScore"] = h.relationScore;
              hopJson["cumulativeScore"] = h.cumulativeScore;
              hops.push_back(hopJson);
          }

          entry["hopDetails"] = hops;
          report["results"].push_back(entry);
      }
    }

    std::vector<std::thread> expansionThreads;
    for (auto& [partitionId, currentPaths] : remoteFrontier) {
      semantic_beam_search_logger.debug("Partion ID : " + partitionId);
      if (!partitionId.empty()) {
        expansionThreads.emplace_back([&, partitionId, currentPaths]() {
          callRemoteExpansion(stoi(partitionId), currentPaths, expandedPaths,
                              embeddingRequestsForNewlyExploredEdges, hop,
                              buffer);
        });
      }
    }

    // Join all threads
    for (auto& t : expansionThreads) {
      if (t.joinable()) t.join();
    }

    // block this thread until all threads are done
    semantic_beam_search_logger.debug("All remote expansion threads joined.");
    semantic_beam_search_logger.debug(
        "Waiting for all remote expansion threads to complete.");

    // check all threads completed
    std::vector<std::vector<float>> newEmbeddings;
    if (!embeddingRequestsForNewlyExploredEdges.empty()) {
      newEmbeddings =  faissEdgeStore->getEmbeddingsByIds(embeddingRequestsForNewlyExploredEdges);
      // store embeddings in cache
      for (size_t i = 0; i < embeddingRequestsForNewlyExploredEdges.size();
           ++i) {
        typeEmbeddingCache[embeddingRequestsForNewlyExploredEdges[i]] =
            newEmbeddings[i];
      }
    }

    // now update scores using cached embeddings
    for (auto& path : expandedPaths) {
      json pathRels = path.pathObj["pathRels"].back();
      if (pathRels.contains("type")) {
        std::string edgeType = pathRels["type"].get<std::string>();
          semantic_beam_search_logger.debug("reconstructed embedding for edge type:" + edgeType);
        auto it = typeEmbeddingCache.find(edgeType);
        if (it != typeEmbeddingCache.end()) {
          float relScore = Utils::cosineSimilarity(emb, it->second);
            semantic_beam_search_logger.debug("Score reconstructed embedding for edge type: " +
                edgeType + ": "+
                std::to_string(relScore));
          path.score += relScore;
            path.hopTraces.back().relationScore = relScore;
            path.hopTraces.back().cumulativeScore += relScore;

        } else {
          semantic_beam_search_logger.warn("No embedding found for type: " +
                                           edgeType);
        }
      }
    }
    // Keep top beamWidth paths
    semantic_beam_search_logger.debug("Sorting expanded paths by score.");
    std::sort(expandedPaths.begin(), expandedPaths.end(),
              [](const ScoredPath& a, const ScoredPath& b) {
                return a.score > b.score;
              });
    if ((int)expandedPaths.size() > beamWidth) expandedPaths.resize(beamWidth);

    semantic_beam_search_logger.debug(
        "Hop " + std::to_string(hop) +
        " finished. Expanded paths: " + std::to_string(expandedPaths.size()));
    for (size_t i = 0; i < expandedPaths.size(); ++i) {
      semantic_beam_search_logger.debug(
          "Expanded path " + std::to_string(i) + ": " +
          expandedPaths[i].pathObj.dump() +
          ", score: " + std::to_string(expandedPaths[i].score));
    }
    paths = expandedPaths;
  }

  // 3. Add final paths to buffer
  semantic_beam_search_logger.info("Adding final paths to buffer. Total: " +
                                   std::to_string(paths.size()));
  for (size_t i = 0; i < paths.size(); ++i) {
    semantic_beam_search_logger.debug("272 Buffering path " +
                                      std::to_string(i) + ": " +
                                      paths[i].pathObj.dump());
    json data;
    data["pathObj"] = paths[i].pathObj;
    data["score"] = paths[i].score/ (numHops + 1);
      data["hop"] = numHops;
    buffer.add(data.dump());
  }


    for (int i = 0; i < paths.size(); ++i) {
        semantic_beam_search_logger.debug("Path Index " + std::to_string(i) +  paths[i].pathObj.dump());
        json entry;
        entry["rank"] = i + 1;
        entry["finalScore"] = paths[i].score;
        entry["path"] = paths[i].pathObj;

        json hops = json::array();
        for (const auto& h : paths[i].hopTraces) {
            json hopJson;
            hopJson["hop"] = h.hop;
            hopJson["from"] = h.expandedFromNode;
            hopJson["relation"] = h.viaRelationType;
            hopJson["to"] = h.toNode;
            hopJson["nodeScore"] = h.nodeScore;
            hopJson["relationScore"] = h.relationScore;
            hopJson["cumulativeScore"] = h.cumulativeScore;
            hops.push_back(hopJson);
        }

        entry["hopDetails"] = hops;
        report["results"].push_back(entry);
    }

    semantic_beam_search_logger.info(report.dump());
  buffer.add("-1");  // End marker
  semantic_beam_search_logger.info("semanticMultiHopBeamSearch completed.");
}

json SemanticBeamSearch::callRemoteExpansion(
    int partitionId, const std::vector<ScoredPath>& currentPaths,
    std::vector<ScoredPath>& expandedPaths,
    vector<std::string>& embeddingRequestsForNewlyExploredEdges, int hop,
    SharedBuffer& buffer) {
  semantic_beam_search_logger.info("Starting remote expansion for partition " +
                                   std::to_string(partitionId));

  // check worklist

  for (const auto& worker : workerList) {
    semantic_beam_search_logger.debug("Worker: " + worker.hostname + ":" +
                                      std::to_string(worker.port));
  }

  std::string host = this->workerList[partitionId].hostname;
  int port = workerList[partitionId].port;
  char data[FED_DATA_LENGTH + 1];
  struct sockaddr_in serv_addr;
  struct hostent* server;

  semantic_beam_search_logger.debug("Preparing to create socket for host: " +
                                   host + ", port: " + std::to_string(port));
  int sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0) {
    semantic_beam_search_logger.error(
        "Cannot create socket for remote expansion");
    return json();
  }

  if (host.find('@') != std::string::npos) {
    semantic_beam_search_logger.debug("Host contains '@', splitting...");
    host = Utils::split(host, '@')[1];
    semantic_beam_search_logger.debug("Host after split: " + host);
  }

  semantic_beam_search_logger.debug("Resolving host: " + host);
  server = gethostbyname(host.c_str());
  if (!server) {
    semantic_beam_search_logger.error("ERROR, no host named " + host);
    return json();
  }

  semantic_beam_search_logger.debug("Setting up server address struct");
  bzero((char*)&serv_addr, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  bcopy((char*)server->h_addr, (char*)&serv_addr.sin_addr.s_addr,
        server->h_length);
  serv_addr.sin_port = htons(port);

  semantic_beam_search_logger.debug("Attempting to connect to " + host + ":" +
                                   std::to_string(port));
  if (Utils::connect_wrapper(sockfd, (struct sockaddr*)&serv_addr,
                             sizeof(serv_addr)) < 0) {
    semantic_beam_search_logger.error("Failed to connect to " + host + ":" +
                                      std::to_string(port));
    return json();
  }

  semantic_beam_search_logger.debug("Connected to " + host + ":" +
                                   std::to_string(port));

  // 2. Send EXPAND_REMOTE_BATCH command
  semantic_beam_search_logger.debug("Sending EXPAND_REMOTE_BATCH command");
  if (!Utils::sendExpectResponse(
          sockfd, data, INSTANCE_DATA_LENGTH,
          JasmineGraphInstanceProtocol::EXPAND_NODE_BATCH,
          JasmineGraphInstanceProtocol::OK)) {
    semantic_beam_search_logger.error("Remote expansion init failed");
    close(sockfd);
    return json();
  }
  semantic_beam_search_logger.debug(
      "Remote expansion command sent successfully");

  // 3. Send graphID
  semantic_beam_search_logger.debug("Sending graphID: " +
                                   std::to_string(gc.graphID));
  Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH,
                            std::to_string(gc.graphID),
                            JasmineGraphInstanceProtocol ::OK);

  // 4. Send nodeIds batch as JSON
  semantic_beam_search_logger.debug(
      "Preparing nodeIds batch for remote expansion");
  json request;
  request["currentPaths"] = json::array();
  for (const auto& sp : currentPaths) {
    json spJson;
    spJson["pathObj"] = sp.pathObj;
      spJson["pathId"] = sp.pathId;

    spJson["score"] = sp.score;
      spJson["pathNodeIds"] = sp.pathNodeIds;
      spJson["pathRelsIds"] = sp.pathRelIds;
      json hopTracesJson = json::array();
      for (const auto& h : sp.hopTraces) {
          json hJson;
          hJson["hop"] = h.hop;
          hJson["from"] = h.expandedFromNode;
          hJson["relation"] = h.viaRelationType;
          hJson["to"] = h.toNode;
          hJson["nodeScore"] = h.nodeScore;
          hJson["relationScore"] = h.relationScore;
          hJson["cumulativeScore"] = h.cumulativeScore;
          hopTracesJson.push_back(hJson);
      }
      spJson["hopTraces"] = hopTracesJson;

    request["currentPaths"].push_back(spJson);
  }
  request["fromPartition"] = std::to_string(gc.partitionID);
  request["toPartition"] = partitionId;
  request["queryEmbedding"] = this->emb;
  std::string requestStr = request.dump();
  semantic_beam_search_logger.debug("Sending request JSON: " + requestStr);

  int requestLen = htonl(requestStr.size());
  send(sockfd, &requestLen, sizeof(int), 0);
  send(sockfd, requestStr.c_str(), requestStr.size(), 0);

  semantic_beam_search_logger.debug(
      "Waiting for response length from remote server");

  int respLen;
  recv(sockfd, &respLen, sizeof(int), 0);
  respLen = ntohl(respLen);
  semantic_beam_search_logger.debug("Response length received: " +
                                   std::to_string(respLen));
  std::string respStr(respLen, 0);
  semantic_beam_search_logger.debug(
      "Receiving response data from remote server");
  ssize_t totalReceived = 0;
  while (totalReceived < respLen) {
    ssize_t bytes =
        recv(sockfd, &respStr[totalReceived], respLen - totalReceived, 0);
    if (bytes <= 0) {
      semantic_beam_search_logger.error(
          "Error or connection closed while receiving response data");
      break;
    }
    totalReceived += bytes;
  }
  semantic_beam_search_logger.debug("Parsing response JSON");
  semantic_beam_search_logger.debug("Sending response JSON: " + respStr);
  json response = json::parse(respStr);
  semantic_beam_search_logger.debug(
      "Remote expansion response parsed successfully");

  for (auto& expanded : response["expandedPaths"]) {
    semantic_beam_search_logger.debug("Processing expanded path: " +
                                     expanded.dump());
    float score_ = expanded["score"];
      set<string> pathNodeIds = expanded["pathNodeIds"];
      set<string> pathRelsIds = expanded["pathRelIds"];
      vector<HopTrace> hopTraces;
      for (const auto& h : expanded["hopTraces"]) {
          HopTrace trace;
          trace.hop = h["hop"];
          trace.expandedFromNode = h["from"];
          trace.viaRelationType = h["relation"];
          trace.toNode = h["to"];
          trace.nodeScore = h["nodeScore"];
          trace.relationScore = h["relationScore"];
          trace.cumulativeScore = h["cumulativeScore"];
          hopTraces.push_back(trace);
      }

      int pathId = expanded["pathId"];


    if (expanded["pathObj"]["pathRels"].size() == hop) {
      semantic_beam_search_logger.debug(
          "Buffering expanded path with correct hop count: " +
          expanded["pathObj"].dump());
      json pathRels = expanded["pathObj"]["pathRels"].back();
      if (pathRels.contains("type")) {
        std::string edgeType = pathRels["type"].get<std::string>();

        // check if this type is already cached
        if (typeEmbeddingCache.find(edgeType) == typeEmbeddingCache.end()) {
          // not cached → request an embedding
          embeddingRequestsForNewlyExploredEdges.push_back(edgeType);
        }
      }

      expandedPaths.push_back({pathId, expanded["pathObj"], pathNodeIds, pathRelsIds,
          score_, hopTraces});
    } else {
      if (score_ > (hop - 1) * 2 + 1) buffer.add(expanded["pathObj"].dump());
    }
  }

  semantic_beam_search_logger.debug("Sending CLOSE command to remote server");
  Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
  close(sockfd);
  semantic_beam_search_logger.debug("Socket closed, returning response");
  return response;
}
