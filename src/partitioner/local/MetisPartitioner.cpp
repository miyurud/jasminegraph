/**
Copyright 2019 JasmineGraph Team
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

#include <flatbuffers/flatbuffers.h>
#include "MetisPartitioner.h"
#include "../../util/Conts.h"
#include "../../util/logger/Logger.h"

Logger partitioner_logger;
std::mutex partFileMutex;
std::mutex masterFileMutex;
std::mutex partAttrFileMutex;
std::mutex masterAttrFileMutex;
std::mutex dbLock;

MetisPartitioner::MetisPartitioner(SQLiteDBInterface *sqlite) {
    this->sqlite = *sqlite;
    Utils utils;
    std::string partitionCount = utils.getJasmineGraphProperty("org.jasminegraph.server.npartitions");
    nParts = atoi(partitionCount.c_str());
}

void MetisPartitioner::loadDataSet(string inputFilePath, int graphID) {
    partitioner_logger.log("Processing dataset for partitioning", "info");
    this->graphID = graphID;
    // Output directory is created under the users home directory '~/.jasminegraph/tmp/'
    this->outputFilePath = utils.getHomeDir() + "/.jasminegraph/tmp/" + std::to_string(this->graphID);

    // Have to call createDirectory twice since it does not support recursive directory creation. Could use boost::filesystem for path creation
    this->utils.createDirectory(utils.getHomeDir() + "/.jasminegraph/");
    this->utils.createDirectory(utils.getHomeDir() + "/.jasminegraph/tmp");
    this->utils.createDirectory(this->outputFilePath);

    std::ifstream dbFile;
    dbFile.open(inputFilePath, std::ios::binary | std::ios::in);

    int firstVertex = -1;
    int secondVertex = -1;
    string line;
    char splitter;

    std::getline(dbFile, line);

    if (!line.empty()) {
        if (line.find(" ") != std::string::npos) {
            splitter = ' ';
        } else if (line.find('\t') != std::string::npos) {
            splitter = '\t';
        } else if (line.find(",") != std::string::npos) {
            splitter = ',';
        }
    }

    while (!line.empty()) {
        string vertexOne;
        string vertexTwo;

        std::istringstream stream(line);
        std::getline(stream, vertexOne, splitter);
        stream >> vertexTwo;

        firstVertex = std::stoi(vertexOne);
        secondVertex = std::stoi(vertexTwo);

        if (!zeroflag) {
            if (firstVertex == 0 || secondVertex == 0) {
                zeroflag = true;
                partitioner_logger.log("Graph has zero vertex", "info");
            }
        }

        std::vector<int> firstEdgeSet = graphStorageMap[firstVertex];
        std::vector<int> vertexEdgeSet = graphEdgeMap[firstVertex];

        if (firstEdgeSet.empty()) {
            vertexCount++;
            edgeCount++;
            edgeCountForMetis++;
            firstEdgeSet.push_back(secondVertex);
            vertexEdgeSet.push_back(secondVertex);

        } else {
            if (std::find(firstEdgeSet.begin(), firstEdgeSet.end(), secondVertex) == firstEdgeSet.end()) {
                firstEdgeSet.push_back(secondVertex);
                edgeCountForMetis++;
            }
            vertexEdgeSet.push_back(secondVertex);
            edgeCount++;
        }

        graphStorageMap[firstVertex] = firstEdgeSet;
        graphEdgeMap[firstVertex] = vertexEdgeSet;

        std::vector<int> secondEdgeSet = graphStorageMap[secondVertex];

        if (secondEdgeSet.empty()) {
            vertexCount++;
            secondEdgeSet.push_back(firstVertex);

        } else {
            if (std::find(secondEdgeSet.begin(), secondEdgeSet.end(), firstVertex) == secondEdgeSet.end()) {
                secondEdgeSet.push_back(firstVertex);
            }
        }

        graphStorageMap[secondVertex] = secondEdgeSet;

        if (firstVertex > largestVertex) {
            largestVertex = firstVertex;
        }

        if (secondVertex > largestVertex) {
            largestVertex = secondVertex;
        }

        if (firstVertex < smallestVertex) {
            smallestVertex = firstVertex;
        }

        if (secondVertex < smallestVertex) {
            smallestVertex = secondVertex;
        }

        std::getline(dbFile, line);
        while (!line.empty() && line.find_first_not_of(splitter) == std::string::npos) {
            std::getline(dbFile, line);
        }
    }
    partitioner_logger.log("Processing dataset completed", "info");
}

int MetisPartitioner::constructMetisFormat(string graph_type) {
    partitioner_logger.log("Constructing metis input format", "info");
    graphType = graph_type;
    int adjacencyIndex = 0;
    std::ofstream outputFile;
    string outputFileName = this->outputFilePath + "/grf";
    outputFile.open(outputFileName);

    outputFile << (vertexCount) << ' ' << (edgeCountForMetis) << std::endl;

    xadj.push_back(adjacencyIndex);

    for (int vertexNum = 0; vertexNum <= largestVertex; vertexNum++) {
        std::vector<int> vertexSet = graphStorageMap[vertexNum];

        if (vertexNum > smallestVertex && vertexSet.empty()) {
            partitioner_logger.log("Vertex list is not sequential. Reformatting vertex list", "info");
            vertexCount = 0;
            edgeCount = 0;
            edgeCountForMetis = 0;
            graphEdgeMap.clear();
            graphStorageMap.clear();
            smallestVertex = std::numeric_limits<int>::max();
            largestVertex = 0;
            zeroflag = false;
            return 0;
        }

        std::sort(vertexSet.begin(), vertexSet.end());

        //To handle situations where a blank line gets printed because vertexSet of zero vertex is zero in graphs with no zero vertex
        if (!zeroflag && vertexNum == 0) {
            continue;
        }

        for (std::vector<int>::const_iterator i = vertexSet.begin(); i != vertexSet.end(); ++i) {
            //To handle zero vertex
            if (zeroflag) {
                //To handle self loops
                if (vertexNum == *i) {
                    outputFile << (*i + 1) << ' ' << (*i + 1) << ' ';
                } else {
                    outputFile << (*i + 1) << ' ';
                }
            } else {
                //To handle self loops
                if (vertexNum == *i) {
                    outputFile << (*i) << ' ' << (*i) << ' ';
                } else {
                    outputFile << (*i) << ' ';
                }
            }
        }

        outputFile << std::endl;
    }
    partitioner_logger.log("Constructing metis format completed", "info");
    return 1;
}

std::vector<std::map<int, std::string>> MetisPartitioner::partitioneWithGPMetis(string partitionCount) {
    partitioner_logger.log("Partitioning with gpmetis", "info");
    if (partitionCount != "") {
        nParts = atoi(partitionCount.c_str());
    } else {
        partitioner_logger.log("Using the default partition count " + partitionCount, "info");
    }

    char buffer[128];
    std::string result = "";
    FILE *headerModify;
    std::string metisBinDir = utils.getJasmineGraphProperty("org.jasminegraph.partitioner.metis.bin");
    string metisCommand =
            metisBinDir + "/gpmetis " + this->outputFilePath + "/grf " + to_string(this->nParts) + " 2>&1";
    FILE *input = popen(metisCommand.c_str(), "r");
    if (input) {
        // read the input
        while (!feof(input)) {
            if (fgets(buffer, 128, input) != NULL) {
                result.append(buffer);
            }
        }
        pclose(input);
        if (!result.empty() && result.find("Premature") != std::string::npos) {
            vertexCount -= 1;
            string newHeader = std::to_string(vertexCount) + ' ' + std::to_string(edgeCountForMetis);
            //string command = "sed -i \"1s/.*/" + newHeader +"/\" /tmp/grf";
            string command = "sed -i \"1s/.*/" + newHeader + "/\" " + this->outputFilePath + "/grf";
            char *newHeaderChar = new char[command.length() + 1];
            strcpy(newHeaderChar, command.c_str());
            headerModify = popen(newHeaderChar, "r");
            partitioneWithGPMetis(to_string(nParts));
        } else if (!result.empty() && result.find("out of bounds") != std::string::npos) {
            vertexCount += 1;
            string newHeader = std::to_string(vertexCount) + ' ' + std::to_string(edgeCountForMetis);
            //string command = "sed -i \"1s/.*/" + newHeader +"/\" /tmp/grf";
            string command = "sed -i \"1s/.*/" + newHeader + "/\" " + this->outputFilePath + "/grf";
            char *newHeaderChar = new char[command.length() + 1];
            strcpy(newHeaderChar, command.c_str());
            headerModify = popen(newHeaderChar, "r");
            partitioneWithGPMetis(to_string(nParts));
            //However, I only found
        } else if (!result.empty() && result.find("However, I only found") != std::string::npos) {
            string firstDelimiter = "I only found";
            string secondDelimite = "edges in the file";
            unsigned first = result.find(firstDelimiter);
            unsigned last = result.find(secondDelimite);
            string newEdgeSize = result.substr(first + firstDelimiter.length() + 1,
                                               last - (first + firstDelimiter.length()) - 2);
            string newHeader = std::to_string(vertexCount) + ' ' + newEdgeSize;
            string command = "sed -i \"1s/.*/" + newHeader + "/\" " + this->outputFilePath + "/grf";
            char *newHeaderChar = new char[command.length() + 1];
            strcpy(newHeaderChar, command.c_str());
            headerModify = popen(newHeaderChar, "r");
            partitioneWithGPMetis(to_string(nParts));
        } else if (!result.empty() && result.find("Timing Information") != std::string::npos) {
            std::string line;
            string fileName = this->outputFilePath + "/grf.part." + to_string(this->nParts);
            std::ifstream infile(fileName);
            int counter = smallestVertex;
            std::map<int, int> partIndex;
            while (std::getline(infile, line)) {
                std::istringstream iss(line);
                int a;
                if (!(iss >> a)) {
                    break;
                } else {
                    partIndex[counter] = a;
                    counter++;
                }
            }
            partitioner_logger.log("Done partitioning with gpmetis", "info");
            createPartitionFiles(partIndex);

            string sqlStatement =
                    "UPDATE graph SET vertexcount = '" + std::to_string(this->vertexCount) +
                    "' ,centralpartitioncount = '" + std::to_string(this->nParts) + "' ,edgecount = '"
                    + std::to_string(this->edgeCount) + "' WHERE idgraph = '" + std::to_string(this->graphID) +
                    "'";
            this->sqlite.runUpdate(sqlStatement);
            this->fullFileList.push_back(this->partitionFileList);
            this->fullFileList.push_back(this->centralStoreFileList);
            this->fullFileList.push_back(this->centralStoreDuplicateFileList);
            this->fullFileList.push_back(this->partitionAttributeFileList);
            this->fullFileList.push_back(this->centralStoreAttributeFileList);
            this->fullFileList.push_back(this->compositeCentralStoreFileList);
            return (this->fullFileList);
        }
    } else {
        perror("Popen error in executing gpmetis command");
        partitioner_logger.log("Popen error in executing gpmetis command", "error");
    }
}

void MetisPartitioner::createPartitionFiles(std::map<int, int> partMap) {
    std::vector<size_t> centralStoreSizeVector;
    std::vector<int> sortedPartVector;
    for (int i = smallestVertex; i <= largestVertex; i++) {
        partVertexCounts[partMap[i]]++;
    }
    partitioner_logger.log("Populating edge lists before writing to files", "info");
    edgeMap = GetConfig::getEdgeMap();
    articlesMap = GetConfig::getAttributesMap();

    std::thread *threadList = new std::thread[nParts];
    int count = 0;
    for (int part = 0; part < nParts; part++) {
        populatePartMaps(partMap, part);
        count++;
    }

    // Populate the masterEdgeLists with the remaining edges after thread functions
    for (int part = 0; part < nParts; part++) {
        std::map<int, std::map<int, std::vector<int>>> commonCentralStoreEdgePartMap = commonCentralStoreEdgeMap[part];
        std::map<int, std::vector<int>>::iterator edgeMapIterator;

        for (int subPart = 0; subPart < nParts; subPart++) {
            if (part == subPart) {
                continue;
            } else {
                std::map<int, std::vector<int>> partMasterEdgesSet = duplicateMasterGraphStorageMap[subPart];
                std::map<int, std::vector<int>> commonMasterEdgeSet = commonCentralStoreEdgePartMap[subPart];
                for (edgeMapIterator = commonMasterEdgeSet.begin();
                     edgeMapIterator != commonMasterEdgeSet.end(); ++edgeMapIterator) {
                    std::vector<int> centralGraphVertexVector = partMasterEdgesSet[edgeMapIterator->first];
                    std::vector<int> secondVertexVector = edgeMapIterator->second;
                    std::vector<int>::iterator secondVertexIterator;
                    for (secondVertexIterator = secondVertexVector.begin();
                         secondVertexIterator != secondVertexVector.end(); ++secondVertexIterator) {
                        int endVertex = *secondVertexIterator;
                        centralGraphVertexVector.push_back(endVertex);
                        masterEdgeCountsWithDups[subPart]++;
                    }
                    partMasterEdgesSet[edgeMapIterator->first] = centralGraphVertexVector;
                }
                duplicateMasterGraphStorageMap[subPart] = partMasterEdgesSet;
            }
        }
    }

    partitioner_logger.log("Populating edge lists completed", "info");
    partitioner_logger.log("Writing edge lists to files", "info");
    int threadCount = nParts * 3;
    if (graphAttributeType == Conts::GRAPH_WITH_TEXT_ATTRIBUTES || graphType == Conts::GRAPH_TYPE_RDF) {
        threadCount = nParts * 5;
    }
    std::thread *threads = new std::thread[threadCount];
    count = 0;
    for (int part = 0; part < nParts; part++) {
        //threads[count] = std::thread(&MetisPartitioner::writePartitionFiles, this, part);
        threads[count] = std::thread(&MetisPartitioner::writeSerializedPartitionFiles, this, part);
        count++;
        //threads[count] = std::thread(&MetisPartitioner::writeMasterFiles, this, part);
        threads[count] = std::thread(&MetisPartitioner::writeSerializedMasterFiles, this, part);
        count++;
        threads[count] = std::thread(&MetisPartitioner::writeSerializedDuplicateMasterFiles, this, part);
        count++;
        if (graphAttributeType == Conts::GRAPH_WITH_TEXT_ATTRIBUTES) {
            threads[count] = std::thread(&MetisPartitioner::writeTextAttributeFilesForPartitions, this, part);
            count++;
            threads[count] = std::thread(&MetisPartitioner::writeTextAttributeFilesForMasterParts, this, part);
            count++;
        }
        if (graphType == Conts::GRAPH_TYPE_RDF) {
            threads[count] = std::thread(&MetisPartitioner::writeRDFAttributeFilesForPartitions, this, part);
            count++;
            threads[count] = std::thread(&MetisPartitioner::writeRDFAttributeFilesForMasterParts, this, part);
            count++;
        }
    }

    for (int tc = 0; tc < threadCount; tc++) {
        threads[tc].join();
    }
    partitioner_logger.log("Writing to files completed", "info");

    for (int part = 0; part < nParts; part++) {
        int masterEdgeCount = masterEdgeCounts[part];
        //Below two lines of code will be used in creating the composite central stores
        centralStoreSizeVector.push_back(masterEdgeCounts[part]);
        sortedPartVector.push_back(part);
        int masterEdgeCountWithDups = masterEdgeCountsWithDups[part];

        string sqlStatement =
                "UPDATE partition SET central_edgecount = '" + std::to_string(masterEdgeCount) +
                "', central_edgecount_with_dups = '" + std::to_string(masterEdgeCountWithDups) +
                "' WHERE graph_idgraph = '" + std::to_string(this->graphID) + "' AND idpartition = '" +
                std::to_string(part) + "'";
        dbLock.lock();
        this->sqlite.runUpdate(sqlStatement);
        dbLock.unlock();
    }

    //Below code segment will create the composite central stores which uses for aggregation

    if (nParts > Conts::COMPOSITE_CENTRAL_STORE_WORKER_THRESHOLD) {
        bool haveSwapped = true;

        for (unsigned j = 1; haveSwapped && j < centralStoreSizeVector.size(); ++j) {
            haveSwapped = false;

            for (unsigned i = 0; i < centralStoreSizeVector.size() - j; ++i) {
                if (centralStoreSizeVector[i] < centralStoreSizeVector[i+1]) {
                    haveSwapped = true;

                    size_t tempSize = centralStoreSizeVector[i];
                    centralStoreSizeVector[i] = centralStoreSizeVector[i+1];
                    centralStoreSizeVector[i+1] = tempSize;

                    int tempPart = sortedPartVector[i];
                    sortedPartVector[i] = sortedPartVector[i+1];
                    sortedPartVector[i+1] = tempPart;
                }
            }
        }

        std::vector<size_t>::iterator centralStoreSizeVectorIterator;
        std::map<int, std::vector<size_t>> centralStoreGroups;
        std::map<int, std::vector<int>> partitionGroups;
        std::vector<std::string> compositeGraphIdList;
        int partitionLoop = 0;

        for (centralStoreSizeVectorIterator = centralStoreSizeVector.begin();
             centralStoreSizeVectorIterator != centralStoreSizeVector.end(); ++centralStoreSizeVectorIterator) {
            size_t centralStoreSize = *centralStoreSizeVectorIterator;
            std::vector<size_t> minSumGroup = centralStoreGroups[0];
            std::vector<int> minPartitionGroup = partitionGroups[0];
            std::vector<size_t>::iterator minSumGroupIterator;
            size_t minGroupTotal = 0;
            int minGroupIndex = 0;

            for (minSumGroupIterator = minSumGroup.begin();
                 minSumGroupIterator != minSumGroup.end(); ++minSumGroupIterator) {
                size_t size = *minSumGroupIterator;
                minGroupTotal += size;
            }

            for (int loop = 0; loop < Conts::NUMBER_OF_COMPOSITE_CENTRAL_STORES; loop++) {
                std::vector<size_t> currentGroup = centralStoreGroups[loop];
                std::vector<int> currentPartitionGroup = partitionGroups[loop];
                std::vector<size_t>::iterator currentGroupIterator;
                size_t currentGroupTotal = 0;

                for (currentGroupIterator = currentGroup.begin();
                     currentGroupIterator != currentGroup.end(); ++currentGroupIterator) {
                    int currentSize = *currentGroupIterator;
                    currentGroupTotal += currentSize;
                }

                if (currentGroupTotal < minGroupTotal) {
                    minSumGroup = currentGroup;
                    minGroupTotal = currentGroupTotal;
                    minPartitionGroup = currentPartitionGroup;
                    minGroupIndex = loop;
                }
            }

            minSumGroup.push_back(centralStoreSize);
            centralStoreGroups[minGroupIndex] = minSumGroup;

            minPartitionGroup.push_back(partitionLoop);
            partitionGroups[minGroupIndex] = minPartitionGroup;

            partitionLoop++;
        }

        std::map<int, std::vector<int>>::iterator partitionGroupsIterator;

        for (partitionGroupsIterator = partitionGroups.begin();
             partitionGroupsIterator != partitionGroups.end(); ++partitionGroupsIterator) {
            std::string aggregatePartitionId = "";
            int group = partitionGroupsIterator->first;
            std::vector<int> partitionList = partitionGroupsIterator->second;

            if (partitionList.size() > 0) {
                std::vector<int>::iterator partitionListIterator;
                std::map<int, std::vector<int>> tempCompositeMap;

                for (partitionListIterator = partitionList.begin();
                     partitionListIterator != partitionList.end(); ++partitionListIterator) {
                    int partitionId = *partitionListIterator;
                    aggregatePartitionId = std::to_string(partitionId) + "_" + aggregatePartitionId;

                    std::map<int, std::vector<int>> currentStorageMap = masterGraphStorageMap[partitionId];
                    std::map<int, std::vector<int>>::iterator currentStorageMapIterator;

                    for (currentStorageMapIterator = currentStorageMap.begin();
                         currentStorageMapIterator != currentStorageMap.end(); ++currentStorageMapIterator) {
                        int startVetex = currentStorageMapIterator->first;
                        std::vector<int> secondVertexVector = currentStorageMapIterator->second;

                        std::vector<int> compositeMapSecondVertexVector = tempCompositeMap[startVetex];
                        std::vector<int>::iterator secondVertexVectorIterator;

                        for (secondVertexVectorIterator = secondVertexVector.begin();
                             secondVertexVectorIterator != secondVertexVector.end(); ++secondVertexVectorIterator) {
                            int secondVertex = *secondVertexVectorIterator;

                            if (std::find(compositeMapSecondVertexVector.begin(), compositeMapSecondVertexVector.end(),
                                          secondVertex) == compositeMapSecondVertexVector.end()) {
                                compositeMapSecondVertexVector.push_back(secondVertex);
                            }
                        }

                        tempCompositeMap[startVetex] = compositeMapSecondVertexVector;
                    }

                }

                std::string adjustedAggregatePartitionId = aggregatePartitionId.substr(0,
                                                                                       aggregatePartitionId.size() - 1);
                compositeGraphIdList.push_back(adjustedAggregatePartitionId);
                compositeMasterGraphStorageMap[adjustedAggregatePartitionId] = tempCompositeMap;
            }
        }

        std::vector<std::string>::iterator compositeGraphIdListIterator;
        std::thread *compositeCopyThreads = new std::thread[threadCount];
        int compositeCopyCount = 0;

        for (compositeGraphIdListIterator = compositeGraphIdList.begin();
             compositeGraphIdListIterator != compositeGraphIdList.end(); ++compositeGraphIdListIterator) {
            std::string compositeGraphId = *compositeGraphIdListIterator;

            compositeCopyThreads[compositeCopyCount] = std::thread(
                    &MetisPartitioner::writeSerializedCompositeMasterFiles, this, compositeGraphId);
            compositeCopyCount++;
        }

        for (int tc = 0; tc < compositeCopyCount; tc++) {
            compositeCopyThreads[tc].join();
        }
    }

    partitioner_logger.log("###METIS###", "info");

}

void MetisPartitioner::populatePartMaps(std::map<int, int> partMap, int part) {
    int partitionEdgeCount = 0;

    std::map<int, std::vector<int>>::iterator edgeMapIterator;
    std::map<int, std::vector<int>> partEdgesSet;
    std::map<int, std::vector<int>> partMasterEdgesSet;
    std::map<int, std::map<int, std::vector<int>>> commonMasterEdgeSet;
    unordered_set<int> centralPartVertices;

    if (graphType == Conts::GRAPH_TYPE_NORMAL_REFORMATTED) {

        for (edgeMapIterator = graphEdgeMap.begin(); edgeMapIterator != graphEdgeMap.end(); ++edgeMapIterator) {

            int startVertexID = edgeMapIterator->first;
            int startVertexPart = partMap[startVertexID];
            int startVertexActual = idToVertexMap[startVertexID];
            if (startVertexPart == part) {
                std::vector<int> secondVertexVector = edgeMapIterator->second;
                std::vector<int> localGraphVertexVector;
                std::vector<int> centralGraphVertexVector;

                std::vector<int>::iterator secondVertexIterator;
                for (secondVertexIterator = secondVertexVector.begin();
                     secondVertexIterator != secondVertexVector.end(); ++secondVertexIterator) {
                    int endVertexID = *secondVertexIterator;
                    int endVertexPart = partMap[endVertexID];
                    int endVertexActual = idToVertexMap[endVertexID];

                    if (endVertexPart == part) {
                        partitionEdgeCount++;
                        localGraphVertexVector.push_back(endVertexActual);
                    } else {
                        centralGraphVertexVector.push_back(endVertexActual);
                        masterEdgeCounts[part]++;
                        masterEdgeCountsWithDups[part]++;
                        centralPartVertices.insert(endVertexActual);
                        commonMasterEdgeSet[endVertexPart][startVertexActual].push_back(endVertexActual);
                    }
                }
                if (localGraphVertexVector.size() > 0) {
                    partEdgesSet[startVertexActual] = localGraphVertexVector;
                }

                if (centralGraphVertexVector.size() > 0) {
                    partMasterEdgesSet[startVertexActual] = centralGraphVertexVector;
                }

            }
        }
    } else {
        for (edgeMapIterator = graphEdgeMap.begin(); edgeMapIterator != graphEdgeMap.end(); ++edgeMapIterator) {

            int startVertex = edgeMapIterator->first;
            int startVertexPart = partMap[startVertex];
            if (startVertexPart == part) {
                std::vector<int> secondVertexVector = edgeMapIterator->second;
                std::vector<int> localGraphVertexVector;
                std::vector<int> centralGraphVertexVector;

                std::vector<int>::iterator secondVertexIterator;
                for (secondVertexIterator = secondVertexVector.begin();
                     secondVertexIterator != secondVertexVector.end(); ++secondVertexIterator) {
                    int endVertex = *secondVertexIterator;
                    int endVertexPart = partMap[endVertex];

                    if (endVertexPart == part) {
                        partitionEdgeCount++;
                        localGraphVertexVector.push_back(endVertex);
                    } else {
                        masterEdgeCounts[part]++;
                        masterEdgeCountsWithDups[part]++;
                        centralPartVertices.insert(endVertex);
                        /*This edge's two vertices belong to two different parts.
                        * Therefore the edge is added to both partMasterEdgeSets
                        * This adds the edge to the masterGraphStorageMap with key being the part of vertex 1
                        */
                        centralGraphVertexVector.push_back(endVertex);

                        /* We need to insert these central store edges to the masterGraphStorageMap where the key is the
                        * second vertex's part. But it cannot be done inside the thread as it generates a race condition
                        * due to multiple threads trying to write to masterGraphStorageMap's maps apart from the one
                        * assigned to the thread. Therefore, we take all such edges to a separate data structure and
                        * add them to the masterGraphStorageMap later by a single thread (main thread)
                        */
                        commonMasterEdgeSet[endVertexPart][startVertex].push_back(endVertex);
                    }
                }

                if (localGraphVertexVector.size() > 0) {
                    partEdgesSet[startVertex] = localGraphVertexVector;
                }

                if (centralGraphVertexVector.size() > 0) {
                    partMasterEdgesSet[startVertex] = centralGraphVertexVector;
                }
            }
        }
    }

    partitionedLocalGraphStorageMap[part] = partEdgesSet;
    masterGraphStorageMap[part] = partMasterEdgesSet;
    commonCentralStoreEdgeMap[part] = commonMasterEdgeSet;

    string sqlStatement =
            "INSERT INTO partition (idpartition,graph_idgraph,vertexcount,central_vertexcount,edgecount) VALUES(\"" +
            std::to_string(part) + "\", \"" + std::to_string(this->graphID) +
            "\", \"" + std::to_string(partVertexCounts[part]) + "\",\"" + std::to_string(centralPartVertices.size()) +
            "\",\""
            + std::to_string(partitionEdgeCount) + "\")";
    dbLock.lock();
    this->sqlite.runUpdate(sqlStatement);
    dbLock.unlock();
    centralPartVertices.clear();
    commonMasterEdgeSet.clear();
    partMasterEdgesSet.clear();
    partEdgesSet.clear();
}

void MetisPartitioner::writeSerializedPartitionFiles(int part) {

    string outputFilePart = outputFilePath + "/" + std::to_string(this->graphID) + "_" + std::to_string(part);

    std::map<int, std::vector<int>> partEdgeMap = partitionedLocalGraphStorageMap[part];

    JasmineGraphHashMapLocalStore *hashMapLocalStore = new JasmineGraphHashMapLocalStore();
    hashMapLocalStore->storePartEdgeMap(partEdgeMap, outputFilePart);

    //Compress part files
    this->utils.compressFile(outputFilePart);
    partFileMutex.lock();
    partitionFileList.insert(make_pair(part, outputFilePart + ".gz"));
    partFileMutex.unlock();
    partitioner_logger.log("Serializing done for local part " + to_string(part), "info");
}

void MetisPartitioner::writeSerializedMasterFiles(int part) {

    string outputFilePartMaster =
            outputFilePath + "/" + std::to_string(this->graphID) + "_centralstore_" + std::to_string(part);

    std::map<int, std::vector<int>> partMasterEdgeMap = masterGraphStorageMap[part];

    JasmineGraphHashMapCentralStore *hashMapCentralStore = new JasmineGraphHashMapCentralStore();
    hashMapCentralStore->storePartEdgeMap(partMasterEdgeMap, outputFilePartMaster);

    this->utils.compressFile(outputFilePartMaster);
    masterFileMutex.lock();
    centralStoreFileList.insert(make_pair(part, outputFilePartMaster + ".gz"));
    masterFileMutex.unlock();
    partitioner_logger.log("Serializing done for central part " + to_string(part), "info");
}

void MetisPartitioner::writeSerializedDuplicateMasterFiles(int part) {

    string outputFilePartMaster =
            outputFilePath + "/" + std::to_string(this->graphID) + "_centralstore_dp_" + std::to_string(part);

    std::map<int, std::vector<int>> partMasterEdgeMap = duplicateMasterGraphStorageMap[part];

    JasmineGraphHashMapCentralStore *hashMapCentralStore = new JasmineGraphHashMapCentralStore();
    hashMapCentralStore->storePartEdgeMap(partMasterEdgeMap, outputFilePartMaster);

    this->utils.compressFile(outputFilePartMaster);
    masterFileMutex.lock();
    centralStoreDuplicateFileList.insert(make_pair(part, outputFilePartMaster + ".gz"));
    masterFileMutex.unlock();
    partitioner_logger.log("Serializing done for duplicate central part " + to_string(part), "info");
}

void MetisPartitioner::writePartitionFiles(int part) {

    string outputFilePart = outputFilePath + "/" + std::to_string(this->graphID) + "_" + std::to_string(part);

    std::map<int, std::vector<int>> partEdgeMap = partitionedLocalGraphStorageMap[part];

    if (!partEdgeMap.empty()) {
        std::ofstream localFile(outputFilePart);

        if (localFile.is_open()) {
            for (auto it = partEdgeMap.begin(); it != partEdgeMap.end(); ++it) {
                int vertex = it->first;
                std::vector<int> destinationSet = it->second;

                if (!destinationSet.empty()) {
                    for (std::vector<int>::iterator itr = destinationSet.begin(); itr != destinationSet.end(); ++itr) {
                        string edge;

                        if (graphType == Conts::GRAPH_TYPE_RDF) {
                            auto entry = edgeMap.find(make_pair(vertex, (*itr)));
                            long article_id = entry->second;

                            edge = std::to_string(vertex) + " " + std::to_string((*itr)) + " " +
                                   std::to_string(article_id);
                        } else {
                            edge = std::to_string(vertex) + " " + std::to_string((*itr));
                        }
                        localFile << edge;
                        localFile << "\n";
                    }
                }
            }
        }
        localFile.flush();
        localFile.close();
    }

    //Compress part files
    this->utils.compressFile(outputFilePart);
    partFileMutex.lock();
    partitionFileList.insert(make_pair(part, outputFilePart + ".gz"));
    partFileMutex.unlock();
}

void MetisPartitioner::writeMasterFiles(int part) {

    string outputFilePartMaster =
            outputFilePath + "/" + std::to_string(this->graphID) + "_centralstore_" + std::to_string(part);

    std::map<int, std::vector<int>> partMasterEdgeMap = masterGraphStorageMap[part];

    if (!partMasterEdgeMap.empty()) {
        std::ofstream masterFile(outputFilePartMaster);

        if (masterFile.is_open()) {
            for (auto it = partMasterEdgeMap.begin(); it != partMasterEdgeMap.end(); ++it) {
                int vertex = it->first;
                std::vector<int> destinationSet = it->second;

                if (!destinationSet.empty()) {
                    for (std::vector<int>::iterator itr = destinationSet.begin();
                         itr != destinationSet.end(); ++itr) {
                        string edge;

                        if (graphType == Conts::GRAPH_TYPE_RDF) {
                            auto entry = edgeMap.find(make_pair(vertex, (*itr)));
                            long article_id = entry->second;

                            edge = std::to_string(vertex) + " " + std::to_string((*itr)) + " " +
                                   std::to_string(article_id);

                        } else {
                            edge = std::to_string(vertex) + " " + std::to_string((*itr));

                        }
                        masterFile << edge;
                        masterFile << "\n";
                    }
                }
            }
        }
        masterFile.flush();
        masterFile.close();
    }

    this->utils.compressFile(outputFilePartMaster);
    masterFileMutex.lock();
    centralStoreFileList.insert(make_pair(part, outputFilePartMaster + ".gz"));
    masterFileMutex.unlock();
}

void MetisPartitioner::writeTextAttributeFilesForPartitions(int part) {
    string attributeFilePart =
            outputFilePath + "/" + std::to_string(this->graphID) + "_attributes_" + std::to_string(part);

    std::map<int, std::vector<int>> partEdgeMap = partitionedLocalGraphStorageMap[part];

    ofstream partfile;
    partfile.open(attributeFilePart);

    unordered_set<int> partVertices;

    for (auto it = partEdgeMap.begin(); it != partEdgeMap.end(); ++it) {
        int vertex1 = it->first;
        if (partVertices.insert(vertex1).second) {
            auto vertex1_ele = attributeDataMap.find(vertex1);
            partfile << vertex1_ele->first << "\t" << vertex1_ele->second << endl;
        }

        for (auto it2 = it->second.begin(); it2 != it->second.end(); ++it2) {
            int vertex2 = *it2;
            if (partVertices.insert(vertex2).second) {
                auto vertex2_ele = attributeDataMap.find(vertex2);
                partfile << vertex2_ele->first << "\t" << vertex2_ele->second << endl;
            }
        }
    }

    partfile.close();

    this->utils.compressFile(attributeFilePart);
    partAttrFileMutex.lock();
    partitionAttributeFileList.insert(make_pair(part, attributeFilePart + ".gz"));
    partAttrFileMutex.unlock();
    partitioner_logger.log("Attribute writing done for local part " + to_string(part), "info");
}

void MetisPartitioner::writeTextAttributeFilesForMasterParts(int part) {
    string attributeFilePartMaster =
            outputFilePath + "/" + std::to_string(this->graphID) + "_centralstore_attributes_" +
            std::to_string(part);

    std::map<int, std::vector<int>> partMasterEdgeMap = masterGraphStorageMap[part];

    ofstream partfile;
    partfile.open(attributeFilePartMaster);
    unordered_set<int> masterPartVertices;

    for (auto it = partMasterEdgeMap.begin(); it != partMasterEdgeMap.end(); ++it) {

        int vertex1 = it->first;
        if (masterPartVertices.insert(vertex1).second) {
            auto vertex1_ele = attributeDataMap.find(vertex1);
            partfile << vertex1_ele->first << "\t" << vertex1_ele->second << endl;
        }

        for (auto it2 = it->second.begin(); it2 != it->second.end(); ++it2) {

            int vertex2 = *it2;
            if (masterPartVertices.insert(vertex2).second) {
                auto vertex2_ele = attributeDataMap.find(vertex2);
                partfile << vertex2_ele->first << "\t" << vertex2_ele->second << endl;
            }
        }
    }

    partfile.close();

    this->utils.compressFile(attributeFilePartMaster);
    masterAttrFileMutex.lock();
    centralStoreAttributeFileList.insert(make_pair(part, attributeFilePartMaster + ".gz"));
    masterAttrFileMutex.unlock();
    partitioner_logger.log("Attribute writing done for central part " + to_string(part), "info");
}

void MetisPartitioner::writeRDFAttributeFilesForPartitions(int part) {

    std::map<int, std::vector<int>> partEdgeMap = partitionedLocalGraphStorageMap[part];
    std::map<long, std::vector<string>> partitionedEdgeAttributes;

    string attributeFilePart =
            outputFilePath + "/" + std::to_string(this->graphID) + "_attributes_" + std::to_string(part);

    //edge attribute separation for partition files
    for (auto it = partEdgeMap.begin(); it != partEdgeMap.end(); ++it) {
        for (auto it2 = it->second.begin(); it2 != it->second.end(); ++it2) {
            auto entry = edgeMap.find(make_pair(it->first, *it2));
            long article_id = entry->second;
            std::vector<string> attributes;
            auto array = (articlesMap.find(article_id))->second;

            for (int itt = 0; itt < 7; itt++) {
                string element = (array)[itt];
                attributes.push_back(element);
            }

            partitionedEdgeAttributes.insert({article_id, attributes});
        }
    }

    JasmineGraphHashMapLocalStore *hashMapLocalStore = new JasmineGraphHashMapLocalStore();
    hashMapLocalStore->storeAttributes(partitionedEdgeAttributes, attributeFilePart);

    this->utils.compressFile(attributeFilePart);
    partAttrFileMutex.lock();
    partitionAttributeFileList.insert(make_pair(part, attributeFilePart + ".gz"));
    partAttrFileMutex.unlock();
}

void MetisPartitioner::writeRDFAttributeFilesForMasterParts(int part) {

    std::map<int, std::vector<int>> partMasterEdgeMap = masterGraphStorageMap[part];
    std::map<long, std::vector<string>> centralStoreEdgeAttributes;

    string attributeFilePartMaster =
            outputFilePath + "/" + std::to_string(this->graphID) + "_centralstore_attributes_" +
            std::to_string(part);

    //edge attribute separation for central store files
    for (auto it = partMasterEdgeMap.begin(); it != partMasterEdgeMap.end(); ++it) {
        for (auto it2 = it->second.begin(); it2 != it->second.end(); ++it2) {
            auto entry = edgeMap.find(make_pair(it->first, *it2));
            long article_id = entry->second;
            std::vector<string> attributes;
            auto array = (articlesMap.find(article_id))->second;

            for (int itt = 0; itt < 7; itt++) {
                string element = (array)[itt];
                attributes.push_back(element);
            }

            centralStoreEdgeAttributes.insert({article_id, attributes});
        }
    }

    JasmineGraphHashMapLocalStore *hashMapLocalStore = new JasmineGraphHashMapLocalStore();
    hashMapLocalStore->storeAttributes(centralStoreEdgeAttributes, attributeFilePartMaster);

    this->utils.compressFile(attributeFilePartMaster);
    masterAttrFileMutex.lock();
    centralStoreAttributeFileList.insert(make_pair(part, attributeFilePartMaster + ".gz"));
    masterAttrFileMutex.unlock();
}

string MetisPartitioner::reformatDataSet(string inputFilePath, int graphID) {
    this->graphID = graphID;

    std::ifstream inFile;
    inFile.open(inputFilePath, std::ios::binary | std::ios::in);

    string outputFile = utils.getHomeDir() + "/.jasminegraph/tmp/" + std::to_string(this->graphID) + "/" +
                        std::to_string(this->graphID);
    std::ofstream outFile;
    outFile.open(outputFile);

    int firstVertex = -1;
    int secondVertex = -1;
    string line;
    char splitter;

    std::getline(inFile, line);

    if (!line.empty()) {
        if (line.find(" ") != std::string::npos) {
            splitter = ' ';
        } else if (line.find('\t') != std::string::npos) {
            splitter = '\t';
        } else if (line.find(",") != std::string::npos) {
            splitter = ',';
        }
    }

    int idCounter = 1;

    while (!line.empty()) {
        string vertexOne;
        string vertexTwo;

        std::istringstream stream(line);
        std::getline(stream, vertexOne, splitter);
        stream >> vertexTwo;

        firstVertex = std::stoi(vertexOne);
        secondVertex = std::stoi(vertexTwo);

        if (vertexToIDMap.find(firstVertex) == vertexToIDMap.end()) {
            vertexToIDMap.insert(make_pair(firstVertex, idCounter));
            idToVertexMap.insert(make_pair(idCounter, firstVertex));
            idCounter++;
        }
        if (vertexToIDMap.find(secondVertex) == vertexToIDMap.end()) {
            vertexToIDMap.insert(make_pair(secondVertex, idCounter));
            idToVertexMap.insert(make_pair(idCounter, secondVertex));
            idCounter++;
        }

        int firstVertexID = vertexToIDMap.find(firstVertex)->second;
        int secondVertexID = vertexToIDMap.find(secondVertex)->second;

        outFile << (firstVertexID) << ' ' << (secondVertexID) << std::endl;

        std::getline(inFile, line);
        while (!line.empty() && line.find_first_not_of(splitter) == std::string::npos) {
            std::getline(inFile, line);
        }
    }

    partitioner_logger.log("Reformatting completed", "info");
    return outputFile;
}


void MetisPartitioner::loadContentData(string inputAttributeFilePath, string graphAttributeType, int graphID, string attrType="") {
    this->graphAttributeType = graphAttributeType;

    if (graphAttributeType == Conts::GRAPH_WITH_TEXT_ATTRIBUTES) {
        std::ifstream dbFile;
        dbFile.open(inputAttributeFilePath, std::ios::binary | std::ios::in);
        partitioner_logger.log("Processing features set", "info");

        char splitter;
        string line;

        std::getline(dbFile, line);

        //Infer attribute data type if not explicitly given from first line in attribute file
        if (attrType == "") {
            string firstLine = line;
            //Get substring containing attribute values
            int strpos = line.find(splitter);
            string attributes = line.substr(strpos + 1, -1);

            //Iterate through attribute string values to infer data type
            vector<string> strArr = Utils::split(attributes, splitter);
            for (vector<string>::iterator i = strArr.begin(); i != strArr.end(); i++) {
                string value = *i;
                if (value.find(".") != std::string::npos) { //Check if contains decimal point (float)
                    attrType = "float";
                    break;
                } else {
                    int convertedValue = stoi(value);
                    //Check value and determine suitable datatype
                    if (-125 <= convertedValue && convertedValue <= 127)
                        attrType = "int8";
                    else if (-32768 <= convertedValue && convertedValue <= 32767)
                        attrType = "int16";
                    else
                        attrType = "int32";
                }
            }
            cout << "Inferred feature type: " << attrType << endl;
        }

        if (!line.empty()) {
            if (line.find(" ") != std::string::npos) {
                splitter = ' ';
            } else if (line.find('\t') != std::string::npos) {
                splitter = '\t';
            } else if (line.find(",") != std::string::npos) {
                splitter = ',';
            }
        }

        while (!line.empty()) {

            int strpos = line.find(splitter);
            string vertex_str = line.substr(0, strpos);
            string attributes = line.substr(strpos + 1, -1);
            int vertex = stoi(vertex_str);
            attributeDataMap.insert({vertex, attributes});

            std::getline(dbFile, line);
            while (!line.empty() && line.find_first_not_of(splitter) == std::string::npos) {
                std::getline(dbFile, line);
            }
        }
        std::ifstream file;
        file.open(inputAttributeFilePath, std::ios::binary | std::ios::in);
        string str_line;

        std::getline(file, str_line);
        int count = 0;
        while (!str_line.empty()) {
            count += 1;
            std::istringstream ss(str_line);
            std::istream_iterator<std::string> begin(ss), end;
            std::vector<std::string> arrayFeatures(begin, end);
            string sqlStatement = "UPDATE graph SET feature_count = '" + std::to_string(arrayFeatures.size() - 1) +
                                  "', feature_type = '" + attrType + "' WHERE idgraph = '" + std::to_string(graphID) + "'";
            cout << sqlStatement << endl;
            cout << "Feature type: " << attrType << endl;
            dbLock.lock();
            this->sqlite.runUpdate(sqlStatement);
            dbLock.unlock();
            if (count == 1) {
                break;
            }
        }
    }

    // TODO :: implement for graphs with json and xml attributes files
}

void MetisPartitioner::writeSerializedCompositeMasterFiles(std::string part) {
    string outputFilePartMaster =
            outputFilePath + "/" + std::to_string(this->graphID) + "_compositecentralstore_" + part;

    std::map<int, std::vector<int>> partMasterEdgeMap = compositeMasterGraphStorageMap[part];

    JasmineGraphHashMapCentralStore *hashMapCentralStore = new JasmineGraphHashMapCentralStore();
    hashMapCentralStore->storePartEdgeMap(partMasterEdgeMap, outputFilePartMaster);

    std::vector<std::string> graphIds = this->utils.split(part,'_');
    std::vector<std::string>::iterator graphIdIterator;

    this->utils.compressFile(outputFilePartMaster);
    masterFileMutex.lock();
    for (graphIdIterator = graphIds.begin(); graphIdIterator != graphIds.end(); ++graphIdIterator) {
        std::string graphId = *graphIdIterator;
        compositeCentralStoreFileList.insert(make_pair(std::atoi(graphId.c_str()), outputFilePartMaster + ".gz"));
    }
    masterFileMutex.unlock();
    partitioner_logger.log("Serializing done for central part " + part, "info");
}
