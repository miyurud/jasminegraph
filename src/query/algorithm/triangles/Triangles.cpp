//
// Created by chinthaka on 5/4/19.
//

#include <vector>
#include "Triangles.h"
#include "../../../localstore/JasmineGraphHashMapLocalStore.h"

std::string Triangles::run(JasmineGraphHashMapLocalStore graphDB, std::string hostName) {
    return run(graphDB,NULL,NULL,hostName);
}

std::string Triangles::run(JasmineGraphHashMapLocalStore graphDB, std::string graphId, std::string partitionId,
                           std::string serverHostName) {
    map<long, unordered_set<long>> localSubGraphMap = graphDB.getUnderlyingHashMap();
    long edgeCount = graphDB.getEdgeCount();
    map<long,long> degreeDistribution = graphDB.getOutDegreeDistributionHashMap();
    std::map<long,long> degreeReverseLookupMap;
    std::map<long,std::set<long>> degreeMap;
    std::set<long> degreeSet;
    long startVId;
    long degree;

    std::map<long,long>::iterator it;

    for (it = degreeDistribution.begin(); it != degreeDistribution.end();++it) {
        startVId = it->first;
        degree = it->second;

        degreeSet = degreeMap[degree];
        degreeSet.insert(startVId);

        degreeMap[degree] = degreeSet;
    }

    long triangleCount = 0;
    long varOne = 0;
    long varTwo = 0;
    long varThree = 0;
    long fullCount = 0;

    std::map<long, std::map<long, std::vector<long>>> triangleTree;
    std::vector<long> degreeListVisited;

    std::map<long,std::set<long>>::iterator iterator;

    for (iterator = degreeMap.begin(); iterator != degreeMap.end();++iterator) {
        long key = iterator->first;
        std::set<long> vertices = iterator->second;

        std::set<long>::iterator verticesIterator;

        for (verticesIterator = vertices.begin();verticesIterator != vertices.end();++verticesIterator) {
            long temp = *verticesIterator;
            std::unordered_set<long> uList = localSubGraphMap[temp];
            std::unordered_set<long>::iterator uListIterator;
            for (uListIterator = uList.begin();uListIterator != uList.end(); ++uListIterator) {
                long u = *uListIterator;
                std::unordered_set<long> nuList = localSubGraphMap[u];
                std::unordered_set<long>::iterator nuListIterator;
                for (nuListIterator = nuList.begin();nuListIterator != nuList.end();++nuListIterator) {
                    long nu = *nuListIterator;
                    unordered_set<long> nwList = localSubGraphMap[nu];
                    if (nwList.find(temp) != nwList.end()) {
                        fullCount++;
                        std::vector<long> tempVector;
                        tempVector.push_back(temp);
                        tempVector.push_back(u);
                        tempVector.push_back(nu);
                        std::sort(tempVector.begin(),tempVector.end());

                        varOne = tempVector[0];
                        varTwo = tempVector[1];
                        varThree = tempVector[2];

                        std::map<long, std::vector<long>> itemRes = triangleTree[varOne];

                        std::map<long, std::vector<long>>::iterator itemResIterator = itemRes.find(varTwo);

                        if (itemResIterator != itemRes.end()) {
                            std::vector<long> list = itemRes[varTwo];

                            std::vector<long>::iterator listIterator;
                            if (std::find(list.begin(),list.end(),varThree) == list.end()) {
                                list.push_back(varThree);
                                itemRes[varTwo] = list;
                                triangleTree[varOne] = itemRes;
                                triangleCount++;
                            }
                        } else {
                            std::vector<long> newU;
                            newU.push_back(varThree);
                            itemRes[varTwo] = newU;
                            triangleTree[varOne] = itemRes;
                            triangleCount++;
                        }
                    }
                }
            }
        }
        degreeListVisited.push_back(key);
    }

    return std::to_string(triangleCount);
}
