//
// Created by chinthaka on 5/4/19.
//

#include <vector>
#include "Triangles.h"
#include "../../../localstore/JasmineGraphHashMapLocalStore.h"

long Triangles::run(JasmineGraphHashMapLocalStore graphDB, JasmineGraphHashMapCentralStore centralStore, std::string hostName) {
    return run(graphDB,centralStore, NULL,NULL);
}

long Triangles::run(JasmineGraphHashMapLocalStore graphDB, JasmineGraphHashMapCentralStore centralStore, std::string graphId, std::string partitionId) {
    map<long, unordered_set<long>> localSubGraphMap = graphDB.getUnderlyingHashMap();
    map<long, unordered_set<long>> centralDBSubGraphMap = centralStore.getUnderlyingHashMap();
    long edgeCount = graphDB.getEdgeCount();
    map<long,long> degreeDistribution = graphDB.getOutDegreeDistributionHashMap();
    map<long,long> centralDBDegreeDistribution = centralStore.getOutDegreeDistributionHashMap();
    std::map<long,long> degreeReverseLookupMap;
    std::map<long,std::set<long>> degreeMap;
    std::set<long> degreeSet;
    long startVId;
    long degree;

    std::map<long,long>::iterator it;
    std::map<long,long>::iterator degreeDistributionIterator;
    std::map<long,long>::iterator centralDBDegreeDistributionIterator;

    //Merging Local Store and Workers central stores before starting triangle count
    for (centralDBDegreeDistributionIterator = centralDBDegreeDistribution.begin();centralDBDegreeDistributionIterator != centralDBDegreeDistribution.end();++centralDBDegreeDistributionIterator) {
        long centralDBStartVid = centralDBDegreeDistributionIterator->first;
        long centralDBDegree = centralDBDegreeDistributionIterator->second;
        bool isFound = false;

        degreeDistribution[centralDBStartVid] += centralDBDegree;
        localSubGraphMap[centralDBStartVid].insert(centralDBSubGraphMap[centralDBStartVid].begin(),centralDBSubGraphMap[centralDBStartVid].end());

        /*for (degreeDistributionIterator = degreeDistribution.begin(); degreeDistributionIterator != degreeDistribution.end();++degreeDistributionIterator) {
            long localStartVid = degreeDistributionIterator->first;
            long localDBDegree = degreeDistributionIterator->second;

            if (centralDBStartVid == localStartVid) {
                localDBDegree += centralDBDegree;
                isFound = true;
                degreeDistribution[localStartVid] = localDBDegree;
                unordered_set<long> localDBMapSet = localSubGraphMap[localStartVid];
                unordered_set<long> centralDBMapSet = centralDBSubGraphMap[centralDBStartVid];

                localDBMapSet.insert(centralDBMapSet.begin(),centralDBMapSet.end());

                localSubGraphMap[localStartVid] = localDBMapSet;
            }
        }*/

        /*if (!isFound) {
            degreeDistribution[centralDBStartVid] = centralDBDegree;
            localSubGraphMap[centralDBStartVid] = centralDBSubGraphMap[centralDBStartVid];
        }*/
    }


    for (it = degreeDistribution.begin(); it != degreeDistribution.end();++it) {
        startVId = it->first;
        degree = it->second;

        degreeMap[degree].insert(startVId);

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
            std::set<long> orderedUList(uList.begin(),uList.end());
            std::set<long>::iterator uListIterator;
            for (uListIterator = orderedUList.begin();uListIterator != orderedUList.end(); ++uListIterator) {
                long u = *uListIterator;
                std::unordered_set<long> nuList = localSubGraphMap[u];
                std::set<long> orderedNuList(nuList.begin(),nuList.end());
                std::set<long>::iterator nuListIterator;
                for (nuListIterator = orderedNuList.begin();nuListIterator != orderedNuList.end();++nuListIterator) {
                    long nu = *nuListIterator;
                    if (uList.find(nu) != uList.end()) {
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
    return triangleCount;
}
