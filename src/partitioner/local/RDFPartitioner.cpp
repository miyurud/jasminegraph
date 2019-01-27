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

#include "RDFPartitioner.h"


void RDFPartitioner::convert(string graphName, string graphID, string

:inputFilePath,
string outputFilePath,
int nParts,
bool isDistributedCentralPartitions,
int nThreads,
int nPlaces
) {
convertWithoutDistribution(graphName, graphID, inputFilePath, outputFilePath, nParts, isDistributedCentralPartitions, nThreads, nPlaces
);

distributeEdges();

}

void MetisPartitioner::convertWithoutDistribution(string graphName, string graphID, string inputFilePath,
                                                  string outputFilePath, int nParts,
                                                  bool isDistributedCentralPartitions, int nThreads, int nPlaces) {
    this.outputFilePath = outputFilePath;
    this.nParts = nParts;
    this.graphName = graphName;
    this.isDistributedCentralPartitions = isDistributedCentralPartitions;
    this.graphID = graphID;
    this.nThreads = nThreads;
    this.nPlaces = nPlaces;

    //The following number of Treemap instances is kind of tricky, but it was decided to use nThreads number of TreeMaps to improve the data loading performance.
    /*    graphStorage = new Rail[TreeMap](nThreads);

    for(var i:int = 0n; i < nThreads; i++){
    graphStorage(i) = new TreeMap();
    }

    loadDataSet(inputFilePath);
    constructMetisFormat(-1n);
    partitionWithMetis(nParts);
     */

}

void distributeEdges() {
    //method implementation
}

void loadDataSet() {

}