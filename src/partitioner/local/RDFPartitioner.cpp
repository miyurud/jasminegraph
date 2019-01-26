//
// Created by shalika-madhushanki on 1/25/19.
//

#include "RDFPartitioner.h"


void RDFPartitioner::convert(string graphName, string graphID, string:inputFilePath, string outputFilePath, int nParts, bool isDistributedCentralPartitions, int nThreads, int nPlaces) {
    convertWithoutDistribution(graphName, graphID, inputFilePath, outputFilePath, nParts, isDistributedCentralPartitions, nThreads, nPlaces);
    distributeEdges();
}

void MetisPartitioner:: convertWithoutDistribution(string graphName, string graphID, string inputFilePath, string outputFilePath, int nParts, bool isDistributedCentralPartitions, int nThreads, int nPlaces){
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

void distributeEdges(){
    //method implementation
}

void loadDataSet(){

}