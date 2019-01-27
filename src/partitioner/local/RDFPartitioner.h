//
// Created by shalika-madhushanki on 1/25/19.
//

#ifndef JASMINEGRAPH_RDFPARTITIONER_H
#define JASMINEGRAPH_RDFPARTITIONER_H


#include <string>
#include <string.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <map>
#include <vector>
#include <unordered_set>
#include <set>
#include "metis.h"
#include "../../metadb/SQLiteDBInterface.h"
#include "../../util/Utils.h"
#include <cstddef>
#include <algorithm>

using std::string;


class RDFPartitioner {
public:
    void convert(string graphName, string graphID, string inputFilePath,
                 string outputFilePath,
                 int nParts,
                 bool isDistributedCentralPartitions,
                 int nThreads,
                 int nPlaces
    );

    void convertWithoutDistribution(string graphName, string graphID, string inputFilePath, string outputFilePath,
                                    int nParts, bool isDistributedCentralPartitions, int nThreads, int nPlaces);

    void loadDataSet(string inputFilePath, string outputFilePath);

    void distributeEdges();
    //void partitionGraph();
//    void constructMetisFormat();
//    void partitioneWithGPMetis();
    //MetisPartitioner(SQLiteDBInterface*);
private:

    string outputFilePath;
    int nParts;
    string graphName;
    string graphID;
    int nThreads;
    int nPlaces;

    string inputFilePath;

//private:
//    idx_t edgeCount = 0;
//    idx_t largestVertex = 0;
//    idx_t vertexCount = 0;
//    //TODO:Need to remove this hardcoded value
//    idx_t nParts = 4;
//    string outputFilePath;
//    bool zeroflag = false;
//    SQLiteDBInterface sqlite;


//private var nodes:HashMap[String,Long] = new HashMap[String,Long]();
//private var nodesTemp:HashMap[Long,String] = new HashMap[Long,String]();
//private var predicates:HashMap[String,Long] = new HashMap[String,Long]();
//private var predicatesTemp:HashMap[Long,String] = new HashMap[Long,String]();
//private var relationsMap:HashMap[Long,HashMap[Long,ArrayList[String]]] = new HashMap[Long,HashMap[Long,ArrayList[String]]]();
//private var attributeMap:HashMap[Long,HashMap[Long,ArrayList[String]]] = new HashMap[Long,HashMap[Long,ArrayList[String]]]();

    std::map<string, long> nodes;
    std::map<long, string> nodesTemp;
    std::map<string, long> predicates;
    std::map<long, string> predicatesTemp;
    std::map<long, std::vector<string>> relationsMap;
    std::map<long, std::vector<string>> attributeMap;

};


#endif //JASMINEGRAPH_RDFPARTITIONER_H
