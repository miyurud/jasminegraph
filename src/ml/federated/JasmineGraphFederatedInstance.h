#ifndef JASMINEGRAPH_JASMINGRAPHFEDERATEDLEARNINGINSTANCE_H
#define JASMINEGRAPH_JASMINGRAPHFEDERATEDLEARNINGINSTANCE_H

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <map>
#include "../../util/Utils.h"
#include <iostream>

//std::vector<Utils::worker> Utils::getWorkerList(SQLiteDBInterface sqlite)

class JasmineGraphFederatedInstance {
public:

    void initiateFiles(std::string graphID, std::string trainingArgs);
    void initiateCommunication(std::string graphID, std::string trainingArgs, SQLiteDBInterface sqlite);
    void initiateMerge(std::string graphID, std::string trainingArgs,SQLiteDBInterface sqlite);
    static bool initiateTrain(std::string host, int port, int dataPort,std::string trainingArgs,int iteration, string partCount);    
    static bool initiateServer(std::string host, int port, int dataPort,std::string trainingArgs,int iteration, string partCount);
    static bool initiateClient(std::string host, int port, int dataPort,std::string trainingArgs,int iteration, string partCount);
    static bool mergeFiles(std::string host, int port, int dataPort,std::string trainingArgs,int iteration, string partCount);
    
};
const int FED_DATA_LENGTH = 300;

#endif //JASMINEGRAPH_JASMINGRAPHFEDERATEDLEARNINGINSTANCE_H