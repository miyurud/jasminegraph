
#ifndef JASMINEGRAPH_K8SINTERFACE_H
#define JASMINEGRAPH_K8SINTERFACE_H


#include <string>

class K8sInterface {
public:
    static int deployFromDefinitionFile(const std::string& filePath);
};


#endif //JASMINEGRAPH_K8SINTERFACE_H
