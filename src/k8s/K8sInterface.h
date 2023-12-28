
#ifndef JASMINEGRAPH_K8SINTERFACE_H
#define JASMINEGRAPH_K8SINTERFACE_H


#include <string>

extern "C" {
#include <kubernetes/include/apiClient.h>
#include <kubernetes/api/CoreV1API.h>
}
#include <kubernetes/config/kube_config.h>
#include <kubernetes/config/incluster_config.h>

class K8sInterface {
public:
    apiClient_t *apiClient;
    K8sInterface();
    ~K8sInterface();

    v1_pod_list_t *getPodList(char* labelSelectors);
    v1_service_list_t *getServiceList(char* labelSelectors);
};


#endif //JASMINEGRAPH_K8SINTERFACE_H
