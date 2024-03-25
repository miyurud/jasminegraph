/**
Copyright 2024 JasmineGraph Team
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

#ifndef JASMINEGRAPH_K8SINTERFACE_H
#define JASMINEGRAPH_K8SINTERFACE_H

#include <string>

extern "C" {
#include <kubernetes/api/AppsV1API.h>
#include <kubernetes/api/CoreV1API.h>
#include <kubernetes/include/apiClient.h>
}

#include <kubernetes/config/incluster_config.h>
#include <kubernetes/config/kube_config.h>

class K8sInterface {
 public:
    static char *namespace_;
    apiClient_t *apiClient;

    K8sInterface();

    ~K8sInterface();

    v1_service_list_t *getServiceList(char *labelSelectors);

    v1_deployment_t *createJasmineGraphWorkerDeployment(int workerId,
                                                        const std::string &ip,
                                                        const std::string &masterIp) const;

    v1_status_t *deleteJasmineGraphWorkerDeployment(int workerId) const;

    v1_service_t *createJasmineGraphWorkerService(int workerId) const;

    v1_service_t *deleteJasmineGraphWorkerService(int workerId) const;

    v1_deployment_list_t *getDeploymentList(char *labelSelectors);

    v1_service_t *createJasmineGraphMasterService();

    v1_service_t *deleteJasmineGraphMasterService();

    std::string getMasterIp();

    v1_node_list_t *getNodes();

    std::string getJasmineGraphConfig(std::string key);

    v1_persistent_volume_t *createJasmineGraphPersistentVolume(int workerId) const;

    v1_persistent_volume_claim_t *createJasmineGraphPersistentVolumeClaim(int workerId) const;

    v1_persistent_volume_t *deleteJasmineGraphPersistentVolume(int workerId) const;

    v1_persistent_volume_claim_t *deleteJasmineGraphPersistentVolumeClaim(int workerId) const;

private:
    std::string loadFromConfig(std::string key);
};

#endif  // JASMINEGRAPH_K8SINTERFACE_H
