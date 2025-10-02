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

#include "K8sInterface.h"

#include "../util/Utils.h"
#include "../util/logger/Logger.h"

// #define LOCAL_CONFIG // enable local config

Logger k8s_logger;
char *K8sInterface::namespace_ = strdup("default");

K8sInterface::K8sInterface() {
    char *basePath = NULL;
    sslConfig_t *sslConfig = NULL;
    list_t *apiKeys = NULL;

#ifdef LOCAL_CONFIG
    std::string configFile = Utils::getJasmineGraphProperty("org.jasminegraph.k8s.config");
    if (configFile.empty()) {
        configFile = "/etc/rancher/k3s/k3s.yaml";
    }
    int rc = load_kube_config(&basePath, &sslConfig, &apiKeys, configFile.c_str());
#else
    int rc = load_incluster_config(&basePath, &sslConfig, &apiKeys);
#endif
    if (rc != 0) {
        k8s_logger.error("Cannot load Kubernetes configuration.");
    }
    apiClient = apiClient_create_with_base_path(basePath, sslConfig, apiKeys);
    if (!apiClient) {
        k8s_logger.error("Cannot create a Kubernetes client.");
    }
}

K8sInterface::~K8sInterface() {
    apiClient_free(apiClient);
    apiClient = nullptr;
}

v1_deployment_list_t *K8sInterface::getDeploymentList(char *labelSelectors) {
    v1_deployment_list_t *deployment_list = NULL;
    deployment_list = AppsV1API_listNamespacedDeployment(apiClient, namespace_, /*namespace */
                                                         NULL,                  /* pretty */
                                                         NULL,                  /* allowWatchBookmarks */
                                                         NULL,                  /* continue */
                                                         NULL,                  /* fieldSelector */
                                                         labelSelectors,        /* labelSelector */
                                                         NULL,                  /* limit */
                                                         NULL,                  /* resourceVersion */
                                                         NULL,                  /* resourceVersionMatch */
                                                         NULL,                  /* sendInitialEvents */
                                                         NULL,                  /* timeoutSeconds */
                                                         NULL);                 /* watch */


    return deployment_list;
}

v1_service_list_t *K8sInterface::getServiceList(char *labelSelectors) {
    v1_service_list_t *service_list = NULL;
    service_list = CoreV1API_listNamespacedService(apiClient, namespace_, /*namespace */
                                                   NULL,                  /* pretty */
                                                   NULL,                  /* allowWatchBookmarks */
                                                   NULL,                  /* continue */
                                                   NULL,                  /* fieldSelector */
                                                   labelSelectors,        /* labelSelector */
                                                   NULL,                  /* limit */
                                                   NULL,                  /* resourceVersion */
                                                   NULL,                  /* resourceVersionMatch */
                                                   NULL,                  /* sendInitialEvents */
                                                   NULL,                  /* timeoutSeconds */
                                                   NULL);                 /* watch */

    return service_list;
}

v1_deployment_t *K8sInterface::createJasmineGraphWorkerDeployment(int workerId,
                                                                  const std::string &ip,
                                                                  const std::string &masterIp) const {
    std::string definiton = Utils::getJsonStringFromYamlFile(ROOT_DIR "/k8s/worker-deployment.yaml");
    definiton = Utils::replaceAll(definiton, "<worker-id>", std::to_string(workerId));
    definiton = Utils::replaceAll(definiton, "<master-ip>", masterIp);
    definiton = Utils::replaceAll(definiton, "<image>", Utils::getJasmineGraphProperty("org.jasminegraph.k8s.image"));
    definiton = Utils::replaceAll(definiton, "<host-name>", ip);

    cJSON *deploymentTemplate = cJSON_Parse(definiton.c_str());
    v1_deployment_t *deployment = v1_deployment_parseFromJSON(deploymentTemplate);

    v1_deployment_t *result =
        AppsV1API_createNamespacedDeployment(this->apiClient, namespace_, deployment, NULL, NULL, NULL, NULL);
    return result;
}

v1_status_t *K8sInterface::deleteJasmineGraphWorkerDeployment(int workerId) const {
    std::string deploymentName = "jasminegraph-worker" + std::to_string(workerId) + "-deployment";
    v1_status_t *result = AppsV1API_deleteNamespacedDeployment(this->apiClient, strdup(deploymentName.c_str()),
                                                               namespace_, NULL, NULL, NULL, NULL, NULL, NULL);
    return result;
}

v1_service_t *K8sInterface::createJasmineGraphWorkerService(int workerId) const {
    std::string definiton = Utils::getJsonStringFromYamlFile(ROOT_DIR "/k8s/worker-service.yaml");
    definiton = Utils::replaceAll(definiton, "<worker-id>", std::to_string(workerId));

    cJSON *serviceTemplate = cJSON_Parse(definiton.c_str());
    v1_service_t *service = v1_service_parseFromJSON(serviceTemplate);

    v1_service_t *result =
        CoreV1API_createNamespacedService(this->apiClient, namespace_, service, NULL, NULL, NULL, NULL);
    return result;
}

v1_service_t *K8sInterface::deleteJasmineGraphWorkerService(int workerId) const {
    std::string serviceName = "jasminegraph-worker" + std::to_string(workerId) + "-service";
    v1_service_t *result = CoreV1API_deleteNamespacedService(this->apiClient, strdup(serviceName.c_str()), namespace_,
                                                             NULL, NULL, NULL, NULL, NULL, NULL);
    return result;
}

v1_service_t *K8sInterface::createJasmineGraphMasterService() {
    std::string definiton = Utils::getJsonStringFromYamlFile(ROOT_DIR "/k8s/master-service.yaml");

    cJSON *serviceTemplate = cJSON_Parse(definiton.c_str());
    v1_service_t *service = v1_service_parseFromJSON(serviceTemplate);

    v1_service_t *result =
        CoreV1API_createNamespacedService(this->apiClient, namespace_, service, NULL, NULL, NULL, NULL);
    return result;
}

v1_service_t *K8sInterface::deleteJasmineGraphMasterService() {
    std::string serviceName = "jasminegraph-master-service";
    v1_service_t *result = CoreV1API_deleteNamespacedService(this->apiClient, strdup(serviceName.c_str()), namespace_,
                                                             NULL, NULL, NULL, NULL, NULL, NULL);
    return result;
}

std::string K8sInterface::getMasterIp() {
    v1_service_t *service =
        CoreV1API_readNamespacedService(apiClient, strdup("jasminegraph-master-service"), namespace_, NULL);
    if (service->metadata->name == NULL) {
        k8s_logger.warn("No master service.");
        return "";
    }
    return service->spec->cluster_ip;
}

v1_node_list_t *K8sInterface::getNodes() {
    v1_node_list_t *node_list = NULL;
    node_list = CoreV1API_listNode(apiClient,
                                   NULL,                  /* pretty */
                                   NULL,                  /* allowWatchBookmarks */
                                   NULL,                  /* continue */
                                   NULL,                  /* fieldSelector */
                                   NULL,        /* labelSelector */
                                   NULL,                  /* limit */
                                   NULL,                  /* resourceVersion */
                                   NULL,                  /* resourceVersionMatch */
                                   NULL,                  /* sendInitialEvents */
                                   NULL,                  /* timeoutSeconds */
                                   NULL);                 /* watch */
    return node_list;
}

std::string K8sInterface::loadFromConfig(std::string key) {
    v1_config_map_t *config_map =
        CoreV1API_readNamespacedConfigMap(apiClient, strdup("jasminegraph-config"), namespace_, NULL);
    if (config_map->metadata->name == NULL) {
        k8s_logger.error("No jasminegraph-config config map.");
        return "";
    }

    listEntry_t *data = NULL;
    list_ForEach(data, config_map->data) {
        auto *pair = static_cast<keyValuePair_t *>(data->data);
        if (strcmp(pair->key, key.c_str()) == 0) {
            return std::string(static_cast<char *>(pair->value));
        }
    }
    return "";
}

static std::map<string, string> configMap;
static std::mutex configMapMutex;
std::string K8sInterface::getJasmineGraphConfig(std::string key) {
    auto item = configMap.find(key);
    if (item == configMap.end()) {
        configMapMutex.lock();
        if (configMap.find(key) == configMap.end()) {
            k8s_logger.info("Key " + key + " not found in cache. Loading from config.");
            configMap[key] = loadFromConfig(key);
        }
        configMapMutex.unlock();
        item = configMap.find(key);
    }
    return item->second;
}

v1_persistent_volume_t *K8sInterface::createJasmineGraphPersistentVolume(int workerId) const {
    std::string definition = Utils::getJsonStringFromYamlFile(ROOT_DIR "/k8s/worker-volume.yaml");
    definition = Utils::replaceAll(definition, "<worker-id>", std::to_string(workerId));
    cJSON *volumeTemplate = cJSON_Parse(definition.c_str());
    v1_persistent_volume_t *volume = v1_persistent_volume_parseFromJSON(volumeTemplate);
    v1_persistent_volume_t *result = CoreV1API_createPersistentVolume(apiClient, volume, NULL, NULL, NULL, NULL);
    return result;
}

v1_persistent_volume_claim_t *K8sInterface::createJasmineGraphPersistentVolumeClaim(int workerId) const {
    std::string definition = Utils::getJsonStringFromYamlFile(ROOT_DIR "/k8s/worker-volume-claim.yaml");
    definition = Utils::replaceAll(definition, "<worker-id>", std::to_string(workerId));
    cJSON *volumeClaimTemplate = cJSON_Parse(definition.c_str());
    v1_persistent_volume_claim_t *volumeClaim = v1_persistent_volume_claim_parseFromJSON(volumeClaimTemplate);
    v1_persistent_volume_claim_t *result = CoreV1API_createNamespacedPersistentVolumeClaim(apiClient,
                                                                                           namespace_,
                                                                                           volumeClaim,
                                                                                           NULL,
                                                                                           NULL,
                                                                                           NULL,
                                                                                           NULL);
    return result;
}

v1_persistent_volume_t *K8sInterface::deleteJasmineGraphPersistentVolume(int workerId) const {
    std::string volumeName = "jasminegraph-worker" + std::to_string(workerId) + "-data";
    v1_persistent_volume_t *result = CoreV1API_deletePersistentVolume(apiClient, strdup(volumeName.c_str()), NULL, NULL,
                                                                      NULL, NULL, NULL, NULL);
    return result;
}

v1_persistent_volume_claim_t *K8sInterface::deleteJasmineGraphPersistentVolumeClaim(int workerId) const {
    std::string volumeClaimName = "jasminegraph-worker" + std::to_string(workerId) + "-data-claim";
    v1_persistent_volume_claim_t *result =
        CoreV1API_deleteNamespacedPersistentVolumeClaim(apiClient,
                                                        strdup(volumeClaimName.c_str()),
                                                        namespace_,
                                                        NULL, NULL, NULL, NULL, NULL,
                                                        NULL);
    return result;
}
