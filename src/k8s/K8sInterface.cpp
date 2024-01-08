
#include "K8sInterface.h"

#include "../util/logger/Logger.h"
#include "../util/Utils.h"

// #define DEBUG // enable debug logging
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
        k8s_logger.error("Cannot load kubernetes configuration.");
    }
    apiClient = apiClient_create_with_base_path(basePath, sslConfig, apiKeys);
    if (!apiClient) {
        k8s_logger.error("Cannot create a kubernetes client.");
    }
}

K8sInterface::~K8sInterface() {
    apiClient_free(apiClient);
    apiClient = nullptr;
}

v1_deployment_list_t *K8sInterface::getDeploymentList(char *labelSelectors) {
    v1_deployment_list_t *deployment_list = NULL;
    deployment_list = AppsV1API_listNamespacedDeployment(apiClient,
                                                         namespace_,    /*namespace */
                                                         NULL,    /* pretty */
                                                         NULL,    /* allowWatchBookmarks */
                                                         NULL,    /* continue */
                                                         NULL,    /* fieldSelector */
                                                         labelSelectors,    /* labelSelector */
                                                         NULL,    /* limit */
                                                         NULL,    /* resourceVersion */
                                                         NULL,    /* resourceVersionMatch */
                                                         NULL,    /* sendInitialEvents */
                                                         NULL,    /* timeoutSeconds */
                                                         NULL);   /* watch */

#ifdef DEBUG
    if (deployment_list) {
        std::string pod_names;
        listEntry_t *listEntry = NULL;
        v1_pod_t *pod = NULL;
        list_ForEach(listEntry, deployment_list->items) {
            pod = static_cast<v1_pod_t *>(listEntry->data);
            pod_names += pod->metadata->name;
        }
        if (pod_names.empty()) {
            k8s_logger.info("No pods found.");
        } else {
            k8s_logger.info("Available pod list: " + pod_names);
        }
    } else {
        k8s_logger.info("Api client response code :" + std::to_string(apiClient->response_code));
        k8s_logger.error("Error in getting pod list.");
    }
#endif

    return deployment_list;
}

v1_service_list_t *K8sInterface::getServiceList(char *labelSelectors) {
    v1_service_list_t *service_list = NULL;
    service_list = CoreV1API_listNamespacedService(apiClient,
                                                   namespace_,    /*namespace */
                                                   NULL,    /* pretty */
                                                   NULL,    /* allowWatchBookmarks */
                                                   NULL,    /* continue */
                                                   NULL,    /* fieldSelector */
                                                   labelSelectors,    /* labelSelector */
                                                   NULL,    /* limit */
                                                   NULL,    /* resourceVersion */
                                                   NULL,    /* resourceVersionMatch */
                                                   NULL,    /* sendInitialEvents */
                                                   NULL,    /* timeoutSeconds */
                                                   NULL);  /* watch */

#ifdef DEBUG
    if (service_list) {
        std::string service_names;
        listEntry_t *listEntry = NULL;
        v1_service_t *service = NULL;
        list_ForEach(listEntry, service_list->items) {
            service = static_cast<v1_service_t *>(listEntry->data);
            service_names += service->metadata->name;
        }
        if (service_names.empty()) {
            k8s_logger.info("No services found.");
        } else {
            k8s_logger.info("Available service list: " + service_names);
        }
    } else {
        k8s_logger.error("Error in getting service list.");
    }
#endif

    return service_list;
}

v1_deployment_t *K8sInterface::createJasmineGraphWorkerDeployment(int workerId, const std::string &masterIp) const {
    std::string definiton = Utils::getJsonStringFromYamlFile(ROOT_DIR"/k8s/worker-deployment.yaml");
    definiton = Utils::replaceAll(definiton, "<worker-id>", std::to_string(workerId));
    definiton = Utils::replaceAll(definiton, "<master-ip>", masterIp);
    definiton = Utils::replaceAll(definiton, "<image>",
                                  Utils::getJasmineGraphProperty("org.jasminegraph.k8s.image"));

    cJSON *deploymentTemplate = cJSON_Parse(definiton.c_str());
    v1_deployment_t *deployment = v1_deployment_parseFromJSON(deploymentTemplate);

    v1_deployment_t *result = AppsV1API_createNamespacedDeployment(this->apiClient,
                                                                   namespace_,
                                                                   deployment,
                                                                   NULL,
                                                                   NULL,
                                                                   NULL,
                                                                   NULL);
    return result;
}

v1_status_t *K8sInterface::deleteJasmineGraphWorkerDeployment(int workerId) const {
    std::string deploymentName = "jasminegraph-worker" + std::to_string(workerId) + "-deployment";
    v1_status_t *result = AppsV1API_deleteNamespacedDeployment(this->apiClient,
                                                               strdup(deploymentName.c_str()),
                                                               namespace_,
                                                               NULL,
                                                               NULL,
                                                               NULL,
                                                               NULL,
                                                               NULL,
                                                               NULL);
    return result;
}

v1_service_t *K8sInterface::createJasmineGraphWorkerService(int workerId) const {
    std::string definiton = Utils::getJsonStringFromYamlFile(ROOT_DIR"/k8s/worker-service.yaml");
    definiton = Utils::replaceAll(definiton, "<worker-id>", std::to_string(workerId));

    cJSON *serviceTemplate = cJSON_Parse(definiton.c_str());
    v1_service_t *service = v1_service_parseFromJSON(serviceTemplate);

    v1_service_t *result = CoreV1API_createNamespacedService(this->apiClient,
                                                             namespace_,
                                                             service,
                                                             NULL,
                                                             NULL,
                                                             NULL,
                                                             NULL);
    return result;
}

v1_service_t *K8sInterface::deleteJasmineGraphWorkerService(int workerId) const {
    std::string serviceName = "jasminegraph-worker" + std::to_string(workerId) + "-service";
    v1_service_t *result = CoreV1API_deleteNamespacedService(this->apiClient,
                                                             strdup(serviceName.c_str()),
                                                             namespace_,
                                                             NULL,
                                                             NULL,
                                                             NULL,
                                                             NULL,
                                                             NULL,
                                                             NULL);
    return result;
}

v1_service_t *K8sInterface::createJasmineGraphMasterService() {
    std::string definiton = Utils::getJsonStringFromYamlFile(ROOT_DIR"/k8s/master-service.yaml");

    cJSON *serviceTemplate = cJSON_Parse(definiton.c_str());
    v1_service_t *service = v1_service_parseFromJSON(serviceTemplate);

    v1_service_t *result = CoreV1API_createNamespacedService(this->apiClient,
                                                             namespace_,
                                                             service,
                                                             NULL,
                                                             NULL,
                                                             NULL,
                                                             NULL);
    return result;
}

v1_service_t *K8sInterface::deleteJasmineGraphMasterService() {
    std::string serviceName = "jasminegraph-master-service";
    v1_service_t *result = CoreV1API_deleteNamespacedService(this->apiClient,
                                                             strdup(serviceName.c_str()),
                                                             namespace_,
                                                             NULL,
                                                             NULL,
                                                             NULL,
                                                             NULL,
                                                             NULL,
                                                             NULL);
    return result;
}

std::string K8sInterface::getMasterIp() {
    v1_service_t *service = CoreV1API_readNamespacedService(apiClient,
                                                            strdup("jasminegraph-master-service"),
                                                            namespace_,
                                                            NULL);
    if (service->metadata->name == NULL) {
        k8s_logger.warn("No master service.");
        return "";
    }
    return service->spec->cluster_ip;
}
