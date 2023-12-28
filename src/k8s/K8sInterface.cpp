
#include "K8sInterface.h"

#include "../util/logger/Logger.h"
#include "../util/Utils.h"

//#define DEBUG // enable debug logging
//#define LOCAL_CONFIG // enable local config

Logger k8s_logger;

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

v1_pod_list_t *K8sInterface::getPodList(char* labelSelectors) {
    v1_pod_list_t *pod_list = NULL;
    pod_list = CoreV1API_listNamespacedPod(apiClient,
                                           "default",    /*namespace */
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
                                           NULL     /* watch */
    );

#ifdef DEBUG
    if(pod_list) {
        std::string pod_names;
        listEntry_t *listEntry = NULL;
        v1_pod_t *pod = NULL;
        list_ForEach(listEntry, pod_list->items) {
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

    return pod_list;
}

v1_service_list_t *K8sInterface::getServiceList(char* labelSelectors) {
    v1_service_list_t *service_list = NULL;
    service_list = CoreV1API_listNamespacedService(apiClient,
                                           "default",    /*namespace */
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
                                           NULL     /* watch */
    );

#ifdef DEBUG
    if(service_list) {
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