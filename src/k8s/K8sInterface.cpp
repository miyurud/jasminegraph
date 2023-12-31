
#include "K8sInterface.h"

#include <unistd.h>
#include <sys/stat.h>

#include "../util/Utils.h"
#include "../util/logger/Logger.h"

Logger k8s_logger;

int K8sInterface::deployFromDefinitionFile(const std::string &filePath) {
    std::string command = "kubectl apply -f " + filePath;
    int result = std::system(command.c_str());
    if (result == 0) {
        k8s_logger.info("Deployed successfully");
    } else {
        k8s_logger.error("Error in deploying");
    }
    return result;
}
