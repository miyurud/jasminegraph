#pragma once
#include <string>

namespace LLMUtils {
std::string callLLM(const std::string& prompt, const std::string& host, const std::string& model,
                    const std::string& engine);
}
