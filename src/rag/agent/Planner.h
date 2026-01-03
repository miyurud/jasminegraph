/**
Copyright 2025 JasmineGraph Team
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

#pragma once
#include <nlohmann/json.hpp>
#include <string>

class Planner {
 public:
    Planner(const std::string& modelName, const std::string& host, const std::string& engine);
    ~Planner();

    nlohmann::json build(const std::string& query);

 private:
    std::string model;
    std::string host;
    std::string engine;

    nlohmann::json buildSemanticBeamSearchPlan(const std::string& query);
};
