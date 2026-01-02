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
#include <optional>
#include <string>
#include <vector>

enum class ExecutorType { SBS, CYPHER };

struct SBSObjective {
    std::string id;
    std::string query;
    std::string searchType;
};

struct SBSPlan {
    std::string planType;
    std::vector<SBSObjective> objectives;
};

struct DecodedPlan {
    int planId;
    std::optional<SBSPlan> sbsPlan;
};
