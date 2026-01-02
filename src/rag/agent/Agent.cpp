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

#include "Agent.h"

#include <atomic>

#include "Planner.h"
#include "Responder.h"

using json = nlohmann::json;

struct Agent::Impl {
    Planner* planner;
    Responder* responder;
};

int Agent::getUid() {
    static std::atomic<std::uint32_t> uid{0};
    return ++uid;
}

Agent::Agent(const std::string& modelName, const std::string& host, const std::string& engine) : p(new Impl()) {
    p->planner = new Planner(modelName, host, engine);
    p->responder = new Responder(modelName, host, engine);
}

Agent::~Agent() {
    delete p->planner;
    delete p;
}

std::string Agent::generatePlan(const std::string& query) {
    json plan = p->planner->build(query);
    plan["plan_id"] = getUid();
    return plan.dump();
}

std::string Agent::generateResponse(const std::string& query, const std::string& retrievedData) {
    json response = p->responder->generateResponse(query, retrievedData);
    response["response_id"] = getUid();
    return response.dump();
}
