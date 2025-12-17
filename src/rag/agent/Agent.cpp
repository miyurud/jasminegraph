#include "Agent.h"
#include "Planner.h"

#include <atomic>

using json = nlohmann::json;

struct Agent::Impl {
    Planner* planner;
};

int Agent::getUid() {
    static std::atomic<std::uint32_t> uid{0};
    return ++uid;
}

Agent::Agent(const std::string& modelName, const std::string& host) 
    : p(new Impl()) 
{
    p->planner = new Planner(modelName, host);
}

Agent::~Agent() {
    delete p->planner;
    delete p;
}

std::string Agent::generatePlan(const std::string &query) {
    json plan = p->planner->build(query);
    plan["plan_id"] = getUid();
    return plan.dump();
}