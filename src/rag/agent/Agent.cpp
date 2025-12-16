#include "Agent.h"
#include "Planner.h"
#include <uuid/uuid.h>

using json = nlohmann::json;

struct Agent::Impl {
    Planner* planner;
};

static std::string make_uuid() {
    uuid_t id;
    uuid_generate(id);
    char str[37];
    uuid_unparse_lower(id, str);
    return std::string(str);
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
    plan["plan_id"] = make_uuid();
    return plan.dump();
}