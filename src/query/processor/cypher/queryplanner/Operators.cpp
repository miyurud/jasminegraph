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

#include "Operators.h"
#include <nlohmann/json.hpp>
#include <map>
#include "../util/Const.h"
#include "../astbuilder/ASTNode.h"
using namespace std;

using json = nlohmann::json;

// NodeScan Implementation
NodeScanByLabel::NodeScanByLabel(string label, string var) : label(label), var(var) {}

string NodeScanByLabel::execute() {
    json nodeByLabel;
    nodeByLabel["Operator"] = "NodeByLabel";
    nodeByLabel["variables"] = var;
    nodeByLabel["Label"] = label;
    return nodeByLabel.dump();
}

// MultipleNodeScanByLabel Implementation
MultipleNodeScanByLabel::MultipleNodeScanByLabel(vector<string> label, const string& var) : label(label), var(var) {}

string MultipleNodeScanByLabel::execute() {

    json multipleNodeByLabel;
    multipleNodeByLabel["Operator"] = "MultipleNodeScanByLabel";
    multipleNodeByLabel["variables"] = var;
    multipleNodeByLabel["Label"] = label;
    return multipleNodeByLabel.dump();
}

// AllNodeScan Implementation
AllNodeScan::AllNodeScan(const string& var) : var(var) {}

string AllNodeScan::execute() {
    json allNodeScan;
    allNodeScan["Operator"] = "AllNodeScan";
    allNodeScan["variables"] = var;
    return allNodeScan.dump();
}

// ProduceResults Implementation
ProduceResults::ProduceResults(Operator* opr, vector<ASTNode*> item) : item(item), op(opr) {}

string ProduceResults::execute() {
    json produceResult;
    produceResult["Operator"] = "ProduceResult";
    if(op){
        produceResult["NextOperator"] = op->execute();
    }
    for(auto* e: item) {
        produceResult["variable"] = e->value;
    }
    return produceResult.dump();
}

// Filter Implementation
Filter::Filter(Operator* input, vector<pair<string,ASTNode*>> filterCases) : input(input), filterCases(filterCases) {}

string Filter::comparisonOperand(ASTNode *ast) {
    json operand;
    if (ast->nodeType == Const::NON_ARITHMETIC_OPERATOR) {
        // there are more cases to handle
        operand["variable"] = ast->elements[0]->value;
        operand["type"] = Const::PROPERTY_LOOKUP;
        vector<string> property;
        for (auto* prop: ast->elements) {
            if (prop->nodeType == Const::PROPERTY_LOOKUP && prop->elements[0]->nodeType != Const::RESERVED_WORD) {
                property.push_back(prop->elements[0]->value);
            }
        }
        operand["property"] = property;
    } else if (ast->nodeType == Const::PROPERTIES_MAP) {
        operand["type"] = Const::PROPERTIES_MAP;
        map<string, string> property;
        for (auto* prop: ast->elements) {
            if (prop->elements[0]->nodeType != Const::RESERVED_WORD){
                property.insert(pair<string, string>(prop->elements[0]->value, prop->elements[1]->value));
            }
        }
        operand["property"] = property;
    } else if (ast->nodeType == Const::LIST) {
        operand["type"] = Const::LIST;
        vector<string> element;
        for (auto* prop: ast->elements) {
            element.push_back(prop->value);
        }

        operand["element"] = element;
    } else {
        operand["type"] = ast->nodeType;
        operand["value"] = ast->value;
    }

    return operand.dump();
}

string Filter::analyze(ASTNode* ast) {
    json where;
    if (ast->nodeType == Const::OR) {
        where["type"] = Const::OR;
        vector<json> comparisons;
        for (auto* element: ast->elements) {
            comparisons.push_back(json::parse(analyze(element)));
        }
        where["comparisons"] = comparisons;
    } else if(ast->nodeType == Const::AND) {
        where["type"] = Const::AND;
        vector<json> comparisons;
        for (auto* element: ast->elements) {
            comparisons.push_back(json::parse(analyze(element)));
        }
        where["comparisons"] = comparisons;
    } else if(ast->nodeType == Const::XOR) {
        where["type"] = Const::XOR;
        vector<string> comparisons;
        for (auto* element: ast->elements) {
            comparisons.push_back(json::parse(analyze(element)));
        }
        where["comparisons"] = comparisons;
    } else if(ast->nodeType == Const::NOT) {
        where["type"] = Const::NOT;
        vector<json> comparisons;
        for (auto* element: ast->elements) {
            comparisons.push_back(json::parse(analyze(element)));
        }
        where["comparisons"] = comparisons;
    } else if(ast->nodeType == Const::COMPARISON) {
        where["type"] = Const::COMPARISON;
        auto* left = ast->elements[0];
        auto* oparator = ast->elements[1];
        auto* right = oparator->elements[0];
        where["left"] = json::parse(comparisonOperand(left));
        where["operator"] = oparator->nodeType;
        where["right"] = json::parse(comparisonOperand(right));
    }
    return where.dump();
}

string Filter::execute() {
    json filter;
    if (input) {
        filter["NextOperator"] = input->execute();
    }
    filter["Operator"] = "Filter";
    string condition;
    for(auto item: filterCases){

        if(item.second->nodeType==Const::WHERE){
            condition = analyze(item.second->elements[0]);
            filter["condition"] = json::parse(condition);
        }else if(item.second->nodeType==Const::PROPERTIES_MAP){
            for(auto* prop: item.second->elements){
                condition+=item.first+"."+prop->elements[0]->value+" = "+prop->elements[1]->value;
                if(prop != item.second->elements.back()){
                    condition+=" AND \n";
                }
            }
        }else if(item.second->nodeType==Const::NODE_LABELS){
            for(auto* prop: item.second->elements){
                condition+=item.first+": "+prop->elements[0]->value;
                if(prop != item.second->elements.back()){
                    condition+=" AND \n";
                }
            }
        }else if(item.second->nodeType==Const::NODE_LABEL){
            condition = item.first+": "+item.second->elements[0]->value;
        }

        if(item != filterCases.back()){
            condition+=" AND \n";
        }
    }
    return filter.dump();
}

// Projection Implementation
Projection::Projection(Operator* input, const vector<ASTNode*> columns) : input(input), columns(columns) {}

string Projection::execute() {
    input->execute();
    cout<<"Projection"<<endl;
    for (auto* col : columns) {
        cout << col->print() << endl;
    }
    return "";
}

// Join Implementation
Join::Join(Operator* left, Operator* right, const string& joinCondition) : left(left), right(right), joinCondition(joinCondition) {}

string Join::execute() {
    left->execute();
    right->execute();
    cout << "Joining on condition: " << joinCondition << endl;
    return "";
}

// Aggregation Implementation
Aggregation::Aggregation(Operator* input, const string& aggFunction, const string& column) : input(input), aggFunction(aggFunction), column(column) {}

string Aggregation::execute() {
    input->execute();
    cout << "Performing aggregation: " << aggFunction << " on column: " << column << endl;
    return "";
}

// Limit Implementation
Limit::Limit(Operator* input, int limit) : input(input), limit(limit) {}

string Limit::execute() {
    input->execute();
    cout << "Limiting result to " << limit << " rows." << endl;
    return "";
}

// Sort Implementation
Sort::Sort(Operator* input, const string& sortByColumn, bool ascending) : input(input), sortByColumn(sortByColumn), ascending(ascending) {}

string Sort::execute() {
    input->execute();
    cout << "Sorting by column: " << sortByColumn << " in " << (ascending ? "ascending" : "descending") << " order." << endl;
    return "";
}

// GroupBy Implementation
GroupBy::GroupBy(Operator* input, const vector<  string>& groupByColumns) : input(input), groupByColumns(groupByColumns) {}

string GroupBy::execute() {
    input->execute();
    cout << "Grouping by columns: ";
    for (const auto& col : groupByColumns) {
        cout << col << " ";
    }
    cout << endl;
    return "";
}

// Distinct Implementation
Distinct::Distinct(Operator* input) : input(input) {}

string Distinct::execute() {
    input->execute();
    cout << "Applying Distinct to remove duplicates." << endl;
    return "";
}

// Union Implementation
Union::Union(Operator* left, Operator* right) : left(left), right(right) {}

string Union::execute() {
    left->execute();
    right->execute();
    cout << "Performing Union of results." << endl;
    return "";
}

// Intersection Implementation
Intersection::Intersection(Operator* left, Operator* right) : left(left), right(right) {}

string Intersection::execute() {
    left->execute();
    right->execute();
    cout << "Performing Intersection of results." << endl;
    return "";
}

CacheProperty::CacheProperty(Operator* input, vector<ASTNode*> property) : property(property), input(input){}

string CacheProperty::execute()
{
    input->execute();
    cout<<"CacheProperty: \n"<<endl;

    int i=1;
    for(auto* prop: property)
    {
        string s = prop->elements[0]->value +"."+prop->elements[1]->elements[0]->value;
        cout<<"get property "<<i++<<" and cache here: "<<s<<endl;

    }
    return "";
}

UndirectedRelationshipTypeScan::UndirectedRelationshipTypeScan(string relType, string relvar, string startVar, string endVar)
        : relType(relType), relvar(relvar), startVar(startVar), endVar(endVar) {}

// Execute method
string UndirectedRelationshipTypeScan::execute() {
    cout<<"UndirectedRelationshipTypeScan: \n"<<endl;
    cout << "("<<startVar<<") -[" << relvar<<" :"<< relType << "]- (" << endVar << ")" << endl;
    return "";
}

UndirectedAllRelationshipScan::UndirectedAllRelationshipScan(string startVar, string endVar, string relVar)
        : startVar(startVar), endVar(endVar), relVar(relVar) {}


string UndirectedAllRelationshipScan::execute() {
    cout<<"UndirectedRelationshipTypeScan: \n"<<endl;
    cout << "("<<startVar<<") -[" << relVar<<"]- (" << endVar << ")" << endl;
    return "";
}

DirectedRelationshipTypeScan::DirectedRelationshipTypeScan(string direction, string relType, string relvar, string startVar, string endVar)
        : relType(relType), relvar(relvar), startVar(startVar), endVar(endVar), direction(direction) {}


// Execute method
string DirectedRelationshipTypeScan::execute() {
    cout<<"DirectedRelationshipTypeScan: \n"<<endl;

    if(direction == "right"){
        cout << "("<<startVar<<") -[" << relvar<<" :"<< relType << "]-> (" << endVar << ")" << endl;
    }else{
        cout << "("<<startVar<<") <-[" << relvar<<" :"<< relType << "]- (" << endVar << ")" << endl;
    }
    return "";
}

DirectedAllRelationshipScan::DirectedAllRelationshipScan(std::string direction, std::string startVar, std::string endVar, std::string relVar)
        : startVar(startVar), endVar(endVar), relVar(relVar), direction(direction) {}

string DirectedAllRelationshipScan::execute() {
    cout<<"DirectedAllRelationshipScan: \n"<<endl;

    if(direction == "right"){
        cout << "("<<startVar<<") -[" << relVar<<"]-> (" << endVar << ")" << endl;
    }else{
        cout << "("<<startVar<<") <-[" << relVar<<"]- (" << endVar << ")" << endl;
    }
    return "";
}

ExpandAll::ExpandAll(Operator *input, std::string startVar, std::string destVar, std::string relVar,
                     std::string relType, std::string direction): input(input), relType(relType), relVar(relVar), startVar(startVar), destVar(destVar), direction(direction){}

string ExpandAll::execute() {
    input->execute();
    cout<<"ExpandAll: \n"<<endl;

    string line;

    if(relType == "null" && direction == ""){
        line = "("+startVar+") -["+relVar+"]- ("+destVar+")";
        cout<<line<<endl;
    }else if(relType != "null" && direction == ""){
        line = "("+startVar+") -["+relVar+" :"+relType+"]- ("+destVar+")";
        cout<<line<<endl;
    }else if(relType == "null" && direction == "right") {
        cout << "(" << startVar << ") -[" << relVar << "]-> (" << destVar << ")" << endl;
    }else if(relType != "null" && direction == "right"){
        cout << "(" << startVar << ") -[" << relVar << " :" << relType << "]-> (" << destVar << ")" << endl;
    }else if(relType == "null" && direction == "left") {
        cout << "(" << startVar << ") <-[" << relVar << "]- (" << destVar << ")" << endl;
    }else if(relType != "null" && direction == "left"){
        cout << "(" << startVar << ") <-[" << relVar << " :" << relType << "]- (" << destVar << ")" << endl;
    }
    return "";
}

Apply::Apply(Operator* opr): opr1(opr){}

void Apply::addOperator(Operator *opr) {
    this->opr2 = opr;
}


// Execute method
string Apply::execute() {
    if (opr1 != nullptr){
        cout<<"     left of Apply"<<endl;
        opr1->execute();
    }
    if (opr2 != nullptr){
        cout<<"     right of Apply"<<endl;
        opr2->execute();
    }

    cout<<"Apply: result merged"<<endl;
    return "";
}

