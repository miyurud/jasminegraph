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
#include "../util/Const.h"
#include "../astbuilder/ASTNode.h"
using namespace std;

// NodeScan Implementation
NodeScanByLabel::NodeScanByLabel(string label, string var) : label(label), var(var) {}

void NodeScanByLabel::execute() {
    cout<<"NodeScanByLabel: \n"<<endl;
    cout << "scanning node based on single label: " << label <<" and assigned with variable: "<<var<< endl;
}

// MultipleNodeScanByLabel Implementation
MultipleNodeScanByLabel::MultipleNodeScanByLabel(vector<string> label, const string& var) : label(label), var(var) {}

void MultipleNodeScanByLabel::execute() {
    cout<<"MultipleNodeScanByLabel: \n"<<endl;

    string label_string = "";
    for(int i=0; i<label.size();i++)
    {
        label_string+= label[i];
        label_string+= ", ";
    }
    cout << "scanning node based on multiple label: " << label_string <<" and assigned with variable: "<<var<< endl;

}

// AllNodeScan Implementation
AllNodeScan::AllNodeScan(const string& var) : var(var) {}

void AllNodeScan::execute() {
    cout<<"AllNodeScan: \n"<<endl;
    cout << "scanning all nodes and assigned with : " << var << endl;
}

// ProduceResults Implementation
ProduceResults::ProduceResults(Operator* opr, vector<ASTNode*> item) : item(item), op(opr) {}

void ProduceResults::execute() {
    op->execute();
    cout<<"ProduceResults: \n"<<endl;

    for(auto* e: item)
    {
        cout<<"Return variable: "<< e->value<<endl;
    }
}

// Filter Implementation
Filter::Filter(Operator* input, vector<pair<string,ASTNode*>> filterCases) : input(input), filterCases(filterCases) {}

void Filter::execute() {
    input->execute();
    cout<<"Filter: \n"<<endl;
    string condition;
    for(auto item: filterCases){
        if(item.second->nodeType==Const::WHERE){
            cout<<item.second->print(1)<<endl;

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
    cout<<condition<<endl;

}

// Projection Implementation
Projection::Projection(Operator* input, const vector<ASTNode*> columns) : input(input), columns(columns) {}

void Projection::execute() {
    input->execute();
    cout<<"Projection: \n"<<endl;

    cout << "Projecting columns: ";
    for (auto* col : columns) {
        cout << col->nodeType << " ";
    }
    cout << endl;
}

// Join Implementation
Join::Join(Operator* left, Operator* right, const string& joinCondition) : left(left), right(right), joinCondition(joinCondition) {}

void Join::execute() {
    left->execute();
    right->execute();
    cout << "Joining on condition: " << joinCondition << endl;
}

// Aggregation Implementation
Aggregation::Aggregation(Operator* input, const string& aggFunction, const string& column) : input(input), aggFunction(aggFunction), column(column) {}

void Aggregation::execute() {
    input->execute();
    cout << "Performing aggregation: " << aggFunction << " on column: " << column << endl;
}

// Limit Implementation
Limit::Limit(Operator* input, int limit) : input(input), limit(limit) {}

void Limit::execute() {
    input->execute();
    cout << "Limiting result to " << limit << " rows." << endl;
}

// Sort Implementation
Sort::Sort(Operator* input, const string& sortByColumn, bool ascending) : input(input), sortByColumn(sortByColumn), ascending(ascending) {}

void Sort::execute() {
    input->execute();
    cout << "Sorting by column: " << sortByColumn << " in " << (ascending ? "ascending" : "descending") << " order." << endl;
}

// GroupBy Implementation
GroupBy::GroupBy(Operator* input, const vector<  string>& groupByColumns) : input(input), groupByColumns(groupByColumns) {}

void GroupBy::execute() {
    input->execute();
    cout << "Grouping by columns: ";
    for (const auto& col : groupByColumns) {
        cout << col << " ";
    }
    cout << endl;
}

// Distinct Implementation
Distinct::Distinct(Operator* input) : input(input) {}

void Distinct::execute() {
    input->execute();
    cout << "Applying Distinct to remove duplicates." << endl;
}

// Union Implementation
Union::Union(Operator* left, Operator* right) : left(left), right(right) {}

void Union::execute() {
    left->execute();
    right->execute();
    cout << "Performing Union of results." << endl;
}

// Intersection Implementation
Intersection::Intersection(Operator* left, Operator* right) : left(left), right(right) {}

void Intersection::execute() {
    left->execute();
    right->execute();
    cout << "Performing Intersection of results." << endl;
}

CacheProperty::CacheProperty(Operator* input, vector<ASTNode*> property) : property(property), input(input){}

void CacheProperty::execute()
{
    input->execute();
    cout<<"CacheProperty: \n"<<endl;

    int i=1;
    for(auto* prop: property)
    {
        string s = prop->elements[0]->value +"."+prop->elements[1]->elements[0]->value;
        cout<<"get property "<<i++<<" and cache here: "<<s<<endl;

    }
}

UndirectedRelationshipTypeScan::UndirectedRelationshipTypeScan(string relType, string relvar, string startVar, string endVar)
        : relType(relType), relvar(relvar), startVar(startVar), endVar(endVar) {}

// Execute method
void UndirectedRelationshipTypeScan::execute() {
    cout<<"UndirectedRelationshipTypeScan: \n"<<endl;
    cout << "("<<startVar<<") -[" << relvar<<" :"<< relType << "]- (" << endVar << ")" << endl;

}

UndirectedAllRelationshipScan::UndirectedAllRelationshipScan(string startVar, string endVar, string relVar)
        : startVar(startVar), endVar(endVar), relVar(relVar) {}


void UndirectedAllRelationshipScan::execute() {
    cout<<"UndirectedRelationshipTypeScan: \n"<<endl;
    cout << "("<<startVar<<") -[" << relVar<<"]- (" << endVar << ")" << endl;
}

DirectedRelationshipTypeScan::DirectedRelationshipTypeScan(string direction, string relType, string relvar, string startVar, string endVar)
        : relType(relType), relvar(relvar), startVar(startVar), endVar(endVar), direction(direction) {

}


// Execute method
void DirectedRelationshipTypeScan::execute() {
    cout<<"DirectedRelationshipTypeScan: \n"<<endl;

    if(direction == "right"){
        cout << "("<<startVar<<") -[" << relvar<<" :"<< relType << "]-> (" << endVar << ")" << endl;
    }else{
        cout << "("<<startVar<<") <-[" << relvar<<" :"<< relType << "]- (" << endVar << ")" << endl;
    }
}

DirectedAllRelationshipScan::DirectedAllRelationshipScan(std::string direction, std::string startVar, std::string endVar, std::string relVar)
        : startVar(startVar), endVar(endVar), relVar(relVar), direction(direction) {}

void DirectedAllRelationshipScan::execute() {
    cout<<"DirectedAllRelationshipScan: \n"<<endl;

    if(direction == "right"){
        cout << "("<<startVar<<") -[" << relVar<<"]-> (" << endVar << ")" << endl;
    }else{
        cout << "("<<startVar<<") <-[" << relVar<<"]- (" << endVar << ")" << endl;
    }
}

ExpandAll::ExpandAll(Operator *input, std::string startVar, std::string destVar, std::string relVar,
                     std::string relType, std::string direction): input(input), relType(relType), relVar(relVar), startVar(startVar), destVar(destVar), direction(direction){}

void ExpandAll::execute() {
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
}

Apply::Apply(Operator* opr): opr1(opr){}

void Apply::addOperator(Operator *opr) {
    this->opr2 = opr;
}


// Execute method
void Apply::execute() {
    if (opr1 != nullptr){
        cout<<"     left of Apply"<<endl;
        opr1->execute();
    }
    if (opr2 != nullptr){
        cout<<"     right of Apply"<<endl;
        opr2->execute();
    }

    cout<<"Apply: result merged"<<endl;
}

