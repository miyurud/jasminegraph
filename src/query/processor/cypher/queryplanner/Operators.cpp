#include "Operators.h"

#include "../astbuilder/ASTNode.h"
using namespace std;
// NodeScan Implementation
NodeScanByLabel::NodeScanByLabel(const string& label, const string& var) : label(label), var(var) {}

void NodeScanByLabel::execute() {
    cout << "scanning node based on single label: " << label <<" and assigned with variable: "<<var<< endl;
}

// MultipleNodeScanByLabel Implementation
MultipleNodeScanByLabel::MultipleNodeScanByLabel(vector<string> label, const string& var) : label(label), var(var) {}

void MultipleNodeScanByLabel::execute() {
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
    cout << "scanning all nodes and assigned with : " << var << endl;
}

// ProduceResults Implementation
ProduceResults::ProduceResults(Operator* opr, vector<ASTNode*> item) : item(item), op(opr) {}

void ProduceResults::execute() {
    op->execute();
    for(auto* e: item)
    {
        cout<< e->nodeType<<endl;
    }
}

// Filter Implementation
Filter::Filter(Operator* input, ASTNode* root) : input(input), root(root) {}

void Filter::execute() {
    input->execute();
    cout << "Applying Filter with predicate: " << root->nodeType << endl;
}

// Projection Implementation
Projection::Projection(Operator* input, const vector<ASTNode*> columns) : input(input), columns(columns) {}

void Projection::execute() {
    input->execute();
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
    int i=1;
    for(auto* prop: property)
    {
        string s = prop->elements[0]->value +"."+prop->elements[1]->elements[0]->value;
        cout<<"get property "<<i++<<" and cache here: "<<s<<endl;

    }
}

UndirectedRelationshipTypeScan::UndirectedRelationshipTypeScan(string relType, string startVar, string endVar)
    : relType(relType), startVar(startVar), endVar(endVar) {}

// Execute method
void UndirectedRelationshipTypeScan::execute() {
      cout << "Executing UndirectedRelationshipTypeScan for relationship type: " << relType <<   endl;
      cout << "Start variable: " << startVar << ", End variable: " << endVar <<   endl;

}

UndirectedAllRelationshipScan::UndirectedAllRelationshipScan(string startVar, string endVar)
    : startVar(startVar), endVar(endVar) {}


void UndirectedAllRelationshipScan::execute() {
     cout << "Executing UndirectedAllRelationshipScan for all relationship types." <<  endl;
     cout << "Start variable: " << startVar << ", End variable: " << endVar <<  endl;


}