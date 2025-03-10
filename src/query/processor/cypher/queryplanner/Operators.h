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

// Operators.h
#ifndef OPERATORS_H
#define OPERATORS_H

#include <string>
#include <iostream>
#include <vector>
class ASTNode;
using namespace std;
// Base Operator Class
class Operator {
public:
    virtual ~Operator() = default;
    virtual string execute() = 0; // Pure virtual function to be implemented by derived classes
};

// NodeScanByLabel Operator
class NodeScanByLabel : public Operator {
public:
    NodeScanByLabel(string label, string var = "var_0");
    string execute() override;

private:
    string label;
    string var;
};

// MultipleNodeScanByLabel Operator
class MultipleNodeScanByLabel : public Operator {
public:
    MultipleNodeScanByLabel(vector<string> label, const string& var = "var_0");
    string execute() override;

private:
    vector<string> label;
    string var;
};

// NodeByIdSeek Operator
class NodeByIdSeek : public Operator {
public:
    NodeByIdSeek(string id, string var);
    string execute() override;
    string getId() {return this->id;};
    string getVariable() {return this->var;};

private:
    string id;
    string var;
};

// AllNodeScan Operator
class AllNodeScan : public Operator {
public:
    AllNodeScan(const string& var = "var_0");
    string execute() override;

private:
    string var;
};

// ProduceResults Operator
class ProduceResults : public Operator {
public:
    ProduceResults(Operator* op, vector<ASTNode*> item);
    string execute() override;

private:
    vector<ASTNode*> item;
    Operator* op;
};

// Filter Operator
class Filter : public Operator {
public:
    Filter(Operator* input, vector<pair<string,ASTNode*>> filterCases);
    string analyze(ASTNode* ast);
    string comparisonOperand(ASTNode* ast);
    string execute() override;

private:
    Operator* input;
    vector<pair<string,ASTNode*>> filterCases;
};

// Projection Operator
class Projection : public Operator {
public:
    Projection(Operator* input, const vector<ASTNode*> columns);
    string execute() override;

private:
    Operator* input;
    vector<ASTNode*> columns;
};

//ExpandAll Operator
class ExpandAll : public Operator {
public:
    ExpandAll(Operator* input, string startVar, string destVar, string relVar, string relType = "null", string direction = "");
    string execute() override;

private:
    Operator* input;
    string startVar;
    string destVar;
    string relVar;
    string relType;
    string direction;
};

// Join Operator
class Join : public Operator {
public:
    Join(Operator* left, Operator* right, const string& joinCondition);
    string execute() override;

private:
    Operator* left;
    Operator* right;
    string joinCondition;
};

// Aggregation Operator
class Aggregation : public Operator {
public:
    Aggregation(Operator* input, const string& aggFunction, const string& column);
    string execute() override;

private:
    Operator* input;
    string aggFunction;
    string column;
};

// Limit Operator
class Limit : public Operator {
    Operator* input;
    int limit;
public:
    Limit(Operator* input, int limit);
    string execute() override;
};

// Sort Operator
class Sort : public Operator {
    Operator* input;
    string sortByColumn;
    bool ascending;
public:
    Sort(Operator* input, const string& sortByColumn, bool ascending);
    string execute() override;
};

// GroupBy Operator
class GroupBy : public Operator {
    Operator* input;
    vector<std::string> groupByColumns;
public:
    GroupBy(Operator* input, const vector<std::string>& groupByColumns);
    string execute() override;
};

// Distinct Operator
class Distinct : public Operator {
    Operator* input;
public:
    Distinct(Operator* input);
    string execute() override;
};

// Union Operator
class Union : public Operator {
    Operator* left;
    Operator* right;
public:
    Union(Operator* left, Operator* right);
    string execute() override;
};

// Intersection Operator
class Intersection : public Operator {
    Operator* left;
    Operator* right;
public:
    Intersection(Operator* left, Operator* right);
    string execute() override;
};

//CacheProperty
class CacheProperty : public Operator {
public:
    CacheProperty(Operator* input, vector<ASTNode*> property);
    string execute() override;

private:
    Operator* input;
    vector<ASTNode*> property;
};

class UndirectedRelationshipTypeScan : public Operator {
public:
    // Constructor
    UndirectedRelationshipTypeScan(string relType, string relvar = "rel_var", string startVar = "var_0", string endVar = "var_1");

    // Execute method to perform the scan
    string execute() override;

private:
    string relType;  // The relationship type to scan for
    string startVar; // Variable name for the start node
    string endVar;   // Variable name for the end node
    string relvar;
};

class UndirectedAllRelationshipScan : public Operator {
public:

    UndirectedAllRelationshipScan( string startVar = "var_0", string endVar = "var_1", string relVar = "edge_var_0");
    string execute() override;

private:
    string startVar; // Variable name for the start node
    string endVar;   // Variable name for the end node
    string relVar;
};

class DirectedAllRelationshipScan : public Operator {
public:

    DirectedAllRelationshipScan( string direction, string startVar = "var_0", string endVar = "var_1", string relVar = "edge_var_0");
    string execute() override;

private:
    string startVar; // Variable name for the start node
    string endVar;   // Variable name for the end node
    string relVar;
    string direction;
};

class DirectedRelationshipTypeScan : public Operator {
public:
    // Constructor
    DirectedRelationshipTypeScan(string direction, string relType, string relvar = "rel_var", string startVar = "var_0", string endVar = "var_1");

    // Execute method to perform the scan
    string execute() override;

private:
    string direction;
    string relType;  // The relationship type to scan for
    string startVar; // Variable name for the start node
    string endVar;   // Variable name for the end node
    string relvar;
};

class Apply : public Operator {
public:
    // Constructor
    Apply(Operator* opr);
    void addOperator(Operator* opr);

    // Execute method to perform the scan
    string execute() override;

private:
    Operator* opr1;
    Operator* opr2;
};

string printDownArrow(int width);
#endif // OPERATORS_H
