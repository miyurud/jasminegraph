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
#include "../../../../util/logger/Logger.h"
class ASTNode;
using namespace std;
// Base Operator Class
class Operator {
 public:
    virtual ~Operator() = default;
    virtual string execute() = 0;  // Pure virtual function to be implemented by derived classes
    static bool isAggregate;
    static string aggregateType;
    static string aggregateKey;
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
    Operator* getOperator();
    void setOperator(Operator* op);

 private:
    vector<ASTNode*> item;
    Operator* op;
};

// Filter Operator
class Filter : public Operator {
 public:
    Filter(Operator* input, vector<pair<string,ASTNode*>> filterCases);
    string analyzeWhere(ASTNode* ast);
    string analyzePropertiesMap(pair<string,ASTNode*> item);
    string comparisonOperand(ASTNode* ast);
    string execute() override;

 private:
    Operator* input;
    vector<pair<string, ASTNode*>> filterCases;
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

// ExpandAll Operator
class ExpandAll : public Operator {
 public:
    ExpandAll(Operator* input, string startVar, string destVar, string relVar,
                string relType = "null", string direction = "");
    string execute() override;

 private:
    Operator* input;
    string startVar;
    string destVar;
    string relVar;
    string relType;
    string direction;
};


// Limit Operator
class Limit : public Operator {
 public:
    Limit(Operator* input, ASTNode* limit);
    string execute() override;
 private:
    Operator* input;
    ASTNode* limit;
};

// Skip Operator
class Skip : public Operator {
public:
    Skip(Operator *input, ASTNode *skip);
    string execute() override;
private:
    Operator *input;
    ASTNode *skip;
};

// Distinct Operator
class Distinct : public Operator {
 public:
    Distinct(Operator* input, const vector<ASTNode*> columns);
    string execute() override;

 private:
    Operator* input;
    vector<ASTNode*> columns;
};

// OrderBy Operator
class OrderBy : public Operator {
public:
    OrderBy(Operator* input, ASTNode* orderByClause);
    string execute() override;
private:
    Operator* input;
    ASTNode* orderByClause;
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

// CacheProperty
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
    UndirectedRelationshipTypeScan(string relType, string relvar = "rel_var",
                                   string startVar = "var_0", string endVar = "var_1");

    // Execute method to perform the scan
    string execute() override;

 private:
    string relType;  // The relationship type to scan for
    string startVar;  // Variable name for the start node
    string endVar;   // Variable name for the end node
    string relvar;
};

class UndirectedAllRelationshipScan : public Operator {
 public:
    UndirectedAllRelationshipScan(string startVar = "var_0", string endVar = "var_1", string relVar = "edge_var_0");
    string execute() override;

 private:
    string startVar;  // Variable name for the start node
    string endVar;   // Variable name for the end node
    string relVar;
};

class DirectedAllRelationshipScan : public Operator {
 public:
    DirectedAllRelationshipScan(string direction, string startVar = "var_0",
                                 string endVar = "var_1", string relVar = "edge_var_0");
    string execute() override;

 private:
    string startVar;  // Variable name for the start node
    string endVar;   // Variable name for the end node
    string relVar;
    string direction;
};

class DirectedRelationshipTypeScan : public Operator {
 public:
    DirectedRelationshipTypeScan(string direction, string relType, string relvar = "rel_var",
                                 string startVar = "var_0", string endVar = "var_1");

    // Execute method to perform the scan
    string execute() override;

 private:
    string direction;
    string relType;  // The relationship type to scan for
    string startVar;  // Variable name for the start node
    string endVar;   // Variable name for the end node
    string relvar;
};

class Apply : public Operator {
 public:
    Apply(Operator* operator1);
    void addOperator(Operator* operator2);

    // Execute method to perform the scan
    string execute() override;

 private:
    Operator* operator1;
    Operator* operator2;
};

class EagerFunction : public Operator {
public:
    // Constructor
    EagerFunction(Operator* input, ASTNode* ast, string functionName);
    string execute() override;

private:
    Operator* input;
    ASTNode* ast;
    string functionName;
};

class Create : public Operator {
public:
    // Constructor
    Create(Operator* input, ASTNode* ast);
    string execute() override;

private:
    Operator* input;
    ASTNode* ast;
};

class CartesianProduct : public Operator {
public:
    // Constructor
    CartesianProduct(Operator* left, Operator* right);
    string execute() override;

private:
    Operator* left;
    Operator* right;
};
string printDownArrow(int width);
#endif  // OPERATORS_H
