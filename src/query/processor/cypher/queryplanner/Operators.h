// Operators.h
#ifndef OPERATORS_H
#define OPERATORS_H

#include <string>
#include <iostream>
#include <vector>

using namespace std;
// Base Operator Class
class Operator {
public:
    virtual ~Operator() = default;
    virtual void execute() = 0; // Pure virtual function to be implemented by derived classes
};

// NodeScanByLabel Operator
class NodeScanByLabel : public Operator {
public:
    NodeScanByLabel(const string& label, const string& var = "var_0");
    void execute() override;

private:
    string label;
    string var;
};

// MultipleNodeScanByLabel Operator
class MultipleNodeScanByLabel : public Operator {
public:
    MultipleNodeScanByLabel(vector<string> label, const string& var = "var_0");
    void execute() override;

private:
    vector<string> label;
    string var;
};

// AllNodeScan Operator
class AllNodeScan : public Operator {
public:
    AllNodeScan(const string& var = "var_0");
    void execute() override;

private:
    string var;
};

// ProduceResults Operator
class ProduceResults : public Operator {
public:
    ProduceResults(const string& variable, Operator* op);
    void execute() override;

private:
    string variable;
    Operator* op;
};


// Filter Operator
class Filter : public Operator {
public:
    Filter(Operator* input, const string& predicate);
    void execute() override;

private:
    Operator* input;
    string predicate;
};

// Projection Operator
class Projection : public Operator {
public:
    Projection(Operator* input, const vector<std::string>& columns);
    void execute() override;

private:
    Operator* input;
    vector<std::string> columns;
};

// Join Operator
class Join : public Operator {
public:
    Join(Operator* left, Operator* right, const string& joinCondition);
    void execute() override;

private:
    Operator* left;
    Operator* right;
    string joinCondition;
};

// Aggregation Operator
class Aggregation : public Operator {
public:
    Aggregation(Operator* input, const string& aggFunction, const string& column);
    void execute() override;

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
    void execute() override;
};

// Sort Operator
class Sort : public Operator {
    Operator* input;
    string sortByColumn;
    bool ascending;
public:
    Sort(Operator* input, const string& sortByColumn, bool ascending);
    void execute() override;
};

// GroupBy Operator
class GroupBy : public Operator {
    Operator* input;
    vector<std::string> groupByColumns;
public:
    GroupBy(Operator* input, const vector<std::string>& groupByColumns);
    void execute() override;
};

// Distinct Operator
class Distinct : public Operator {
    Operator* input;
public:
    Distinct(Operator* input);
    void execute() override;
};

// Union Operator
class Union : public Operator {
    Operator* left;
    Operator* right;
public:
    Union(Operator* left, Operator* right);
    void execute() override;
};

// Intersection Operator
class Intersection : public Operator {
    Operator* left;
    Operator* right;
public:
    Intersection(Operator* left, Operator* right);
    void execute() override;
};
// Optional: Add other operators like Project, Join, etc.

#endif // OPERATORS_H
