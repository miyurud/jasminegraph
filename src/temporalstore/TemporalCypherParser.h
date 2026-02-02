/**
Copyright 2026 JasminGraph Team
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

#ifndef TEMPORAL_CYPHER_PARSER_H
#define TEMPORAL_CYPHER_PARSER_H

#include <string>
#include <vector>
#include <map>
#include <sstream>
#include <algorithm>
#include <cctype>

/**
 * TemporalCypherParser - Parse temporal Cypher query extensions
 * 
 * Supports temporal syntax:
 * - AT SNAPSHOT <id>
 * - FROM <start> TO <end>
 * - TEMPORAL PATTERN <pattern>
 * 
 * Examples:
 *   MATCH (a)-[r]->(b) AT SNAPSHOT 42
 *   MATCH (a)-[r]->(b) FROM 0 TO 100
 *   MATCH (n) RETURN n.city FROM 10 TO 20
 */
class TemporalCypherParser {
public:
    /**
     * Temporal query clause types
     */
    enum class TemporalClauseType {
        NONE,           // No temporal clause
        AT_SNAPSHOT,    // Query at specific snapshot
        TIME_RANGE,     // Query over time range
        TEMPORAL_MATCH  // Temporal pattern matching
    };
    
    /**
     * Parsed temporal clause
     */
    struct TemporalClause {
        TemporalClauseType type;
        uint32_t snapshotId;      // For AT_SNAPSHOT
        uint32_t startSnapshot;   // For TIME_RANGE
        uint32_t endSnapshot;     // For TIME_RANGE
        std::string pattern;      // For TEMPORAL_MATCH
        
        TemporalClause() 
            : type(TemporalClauseType::NONE), 
              snapshotId(0), 
              startSnapshot(0), 
              endSnapshot(0) {}
    };
    
    /**
     * Parsed Cypher query with temporal extensions
     */
    struct ParsedQuery {
        std::string baseQuery;        // Base Cypher query without temporal clauses
        TemporalClause temporalClause;
        bool isTemporalQuery;
        
        // Pattern matching components
        std::string matchPattern;     // e.g., "(a)-[r]->(b)"
        std::vector<std::string> nodeVariables;  // e.g., ["a", "b"]
        std::vector<std::string> edgeVariables;  // e.g., ["r"]
        
        // Property tracking
        std::map<std::string, std::string> nodeProperties; // var -> property
        
        ParsedQuery() : isTemporalQuery(false) {}
    };

private:
    /**
     * Convert string to uppercase
     */
    static std::string toUpper(const std::string& str) {
        std::string result = str;
        std::transform(result.begin(), result.end(), result.begin(), ::toupper);
        return result;
    }
    
    /**
     * Trim whitespace from string
     */
    static std::string trim(const std::string& str) {
        size_t start = str.find_first_not_of(" \t\n\r");
        if (start == std::string::npos) return "";
        
        size_t end = str.find_last_not_of(" \t\n\r");
        return str.substr(start, end - start + 1);
    }
    
    /**
     * Split string by delimiter
     */
    static std::vector<std::string> split(const std::string& str, char delimiter) {
        std::vector<std::string> tokens;
        std::stringstream ss(str);
        std::string token;
        
        while (std::getline(ss, token, delimiter)) {
            tokens.push_back(trim(token));
        }
        
        return tokens;
    }
    
    /**
     * Extract node variables from MATCH pattern
     * Example: "(a)-[r]->(b)" -> ["a", "b"]
     */
    static std::vector<std::string> extractNodeVariables(const std::string& pattern) {
        std::vector<std::string> nodes;
        size_t pos = 0;
        
        while ((pos = pattern.find('(', pos)) != std::string::npos) {
            size_t end = pattern.find(')', pos);
            if (end == std::string::npos) break;
            
            std::string nodeContent = pattern.substr(pos + 1, end - pos - 1);
            nodeContent = trim(nodeContent);
            
            // Extract variable name (before any properties or labels)
            size_t spacePos = nodeContent.find(' ');
            size_t colonPos = nodeContent.find(':');
            size_t bracePos = nodeContent.find('{');
            
            size_t varEnd = std::min({spacePos, colonPos, bracePos, nodeContent.length()});
            std::string varName = nodeContent.substr(0, varEnd);
            
            if (!varName.empty() && varName != "_") {
                nodes.push_back(varName);
            }
            
            pos = end + 1;
        }
        
        return nodes;
    }
    
    /**
     * Extract edge variables from MATCH pattern
     * Example: "(a)-[r]->(b)" -> ["r"]
     */
    static std::vector<std::string> extractEdgeVariables(const std::string& pattern) {
        std::vector<std::string> edges;
        size_t pos = 0;
        
        while ((pos = pattern.find('[', pos)) != std::string::npos) {
            size_t end = pattern.find(']', pos);
            if (end == std::string::npos) break;
            
            std::string edgeContent = pattern.substr(pos + 1, end - pos - 1);
            edgeContent = trim(edgeContent);
            
            // Extract variable name
            size_t spacePos = edgeContent.find(' ');
            size_t colonPos = edgeContent.find(':');
            
            size_t varEnd = std::min({spacePos, colonPos, edgeContent.length()});
            std::string varName = edgeContent.substr(0, varEnd);
            
            if (!varName.empty() && varName != "_") {
                edges.push_back(varName);
            }
            
            pos = end + 1;
        }
        
        return edges;
    }

public:
    /**
     * Parse Cypher query with temporal extensions
     * @param query Full Cypher query string
     * @return Parsed query structure
     */
    static ParsedQuery parse(const std::string& query) {
        ParsedQuery parsed;
        
        // Check for temporal clauses
        std::string upperQuery = toUpper(query);
        
        // Check for AT SNAPSHOT clause
        size_t atPos = upperQuery.find("AT SNAPSHOT");
        if (atPos != std::string::npos) {
            parsed.isTemporalQuery = true;
            parsed.temporalClause.type = TemporalClauseType::AT_SNAPSHOT;
            
            // Extract snapshot ID
            size_t idStart = atPos + 11; // Length of "AT SNAPSHOT"
            size_t idEnd = query.find_first_not_of("0123456789 \t", idStart);
            if (idEnd == std::string::npos) idEnd = query.length();
            
            std::string idStr = trim(query.substr(idStart, idEnd - idStart));
            parsed.temporalClause.snapshotId = std::stoul(idStr);
            
            // Remove temporal clause from base query
            parsed.baseQuery = trim(query.substr(0, atPos));
        }
        
        // Check for FROM...TO clause
        size_t fromPos = upperQuery.find(" FROM ");
        size_t toPos = upperQuery.find(" TO ", fromPos);
        
        if (fromPos != std::string::npos && toPos != std::string::npos) {
            parsed.isTemporalQuery = true;
            parsed.temporalClause.type = TemporalClauseType::TIME_RANGE;
            
            // Extract start snapshot
            size_t startIdStart = fromPos + 6; // Length of " FROM "
            std::string startStr = trim(query.substr(startIdStart, toPos - startIdStart));
            parsed.temporalClause.startSnapshot = std::stoul(startStr);
            
            // Extract end snapshot
            size_t endIdStart = toPos + 4; // Length of " TO "
            size_t endIdEnd = query.length();
            std::string endStr = trim(query.substr(endIdStart, endIdEnd - endIdStart));
            parsed.temporalClause.endSnapshot = std::stoul(endStr);
            
            // Remove temporal clause from base query
            parsed.baseQuery = trim(query.substr(0, fromPos));
        }
        
        // If no temporal clause found, use entire query as base
        if (!parsed.isTemporalQuery) {
            parsed.baseQuery = query;
        }
        
        // Extract MATCH pattern
        size_t matchPos = upperQuery.find("MATCH ");
        if (matchPos != std::string::npos) {
            size_t patternStart = matchPos + 6; // Length of "MATCH "
            
            // Find end of pattern (RETURN, WHERE, WITH, etc.)
            std::vector<std::string> clauseKeywords = {"RETURN", "WHERE", "WITH", "AT SNAPSHOT", " FROM "};
            size_t patternEnd = query.length();
            
            for (const auto& keyword : clauseKeywords) {
                size_t pos = upperQuery.find(keyword, patternStart);
                if (pos != std::string::npos && pos < patternEnd) {
                    patternEnd = pos;
                }
            }
            
            parsed.matchPattern = trim(query.substr(patternStart, patternEnd - patternStart));
            
            // Extract variables
            parsed.nodeVariables = extractNodeVariables(parsed.matchPattern);
            parsed.edgeVariables = extractEdgeVariables(parsed.matchPattern);
        }
        
        return parsed;
    }
    
    /**
     * Build temporal query from components
     * @param baseQuery Base Cypher query
     * @param snapshotId Snapshot ID for AT SNAPSHOT clause
     * @return Complete temporal query string
     */
    static std::string buildAtSnapshotQuery(const std::string& baseQuery, uint32_t snapshotId) {
        return baseQuery + " AT SNAPSHOT " + std::to_string(snapshotId);
    }
    
    /**
     * Build time range query
     * @param baseQuery Base Cypher query
     * @param startSnapshot Start snapshot
     * @param endSnapshot End snapshot
     * @return Complete temporal query string
     */
    static std::string buildTimeRangeQuery(const std::string& baseQuery,
                                          uint32_t startSnapshot,
                                          uint32_t endSnapshot) {
        return baseQuery + " FROM " + std::to_string(startSnapshot) + 
               " TO " + std::to_string(endSnapshot);
    }
    
    /**
     * Validate temporal query syntax
     * @param query Query to validate
     * @return true if valid, false otherwise
     */
    static bool validate(const std::string& query) {
        ParsedQuery parsed = parse(query);
        
        // Check for conflicting temporal clauses
        std::string upperQuery = toUpper(query);
        bool hasAtSnapshot = upperQuery.find("AT SNAPSHOT") != std::string::npos;
        bool hasFromTo = upperQuery.find(" FROM ") != std::string::npos && 
                        upperQuery.find(" TO ") != std::string::npos;
        
        if (hasAtSnapshot && hasFromTo) {
            // Can't have both AT SNAPSHOT and FROM...TO
            return false;
        }
        
        // Validate snapshot IDs are non-negative (already handled by stoul)
        if (parsed.temporalClause.type == TemporalClauseType::TIME_RANGE) {
            if (parsed.temporalClause.endSnapshot < parsed.temporalClause.startSnapshot) {
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * Extract property tracking from query
     * Example: "RETURN n.city" -> {n -> city}
     */
    static std::map<std::string, std::string> extractPropertyTracking(const std::string& query) {
        std::map<std::string, std::string> properties;
        
        std::string upperQuery = toUpper(query);
        size_t returnPos = upperQuery.find("RETURN ");
        if (returnPos == std::string::npos) {
            return properties;
        }
        
        size_t returnStart = returnPos + 7; // Length of "RETURN "
        size_t returnEnd = query.find(" FROM ", returnStart);
        if (returnEnd == std::string::npos) {
            returnEnd = query.find(" AT SNAPSHOT", returnStart);
        }
        if (returnEnd == std::string::npos) {
            returnEnd = query.length();
        }
        
        std::string returnClause = trim(query.substr(returnStart, returnEnd - returnStart));
        
        // Parse property access (e.g., "n.city")
        size_t dotPos = returnClause.find('.');
        if (dotPos != std::string::npos) {
            std::string var = trim(returnClause.substr(0, dotPos));
            std::string prop = trim(returnClause.substr(dotPos + 1));
            properties[var] = prop;
        }
        
        return properties;
    }
    
    /**
     * Generate query description
     */
    static std::string describeQuery(const ParsedQuery& parsed) {
        std::stringstream desc;
        
        if (!parsed.isTemporalQuery) {
            desc << "Standard Cypher query (no temporal clause)";
            return desc.str();
        }
        
        switch (parsed.temporalClause.type) {
            case TemporalClauseType::AT_SNAPSHOT:
                desc << "Query at snapshot " << parsed.temporalClause.snapshotId;
                break;
                
            case TemporalClauseType::TIME_RANGE:
                desc << "Query from snapshot " << parsed.temporalClause.startSnapshot
                     << " to " << parsed.temporalClause.endSnapshot;
                break;
                
            default:
                desc << "Unknown temporal query type";
        }
        
        if (!parsed.matchPattern.empty()) {
            desc << ", Pattern: " << parsed.matchPattern;
        }
        
        return desc.str();
    }
};

#endif // TEMPORAL_CYPHER_PARSER_H
