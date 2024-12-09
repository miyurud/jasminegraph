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

#include <any>
#include "/home/ubuntu/software/antlr/CypherBaseVisitor.h"
#ifndef AST_BUILDER_H
#define AST_BUILDER_H

using namespace std;

class ASTBuilder : public CypherBaseVisitor {
public:

    std::any visitOC_Cypher(CypherParser::OC_CypherContext *ctx) override;

    std::any visitOC_Statement(CypherParser::OC_StatementContext *ctx) override;

    std::any visitOC_Query(CypherParser::OC_QueryContext *ctx) override;

    std::any visitOC_RegularQuery(CypherParser::OC_RegularQueryContext *ctx) override ;

    std::any visitOC_Union(CypherParser::OC_UnionContext *ctx) override;

    std::any visitOC_SingleQuery(CypherParser::OC_SingleQueryContext *ctx) override;

    std::any visitOC_SinglePartQuery(CypherParser::OC_SinglePartQueryContext *ctx) override;

    std::any visitOC_MultiPartQuery(CypherParser::OC_MultiPartQueryContext *ctx) override;

    std::any visitOC_UpdatingClause(CypherParser::OC_UpdatingClauseContext *ctx) override ;
  
    std::any visitOC_ReadingClause(CypherParser::OC_ReadingClauseContext *ctx) override ;

    std::any visitOC_Match(CypherParser::OC_MatchContext *ctx) override ;

    std::any visitOC_Unwind(CypherParser::OC_UnwindContext *ctx) override ;

    std::any visitOC_Merge(CypherParser::OC_MergeContext *ctx) override ;

    std::any visitOC_MergeAction(CypherParser::OC_MergeActionContext *ctx) override ;

    std::any visitOC_Create(CypherParser::OC_CreateContext *ctx) override ;

    std::any visitOC_Set(CypherParser::OC_SetContext *ctx) override ;

    std::any visitOC_SetItem(CypherParser::OC_SetItemContext *ctx) override;

    std::any visitOC_Delete(CypherParser::OC_DeleteContext *ctx) override ;
  
    std::any visitOC_Remove(CypherParser::OC_RemoveContext *ctx) override ;

    std::any visitOC_RemoveItem(CypherParser::OC_RemoveItemContext *ctx) override ;

    std::any visitOC_InQueryCall(CypherParser::OC_InQueryCallContext *ctx) override ;

    std::any visitOC_StandaloneCall(CypherParser::OC_StandaloneCallContext *ctx) override;
  
    std::any visitOC_YieldItems(CypherParser::OC_YieldItemsContext *ctx) override ;

    std::any visitOC_YieldItem(CypherParser::OC_YieldItemContext *ctx) override ;
  
    std::any visitOC_With(CypherParser::OC_WithContext *ctx) override ;

    std::any visitOC_Return(CypherParser::OC_ReturnContext *ctx) override ;

    std::any visitOC_ProjectionBody(CypherParser::OC_ProjectionBodyContext *ctx) override ;

    std::any visitOC_ProjectionItems(CypherParser::OC_ProjectionItemsContext *ctx) override ;
  
    std::any visitOC_ProjectionItem(CypherParser::OC_ProjectionItemContext *ctx) override ;

    std::any visitOC_Order(CypherParser::OC_OrderContext *ctx) override ;

    std::any visitOC_Skip(CypherParser::OC_SkipContext *ctx) override ;

    std::any visitOC_Limit(CypherParser::OC_LimitContext *ctx) override ;

    std::any visitOC_SortItem(CypherParser::OC_SortItemContext *ctx) override ;
  
    std::any visitOC_Where(CypherParser::OC_WhereContext *ctx) override ;
  
    std::any visitOC_Pattern(CypherParser::OC_PatternContext *ctx) override ;

    std::any visitOC_PatternPart(CypherParser::OC_PatternPartContext *ctx) override ;
  
    std::any visitOC_AnonymousPatternPart(CypherParser::OC_AnonymousPatternPartContext *ctx) override ;
  
    std::any visitOC_PatternElement(CypherParser::OC_PatternElementContext *ctx) override ;

    std::any visitOC_RelationshipsPattern(CypherParser::OC_RelationshipsPatternContext *ctx) override ;

    std::any visitOC_NodePattern(CypherParser::OC_NodePatternContext *ctx) override ;
  
    std::any visitOC_PatternElementChain(CypherParser::OC_PatternElementChainContext *ctx) override ;
  
    std::any visitOC_RelationshipPattern(CypherParser::OC_RelationshipPatternContext *ctx) override ;

    std::any visitOC_RelationshipDetail(CypherParser::OC_RelationshipDetailContext *ctx) override ;
  
    std::any visitOC_Properties(CypherParser::OC_PropertiesContext *ctx) override ;
  
    std::any visitOC_RelationshipTypes(CypherParser::OC_RelationshipTypesContext *ctx) override ;

  
    std::any visitOC_NodeLabels(CypherParser::OC_NodeLabelsContext *ctx) override ;

    std::any visitOC_NodeLabel(CypherParser::OC_NodeLabelContext *ctx) override ;
  
    std::any visitOC_RangeLiteral(CypherParser::OC_RangeLiteralContext *ctx) override ;
  
    std::any visitOC_LabelName(CypherParser::OC_LabelNameContext *ctx) override ;

    std::any visitOC_RelTypeName(CypherParser::OC_RelTypeNameContext *ctx) override ;

    std::any visitOC_PropertyExpression(CypherParser::OC_PropertyExpressionContext *ctx) override ;
  
    std::any visitOC_Expression(CypherParser::OC_ExpressionContext *ctx) override ;
  
    std::any visitOC_OrExpression(CypherParser::OC_OrExpressionContext *ctx) override ;

    std::any visitOC_XorExpression(CypherParser::OC_XorExpressionContext *ctx) override ;
  
    std::any visitOC_AndExpression(CypherParser::OC_AndExpressionContext *ctx) override ;
  
    std::any visitOC_NotExpression(CypherParser::OC_NotExpressionContext *ctx) override ;

    std::any visitOC_ComparisonExpression(CypherParser::OC_ComparisonExpressionContext *ctx) override ;

    std::any visitOC_PartialComparisonExpression(CypherParser::OC_PartialComparisonExpressionContext *ctx) override ;

  
    std::any visitOC_StringListNullPredicateExpression(CypherParser::OC_StringListNullPredicateExpressionContext *ctx) override ;

    std::any visitOC_StringPredicateExpression(CypherParser::OC_StringPredicateExpressionContext *ctx) override ;

    std::any visitOC_ListPredicateExpression(CypherParser::OC_ListPredicateExpressionContext *ctx) override ;

    std::any visitOC_NullPredicateExpression(CypherParser::OC_NullPredicateExpressionContext *ctx) override ;

    std::any visitOC_AddOrSubtractExpression(CypherParser::OC_AddOrSubtractExpressionContext *ctx) override ;
  
    std::any visitOC_MultiplyDivideModuloExpression(CypherParser::OC_MultiplyDivideModuloExpressionContext *ctx) override ;
  
    std::any visitOC_PowerOfExpression(CypherParser::OC_PowerOfExpressionContext *ctx) override ;

    std::any visitOC_UnaryAddOrSubtractExpression(CypherParser::OC_UnaryAddOrSubtractExpressionContext *ctx) override ;
  
    std::any visitOC_NonArithmeticOperatorExpression(CypherParser::OC_NonArithmeticOperatorExpressionContext *ctx) override ;
  
    std::any visitOC_ListOperatorExpression(CypherParser::OC_ListOperatorExpressionContext *ctx) override ;

    std::any visitOC_PropertyLookup(CypherParser::OC_PropertyLookupContext *ctx) override ;

    std::any visitOC_Atom(CypherParser::OC_AtomContext *ctx) override ;
  
    std::any visitOC_CaseExpression(CypherParser::OC_CaseExpressionContext *ctx) override ;
  
    std::any visitOC_CaseAlternative(CypherParser::OC_CaseAlternativeContext *ctx) override ;

    std::any visitOC_ListComprehension(CypherParser::OC_ListComprehensionContext *ctx) override ;

    std::any visitOC_PatternComprehension(CypherParser::OC_PatternComprehensionContext *ctx) override ;

    std::any visitOC_Quantifier(CypherParser::OC_QuantifierContext *ctx) override ;

    std::any visitOC_FilterExpression(CypherParser::OC_FilterExpressionContext *ctx) override ;
  
    std::any visitOC_PatternPredicate(CypherParser::OC_PatternPredicateContext *ctx) override ;
  
    std::any visitOC_ParenthesizedExpression(CypherParser::OC_ParenthesizedExpressionContext *ctx) override ;
  
    std::any visitOC_IdInColl(CypherParser::OC_IdInCollContext *ctx) override ;
  
    std::any visitOC_FunctionInvocation(CypherParser::OC_FunctionInvocationContext *ctx) override ;
  
    std::any visitOC_FunctionName(CypherParser::OC_FunctionNameContext *ctx) override ;

    std::any visitOC_ExistentialSubquery(CypherParser::OC_ExistentialSubqueryContext *ctx) override ;

    std::any visitOC_ExplicitProcedureInvocation(CypherParser::OC_ExplicitProcedureInvocationContext *ctx) override ;

    std::any visitOC_ImplicitProcedureInvocation(CypherParser::OC_ImplicitProcedureInvocationContext *ctx) override ;

    std::any visitOC_ProcedureResultField(CypherParser::OC_ProcedureResultFieldContext *ctx) override ;
  
    std::any visitOC_ProcedureName(CypherParser::OC_ProcedureNameContext *ctx) override ;

    std::any visitOC_Namespace(CypherParser::OC_NamespaceContext *ctx) override ;

    std::any visitOC_Variable(CypherParser::OC_VariableContext *ctx) override ;

    std::any visitOC_Literal(CypherParser::OC_LiteralContext *ctx) override ;

    std::any visitOC_BooleanLiteral(CypherParser::OC_BooleanLiteralContext *ctx) override ;

    std::any visitOC_NumberLiteral(CypherParser::OC_NumberLiteralContext *ctx) override ;
  
    std::any visitOC_IntegerLiteral(CypherParser::OC_IntegerLiteralContext *ctx) override ;
  
    std::any visitOC_DoubleLiteral(CypherParser::OC_DoubleLiteralContext *ctx) override ;

    std::any visitOC_ListLiteral(CypherParser::OC_ListLiteralContext *ctx) override ;

    std::any visitOC_MapLiteral(CypherParser::OC_MapLiteralContext *ctx) override ;

    std::any visitOC_PropertyKeyName(CypherParser::OC_PropertyKeyNameContext *ctx) override ;
  
    std::any visitOC_Parameter(CypherParser::OC_ParameterContext *ctx) override ;

    std::any visitOC_SchemaName(CypherParser::OC_SchemaNameContext *ctx) override ;
  
    std::any visitOC_ReservedWord(CypherParser::OC_ReservedWordContext *ctx) override ;
  
    std::any visitOC_SymbolicName(CypherParser::OC_SymbolicNameContext *ctx) override ;
  
    std::any visitOC_LeftArrowHead(CypherParser::OC_LeftArrowHeadContext *ctx) override ;
  
    std::any visitOC_RightArrowHead(CypherParser::OC_RightArrowHeadContext *ctx) override ;

};

#endif //AST_BUILDER_H
