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

#include "ASTBuilder.h"
#include <any>
#include <string>
#include "ASTInternalNode.h"
#include "ASTLeafNoValue.h"
#include "ASTLeafValue.h"
#include "/home/ubuntu/software/jasminegraph/code_generated/antlr/CypherBaseVisitor.h"

any visitOC_Cypher(CypherParser::OC_CypherContext *ctx) ;

any visitOC_Statement(CypherParser::OC_StatementContext *ctx) ;

any visitOC_Query(CypherParser::OC_QueryContext *ctx) ;

any visitOC_RegularQuery(CypherParser::OC_RegularQueryContext *ctx)  ;

any visitOC_Union(CypherParser::OC_UnionContext *ctx) ;

any visitOC_SingleQuery(CypherParser::OC_SingleQueryContext *ctx) ;

any visitOC_SinglePartQuery(CypherParser::OC_SinglePartQueryContext *ctx) ;

any visitOC_MultiPartQuery(CypherParser::OC_MultiPartQueryContext *ctx) ;

any visitOC_UpdatingClause(CypherParser::OC_UpdatingClauseContext *ctx)  ;

any visitOC_ReadingClause(CypherParser::OC_ReadingClauseContext *ctx)  ;

any visitOC_Match(CypherParser::OC_MatchContext *ctx)  ;

any visitOC_Unwind(CypherParser::OC_UnwindContext *ctx)  ;

any visitOC_Merge(CypherParser::OC_MergeContext *ctx)  ;

any visitOC_MergeAction(CypherParser::OC_MergeActionContext *ctx)  ;

any visitOC_Create(CypherParser::OC_CreateContext *ctx)  ;

any visitOC_Set(CypherParser::OC_SetContext *ctx)  ;

any visitOC_SetItem(CypherParser::OC_SetItemContext *ctx) ;

any visitOC_Delete(CypherParser::OC_DeleteContext *ctx)  ;

any visitOC_Remove(CypherParser::OC_RemoveContext *ctx)  ;

any visitOC_RemoveItem(CypherParser::OC_RemoveItemContext *ctx)  ;

any visitOC_InQueryCall(CypherParser::OC_InQueryCallContext *ctx)  ;

any visitOC_StandaloneCall(CypherParser::OC_StandaloneCallContext *ctx) ;

any visitOC_YieldItems(CypherParser::OC_YieldItemsContext *ctx)  ;

any visitOC_YieldItem(CypherParser::OC_YieldItemContext *ctx)  ;

any visitOC_With(CypherParser::OC_WithContext *ctx)  ;

any visitOC_Return(CypherParser::OC_ReturnContext *ctx)  ;

any visitOC_ProjectionBody(CypherParser::OC_ProjectionBodyContext *ctx)  ;

any visitOC_ProjectionItems(CypherParser::OC_ProjectionItemsContext *ctx)  ;

any visitOC_ProjectionItem(CypherParser::OC_ProjectionItemContext *ctx)  ;

any visitOC_Order(CypherParser::OC_OrderContext *ctx)  ;

any visitOC_Skip(CypherParser::OC_SkipContext *ctx)  ;

any visitOC_Limit(CypherParser::OC_LimitContext *ctx)  ;

any visitOC_SortItem(CypherParser::OC_SortItemContext *ctx)  ;

any visitOC_Where(CypherParser::OC_WhereContext *ctx)  ;

any visitOC_Pattern(CypherParser::OC_PatternContext *ctx)  ;

any visitOC_PatternPart(CypherParser::OC_PatternPartContext *ctx)  ;

any visitOC_AnonymousPatternPart(CypherParser::OC_AnonymousPatternPartContext *ctx)  ;

any visitOC_PatternElement(CypherParser::OC_PatternElementContext *ctx)  ;

any visitOC_RelationshipsPattern(CypherParser::OC_RelationshipsPatternContext *ctx)  ;

any visitOC_NodePattern(CypherParser::OC_NodePatternContext *ctx)  ;

any visitOC_PatternElementChain(CypherParser::OC_PatternElementChainContext *ctx)  ;

any visitOC_RelationshipPattern(CypherParser::OC_RelationshipPatternContext *ctx)  ;

any visitOC_RelationshipDetail(CypherParser::OC_RelationshipDetailContext *ctx)  ;

any visitOC_Properties(CypherParser::OC_PropertiesContext *ctx)  ;

any visitOC_RelationshipTypes(CypherParser::OC_RelationshipTypesContext *ctx)  ;


any visitOC_NodeLabels(CypherParser::OC_NodeLabelsContext *ctx)  ;

any visitOC_NodeLabel(CypherParser::OC_NodeLabelContext *ctx)  ;

any visitOC_RangeLiteral(CypherParser::OC_RangeLiteralContext *ctx)  ;

any visitOC_LabelName(CypherParser::OC_LabelNameContext *ctx)  ;

any visitOC_RelTypeName(CypherParser::OC_RelTypeNameContext *ctx)  ;

any visitOC_PropertyExpression(CypherParser::OC_PropertyExpressionContext *ctx)  ;

any visitOC_Expression(CypherParser::OC_ExpressionContext *ctx)  ;

any visitOC_OrExpression(CypherParser::OC_OrExpressionContext *ctx)  ;

any visitOC_XorExpression(CypherParser::OC_XorExpressionContext *ctx)  ;

any visitOC_AndExpression(CypherParser::OC_AndExpressionContext *ctx)  ;

any visitOC_NotExpression(CypherParser::OC_NotExpressionContext *ctx)  ;

any visitOC_ComparisonExpression(CypherParser::OC_ComparisonExpressionContext *ctx)  ;

any visitOC_PartialComparisonExpression(CypherParser::OC_PartialComparisonExpressionContext *ctx)  ;

any visitOC_StringListNullPredicateExpression(CypherParser::OC_StringListNullPredicateExpressionContext *ctx)  ;

any visitOC_StringPredicateExpression(CypherParser::OC_StringPredicateExpressionContext *ctx)  ;

any visitOC_ListPredicateExpression(CypherParser::OC_ListPredicateExpressionContext *ctx)  ;

any visitOC_NullPredicateExpression(CypherParser::OC_NullPredicateExpressionContext *ctx)  ;

any visitOC_AddOrSubtractExpression(CypherParser::OC_AddOrSubtractExpressionContext *ctx)  ;

any visitOC_MultiplyDivideModuloExpression(CypherParser::OC_MultiplyDivideModuloExpressionContext *ctx)  ;

any visitOC_PowerOfExpression(CypherParser::OC_PowerOfExpressionContext *ctx)  ;

any visitOC_UnaryAddOrSubtractExpression(CypherParser::OC_UnaryAddOrSubtractExpressionContext *ctx)  ;

any visitOC_NonArithmeticOperatorExpression(CypherParser::OC_NonArithmeticOperatorExpressionContext *ctx)  ;

any visitOC_ListOperatorExpression(CypherParser::OC_ListOperatorExpressionContext *ctx)  ;

any visitOC_PropertyLookup(CypherParser::OC_PropertyLookupContext *ctx)  ;

any visitOC_Atom(CypherParser::OC_AtomContext *ctx)  ;

any visitOC_CaseExpression(CypherParser::OC_CaseExpressionContext *ctx)  ;

any visitOC_CaseAlternative(CypherParser::OC_CaseAlternativeContext *ctx)  ;

any visitOC_ListComprehension(CypherParser::OC_ListComprehensionContext *ctx)  ;

any visitOC_PatternComprehension(CypherParser::OC_PatternComprehensionContext *ctx)  ;

any visitOC_Quantifier(CypherParser::OC_QuantifierContext *ctx)  ;

any visitOC_FilterExpression(CypherParser::OC_FilterExpressionContext *ctx)  ;

any visitOC_PatternPredicate(CypherParser::OC_PatternPredicateContext *ctx)  ;

any visitOC_ParenthesizedExpression(CypherParser::OC_ParenthesizedExpressionContext *ctx)  ;

any visitOC_IdInColl(CypherParser::OC_IdInCollContext *ctx)  ;

any visitOC_FunctionInvocation(CypherParser::OC_FunctionInvocationContext *ctx)  ;

any visitOC_FunctionName(CypherParser::OC_FunctionNameContext *ctx)  ;

any visitOC_ExistentialSubquery(CypherParser::OC_ExistentialSubqueryContext *ctx)  ;

any visitOC_ExplicitProcedureInvocation(CypherParser::OC_ExplicitProcedureInvocationContext *ctx)  ;

any visitOC_ImplicitProcedureInvocation(CypherParser::OC_ImplicitProcedureInvocationContext *ctx)  ;

any visitOC_ProcedureResultField(CypherParser::OC_ProcedureResultFieldContext *ctx)  ;

any visitOC_ProcedureName(CypherParser::OC_ProcedureNameContext *ctx)  ;

any visitOC_Namespace(CypherParser::OC_NamespaceContext *ctx)  ;

any visitOC_Variable(CypherParser::OC_VariableContext *ctx)  ;

any visitOC_Literal(CypherParser::OC_LiteralContext *ctx)  ;

any visitOC_BooleanLiteral(CypherParser::OC_BooleanLiteralContext *ctx)  ;

any visitOC_NumberLiteral(CypherParser::OC_NumberLiteralContext *ctx)  ;

any visitOC_IntegerLiteral(CypherParser::OC_IntegerLiteralContext *ctx)  ;

any visitOC_DoubleLiteral(CypherParser::OC_DoubleLiteralContext *ctx)  ;

any visitOC_ListLiteral(CypherParser::OC_ListLiteralContext *ctx)  ;

any visitOC_MapLiteral(CypherParser::OC_MapLiteralContext *ctx)  ;

any visitOC_PropertyKeyName(CypherParser::OC_PropertyKeyNameContext *ctx)  ;

any visitOC_Parameter(CypherParser::OC_ParameterContext *ctx)  ;

any visitOC_SchemaName(CypherParser::OC_SchemaNameContext *ctx)  ;

any visitOC_ReservedWord(CypherParser::OC_ReservedWordContext *ctx)  ;

any visitOC_SymbolicName(CypherParser::OC_SymbolicNameContext *ctx)  ;

any visitOC_LeftArrowHead(CypherParser::OC_LeftArrowHeadContext *ctx)  ;

any visitOC_RightArrowHead(CypherParser::OC_RightArrowHeadContext *ctx)  ;

any visitOC_Dash(CypherParser::OC_DashContext *ctx)  ;



any ASTBuilder::visitOC_Cypher(CypherParser::OC_CypherContext *ctx)  {
  return visitOC_Statement(ctx->oC_Statement());
}


any ASTBuilder::visitOC_Statement(CypherParser::OC_StatementContext *ctx)  {
  return visitOC_Query(ctx->oC_Query());
}


any ASTBuilder::visitOC_Query(CypherParser::OC_QueryContext *ctx)  {
  if(ctx->oC_RegularQuery())
  {
    return visitOC_RegularQuery(ctx->oC_RegularQuery());
  }
  return visitOC_StandaloneCall(ctx->oC_StandaloneCall());
}


any ASTBuilder::visitOC_RegularQuery(CypherParser::OC_RegularQueryContext *ctx)  {
  if(!ctx->oC_Union().empty())
  {
    auto *node = new ASTInternalNode("UNION");
    node->addElements(any_cast<ASTNode*>(visitOC_SingleQuery(ctx->oC_SingleQuery())));
    for(CypherParser::OC_UnionContext* element : ctx->oC_Union())
    {
      node->addElements(any_cast<ASTNode*>(visitOC_Union(element)));
    }
    return static_cast<ASTNode*>(node);
  }
  return visitOC_SingleQuery(ctx->oC_SingleQuery());
}


any ASTBuilder::visitOC_Union(CypherParser::OC_UnionContext *ctx)  {
  if(ctx->ALL())
  {
    auto *node = new ASTInternalNode("ALL");
    node->addElements(any_cast<ASTNode*>(visitOC_SingleQuery(ctx->oC_SingleQuery())));
    return static_cast<ASTNode*>(node);
  }
  return visitOC_SingleQuery(ctx->oC_SingleQuery());
}


any ASTBuilder::visitOC_SingleQuery(CypherParser::OC_SingleQueryContext *ctx)  {
  if(ctx->oC_MultiPartQuery())
  {
    return visitOC_MultiPartQuery(ctx->oC_MultiPartQuery());
  }
  return visitOC_SinglePartQuery(ctx->oC_SinglePartQuery());
}


any ASTBuilder::visitOC_SinglePartQuery(CypherParser::OC_SinglePartQueryContext *ctx)  {
  auto *queryNode = new ASTInternalNode("SINGLE_QUERY");
  for (CypherParser::OC_ReadingClauseContext* element : ctx->oC_ReadingClause()) {
    queryNode->addElements(any_cast<ASTNode*>(visitOC_ReadingClause(element)));
  }

  for(CypherParser::OC_UpdatingClauseContext* element : ctx->oC_UpdatingClause()){
    queryNode->addElements(any_cast<ASTNode*>(visitOC_UpdatingClause(element)));
  }

  if(ctx->oC_Return())
  {
    queryNode->addElements(any_cast<ASTNode*>(visitOC_Return(ctx->oC_Return())));
  }
  return static_cast<ASTNode*>(queryNode);
}

any ASTBuilder::visitOC_MultiPartQuery(CypherParser::OC_MultiPartQueryContext *ctx) {
  auto *node = new ASTInternalNode("MULTI_PART_QUERY");
  auto *newNode = new ASTInternalNode("SINGLE_QUERY");

  int withIndex = 0;
  int readIndex = 0;
  int updateIndex = 0;

  for (int i = 0; i < ctx->children.size(); i++) {
    auto *child = ctx->children[i];
    string typeName = typeid(*child).name();
    string grammarName = typeName.substr(17);

    if (i + 1 < ctx->children.size()) {
      auto *nextChild = ctx->children[i + 1];
      string nextTypeName = typeid(*nextChild).name();
      string nextGrammarName = nextTypeName.substr(17);

      if (grammarName == "OC_WithContextE" && nextGrammarName == "OC_SinglePartQueryContextE") {
        newNode->addElements(any_cast<ASTNode*>(visitOC_With(ctx->oC_With(withIndex++))));
        node->addElements(newNode);
        break;
      } else if (grammarName == "OC_WithContextE") {
        newNode->addElements(any_cast<ASTNode*>(visitOC_With(ctx->oC_With(withIndex++))));
        node->addElements(newNode);
        newNode = new ASTInternalNode("SINGLE_QUERY");
      } else if (grammarName == "OC_ReadingClauseContextE") {
        newNode->addElements(any_cast<ASTNode*>(visitOC_ReadingClause(ctx->oC_ReadingClause(readIndex++))));
      } else if (grammarName == "OC_UpdatingClauseContextE") {
        newNode->addElements(any_cast<ASTNode*>(visitOC_UpdatingClause(ctx->oC_UpdatingClause(updateIndex++))));
      }
    }
  }

  node->addElements(any_cast<ASTNode*>(visitOC_SinglePartQuery(ctx->oC_SinglePartQuery())));

  return static_cast<ASTNode*>(node);
}



any ASTBuilder::visitOC_UpdatingClause(CypherParser::OC_UpdatingClauseContext *ctx)  {

  if(ctx->oC_Create())
  {
    return visitOC_Create(ctx->oC_Create());
  }else if(ctx->oC_Delete())
  {
    return visitOC_Delete(ctx->oC_Delete());
  }else if(ctx->oC_Merge())
  {
    return visitOC_Merge(ctx->oC_Merge());
  }else if(ctx->oC_Remove())
  {
    return visitOC_Remove(ctx->oC_Remove());
  }
  return visitOC_Set(ctx->oC_Set());
}

any ASTBuilder::visitOC_ReadingClause(CypherParser::OC_ReadingClauseContext *ctx)  {
  if(ctx->oC_Match())
  {
    return visitOC_Match(ctx->oC_Match());
  }else if(ctx->oC_Unwind())
  {
    return visitOC_Unwind(ctx->oC_Unwind());
  }else
  {
    return visitOC_InQueryCall(ctx->oC_InQueryCall());
  }
}


any ASTBuilder::visitOC_Match(CypherParser::OC_MatchContext *ctx)  {
  auto *node = new ASTInternalNode("MATCH");
  if(ctx->OPTIONAL())
  {
    node->addElements(new ASTLeafNoValue("OPTIONAL"));
  }
  node->addElements(any_cast<ASTNode*>(visitOC_Pattern(ctx->oC_Pattern())));
  if (ctx->oC_Where()) {
    node->addElements(any_cast<ASTNode*>(visitOC_Where(ctx->oC_Where())));
  }
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_Unwind(CypherParser::OC_UnwindContext *ctx)  {
  auto *node = new ASTInternalNode("UNWIND");
  auto *nodeAS = new ASTInternalNode("AS");
  nodeAS->addElements(any_cast<ASTNode*>(visitOC_Expression(ctx->oC_Expression())));
  nodeAS->addElements(any_cast<ASTNode*>(visitOC_Variable(ctx->oC_Variable())));
  node->addElements(nodeAS);
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_Merge(CypherParser::OC_MergeContext *ctx)  {
  auto *node = new ASTInternalNode("MERGE");
  node->addElements(any_cast<ASTNode*>(visitOC_PatternPart(ctx->oC_PatternPart())));
  for(CypherParser::OC_MergeActionContext* element : ctx->oC_MergeAction())
  {
    node->addElements(any_cast<ASTNode*>(visitOC_MergeAction(element)));
  }

  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_MergeAction(CypherParser::OC_MergeActionContext *ctx)  {
  if(ctx->CREATE())
  {
    auto *node = new ASTInternalNode("ON_CREATE");
    node->addElements(any_cast<ASTNode*>(visitOC_Set(ctx->oC_Set())));
    return static_cast<ASTNode*>(node);
  }
  auto *node = new ASTInternalNode("ON_MATCH");
  node->addElements(any_cast<ASTNode*>(visitOC_Set(ctx->oC_Set())));
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_Create(CypherParser::OC_CreateContext *ctx)  {
  auto *node = new ASTInternalNode("CREATE");
  node->addElements(any_cast<ASTNode*>(visitOC_Pattern(ctx->oC_Pattern())));
  return static_cast<ASTNode*>(node);
}



any ASTBuilder::visitOC_Set(CypherParser::OC_SetContext *ctx)  {
  if(ctx->oC_SetItem().size()>1)
  {
    auto *node = new ASTInternalNode("MULTIPLE_SET");
    for(CypherParser::OC_SetItemContext* element: ctx->oC_SetItem())
    {
      node->addElements(any_cast<ASTNode*>(visitOC_SetItem(element)));
    }
    return static_cast<ASTNode*>(node);
  }
  return visitOC_SetItem(ctx->oC_SetItem()[0]);
}


any ASTBuilder::visitOC_SetItem(CypherParser::OC_SetItemContext *ctx)  {
  if(ctx->oC_PropertyExpression())
  {
    auto *node = new ASTInternalNode("SET");
    node->addElements(any_cast<ASTNode*>(visitOC_PropertyExpression(ctx->oC_PropertyExpression())));
    node->addElements(any_cast<ASTNode*>(visitOC_Expression(ctx->oC_Expression())));
    return static_cast<ASTNode*>(node);
  } else
  {
    if(ctx->getText().find("+=") != std::string::npos)
    {
      auto *node = new ASTInternalNode("SET_+=");
      node->addElements(any_cast<ASTNode*>(visitOC_Variable(ctx->oC_Variable())));
      node->addElements(any_cast<ASTNode*>(visitOC_Expression(ctx->oC_Expression())));
      return static_cast<ASTNode*>(node);
    }else if (ctx->getText().find("=") != std::string::npos)
    {
      auto *node = new ASTInternalNode("SET_=");
    node->addElements(any_cast<ASTNode*>(visitOC_Variable(ctx->oC_Variable())));
    node->addElements(any_cast<ASTNode*>(visitOC_Expression(ctx->oC_Expression())));
    return static_cast<ASTNode*>(node);
    }
    auto *node = new ASTInternalNode("SET");
    node->addElements(any_cast<ASTNode*>(visitOC_Variable(ctx->oC_Variable())));
    node->addElements(any_cast<ASTNode*>(visitOC_NodeLabels(ctx->oC_NodeLabels())));
    return static_cast<ASTNode*>(node);
  }
}


any ASTBuilder::visitOC_Delete(CypherParser::OC_DeleteContext *ctx)  {
  auto *deleteNode = new ASTInternalNode("DELETE");
  for(CypherParser::OC_ExpressionContext* element : ctx->oC_Expression())
  {
    deleteNode->addElements(any_cast<ASTNode*>(visitOC_Expression(element)));
  }
  if(ctx->DETACH())
  {
    auto *node = new ASTInternalNode("DETACH");
    node->addElements(deleteNode);
    return static_cast<ASTNode*>(node);
  }
  return static_cast<ASTNode*>(deleteNode);
}


any ASTBuilder::visitOC_Remove(CypherParser::OC_RemoveContext *ctx)  {
  if(ctx->oC_RemoveItem().size()>1)
  {
    auto *node = new ASTInternalNode("REMOVE_LIST");
    for(CypherParser::OC_RemoveItemContext* element : ctx->oC_RemoveItem())
    {
      node->addElements(any_cast<ASTNode*>(visitOC_RemoveItem(element)));
    }
    return static_cast<ASTNode*>(node);
  }

  return visitOC_RemoveItem(ctx->oC_RemoveItem()[0]);
}



any ASTBuilder::visitOC_RemoveItem(CypherParser::OC_RemoveItemContext *ctx)  {
  auto *node = new ASTInternalNode("REMOVE");
  if(ctx->oC_Variable())
  {
    node->addElements(any_cast<ASTNode*>(visitOC_Variable(ctx->oC_Variable())));
    node->addElements(any_cast<ASTNode*>(visitOC_NodeLabels(ctx->oC_NodeLabels())));
    return static_cast<ASTNode*>(node);
  }
  node->addElements(any_cast<ASTNode*>(visitOC_PropertyExpression(ctx->oC_PropertyExpression())));
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_InQueryCall(CypherParser::OC_InQueryCallContext *ctx)  {
  auto *node = new ASTInternalNode("CALL");
  node->addElements(any_cast<ASTNode*>(visitOC_ExplicitProcedureInvocation(ctx->oC_ExplicitProcedureInvocation())));

  if(ctx->oC_YieldItems())
  {
    node->addElements(any_cast<ASTNode*>(visitOC_YieldItems(ctx->oC_YieldItems())));
  }
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_StandaloneCall(CypherParser::OC_StandaloneCallContext *ctx)  {
  if(ctx->oC_ExplicitProcedureInvocation())
  {
    auto *node = new ASTInternalNode("CALL");
    node->addElements(any_cast<ASTNode*>(visitOC_ExplicitProcedureInvocation(ctx->oC_ExplicitProcedureInvocation())));
    if(ctx->oC_YieldItems())
    {
      node->addElements(any_cast<ASTNode*>(visitOC_YieldItems(ctx->oC_YieldItems())));
    }else if(ctx->getText().find('*') != std::string::npos)
    {
      auto *star = new ASTLeafNoValue("*");
      node->addElements(star);
    }
    return static_cast<ASTNode*>(node);
  }

  auto *node = new ASTInternalNode("CALL");
  node->addElements(any_cast<ASTNode*>(visitOC_ImplicitProcedureInvocation(ctx->oC_ImplicitProcedureInvocation())));
  if(ctx->oC_YieldItems())
  {
    node->addElements(any_cast<ASTNode*>(visitOC_YieldItems(ctx->oC_YieldItems())));
  }else if(ctx->getText().find('*') != std::string::npos)
  {
    auto *star = new ASTLeafNoValue("*");
    node->addElements(star);
  }
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_YieldItems(CypherParser::OC_YieldItemsContext *ctx)  {

  auto *node = new ASTInternalNode("YIELD_ITEMS");
  for(CypherParser::OC_YieldItemContext* element : ctx->oC_YieldItem())
  {
    node->addElements(any_cast<ASTNode*>(visitOC_YieldItem(element)));
  }
  if(ctx->oC_Where())
  {
    node->addElements(any_cast<ASTNode*>(visitOC_Where(ctx->oC_Where())));
  }
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_YieldItem(CypherParser::OC_YieldItemContext *ctx)  {
  auto *node = new ASTInternalNode("YIELD");
  if(ctx->oC_ProcedureResultField())
  {
    auto *as = new ASTInternalNode("AS");
    as->addElements(any_cast<ASTNode*>(visitOC_ProcedureResultField(ctx->oC_ProcedureResultField())));
    as->addElements(any_cast<ASTNode*>(visitOC_Variable(ctx->oC_Variable())));
    node->addElements(as);
    return static_cast<ASTNode*>(node);
  }
  node->addElements(any_cast<ASTNode*>(visitOC_Variable(ctx->oC_Variable())));
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_With(CypherParser::OC_WithContext *ctx)  {
  auto *node = new ASTInternalNode("WITH");
  node->addElements(any_cast<ASTNode*>(visitOC_ProjectionBody(ctx->oC_ProjectionBody())));
  if(ctx->oC_Where())
  {
    node->addElements(any_cast<ASTNode*>(visitOC_Where(ctx->oC_Where())));
  }
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_Return(CypherParser::OC_ReturnContext *ctx)  {
  return visitOC_ProjectionBody(ctx->oC_ProjectionBody());
}


any ASTBuilder::visitOC_ProjectionBody(CypherParser::OC_ProjectionBodyContext *ctx)  {
  auto *node = new ASTInternalNode("RETURN");
  if(ctx->DISTINCT())
  {
    auto *distinct = new ASTInternalNode("DISTINCT");
    distinct->addElements(any_cast<ASTNode*>(visitOC_ProjectionItems(ctx->oC_ProjectionItems())));
    node->addElements(distinct);
  }else
  {
    node->addElements(any_cast<ASTNode*>(visitOC_ProjectionItems(ctx->oC_ProjectionItems())));
  }
  if(ctx->oC_Order())
  {
    node->addElements(any_cast<ASTNode*>(visitOC_Order(ctx->oC_Order())));
  }
  if(ctx->oC_Skip())
  {
    node->addElements(any_cast<ASTNode*>(visitOC_Skip(ctx->oC_Skip())));
  }
  if(ctx->oC_Limit())
  {
    node->addElements(any_cast<ASTNode*>(visitOC_Limit(ctx->oC_Limit())));
  }
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_ProjectionItems(CypherParser::OC_ProjectionItemsContext *ctx)  {
  auto *node = new ASTInternalNode("RETURN_BODY");
  if(ctx->children[0]->getText() == "*")
  {
    node->addElements(new ASTLeafNoValue("*"));
  }
  for(CypherParser::OC_ProjectionItemContext* element : ctx->oC_ProjectionItem())
  {
    node->addElements(any_cast<ASTNode*>(visitOC_ProjectionItem(element)));
  }
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_ProjectionItem(CypherParser::OC_ProjectionItemContext *ctx)  {
  if(ctx->oC_Variable())
  {
    auto *node = new ASTInternalNode("AS");
    node->addElements(any_cast<ASTNode*>(visitOC_Expression(ctx->oC_Expression())));
    node->addElements(any_cast<ASTNode*>(visitOC_Variable(ctx->oC_Variable())));
    return static_cast<ASTNode*>(node);
  }
  return visitOC_Expression(ctx->oC_Expression());
}


any ASTBuilder::visitOC_Order(CypherParser::OC_OrderContext *ctx)  {
  auto *node = new ASTInternalNode("ORDERED BY");
  for(CypherParser::OC_SortItemContext* element : ctx->oC_SortItem())
  {
    node->addElements(any_cast<ASTNode*>(visitOC_SortItem(element)));
  }
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_Skip(CypherParser::OC_SkipContext *ctx)  {
  auto *node = new ASTInternalNode("SKIP");
  node->addElements(any_cast<ASTNode*>(visitOC_Expression(ctx->oC_Expression())));
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_Limit(CypherParser::OC_LimitContext *ctx)  {
  auto *node = new ASTInternalNode("LIMIT");
  node->addElements(any_cast<ASTNode*>(visitOC_Expression(ctx->oC_Expression())));
  return static_cast<ASTNode*>(node);
}



any ASTBuilder::visitOC_SortItem(CypherParser::OC_SortItemContext *ctx)  {
  if(ctx->ASC() or ctx->ASCENDING())
  {
    auto *node = new ASTInternalNode("ASC");
    node->addElements(any_cast<ASTNode*>(visitOC_Expression(ctx->oC_Expression())));
    return static_cast<ASTNode*>(node);
  }else
  {
    auto *node = new ASTInternalNode("DESC");
    node->addElements(any_cast<ASTNode*>(visitOC_Expression(ctx->oC_Expression())));
    return static_cast<ASTNode*>(node);
  }
}


any ASTBuilder::visitOC_Where(CypherParser::OC_WhereContext *ctx)  {
  auto *node = new ASTInternalNode("WHERE");
  node->addElements(any_cast<ASTNode*>(visitOC_Expression(ctx->oC_Expression())));
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_Pattern(CypherParser::OC_PatternContext *ctx)  {
  auto *node = new ASTInternalNode("PATTERN");
  for(CypherParser::OC_PatternPartContext* element : ctx->oC_PatternPart())
  {
    node->addElements(any_cast<ASTNode*>(visitOC_PatternPart(element)));
  }
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_PatternPart(CypherParser::OC_PatternPartContext *ctx)  {
  if(ctx->oC_Variable())
  {
    auto *node = new ASTInternalNode("=");

    node->addElements(any_cast<ASTNode*>(visitOC_Variable(ctx->oC_Variable())));
    node->addElements(any_cast<ASTNode*>(visitOC_AnonymousPatternPart(ctx->oC_AnonymousPatternPart())));

    return static_cast<ASTNode*>(node);
  }
  return visitOC_AnonymousPatternPart(ctx->oC_AnonymousPatternPart());
}


any ASTBuilder::visitOC_AnonymousPatternPart(CypherParser::OC_AnonymousPatternPartContext *ctx)  {
  return visitOC_PatternElement(ctx->oC_PatternElement());
}


any ASTBuilder::visitOC_PatternElement(CypherParser::OC_PatternElementContext *ctx)  {
  if(ctx->oC_PatternElement())
  {
    return visitOC_PatternElement(ctx->oC_PatternElement());
  }

  if(!ctx->oC_PatternElementChain().empty())
  {
    auto *node = new ASTInternalNode("PATTERN_ELEMENTS");
    node->addElements(any_cast<ASTNode*>(visitOC_NodePattern(ctx->oC_NodePattern())));
    for(CypherParser::OC_PatternElementChainContext* element : ctx->oC_PatternElementChain())
    {
      node->addElements(any_cast<ASTNode*>(visitOC_PatternElementChain(element)));
    }
    return static_cast<ASTNode*>(node);
  }
  return visitOC_NodePattern(ctx->oC_NodePattern());
}

//finsihed
any ASTBuilder::visitOC_RelationshipsPattern(CypherParser::OC_RelationshipsPatternContext *ctx)  {
  auto *node = new ASTInternalNode("PATTERN_ELEMENTS");
  node->addElements(any_cast<ASTNode*>(visitOC_NodePattern(ctx->oC_NodePattern())));
  for(CypherParser::OC_PatternElementChainContext* element : ctx->oC_PatternElementChain())
  {
    node->addElements(any_cast<ASTNode*>(visitOC_PatternElementChain(element)));
  }
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_NodePattern(CypherParser::OC_NodePatternContext *ctx)  {
  auto *node = new ASTInternalNode("NODE_PATTERN");
  if(ctx->oC_Variable())
  {
    node->addElements(any_cast<ASTNode*>(visitOC_Variable(ctx->oC_Variable())));
  }
  if(ctx->oC_NodeLabels())
  {
    node->addElements(any_cast<ASTNode*>(visitOC_NodeLabels(ctx->oC_NodeLabels())));
  }
  if(ctx->oC_Properties())
  {
    node->addElements(any_cast<ASTNode*>(visitOC_Properties(ctx->oC_Properties())));
  }
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_PatternElementChain(CypherParser::OC_PatternElementChainContext *ctx)  {
  auto *node = new ASTInternalNode("PATTERN_ELEMENT_CHAIN");
  node->addElements(any_cast<ASTNode*>(visitOC_RelationshipPattern(ctx->oC_RelationshipPattern())));
  node->addElements(any_cast<ASTNode*>(visitOC_NodePattern(ctx->oC_NodePattern())));
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_RelationshipPattern(CypherParser::OC_RelationshipPatternContext *ctx)  {
  auto *node = new ASTInternalNode("RELATIONSHIP_PATTTERN");
  if(ctx->oC_LeftArrowHead() && !ctx->oC_RightArrowHead())
  {
    node->addElements(any_cast<ASTNode*>(visitOC_LeftArrowHead(ctx->oC_LeftArrowHead())));
  }else if(!ctx->oC_LeftArrowHead() && ctx->oC_RightArrowHead())
  {
    node->addElements(any_cast<ASTNode*>(visitOC_RightArrowHead(ctx->oC_RightArrowHead())));
  }else
  {
    node->addElements(new ASTLeafNoValue("UNIDIRECTION_ARROW"));
  }
  if(ctx->oC_RelationshipDetail())
  {
    node->addElements(any_cast<ASTNode*>(visitOC_RelationshipDetail(ctx->oC_RelationshipDetail())));
  }
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_RelationshipDetail(CypherParser::OC_RelationshipDetailContext *ctx)  {
  auto *node = new ASTInternalNode("RELATIONSHIP_DETAILS");
  if(ctx->oC_Variable())
  {
    node->addElements(any_cast<ASTNode*>(visitOC_Variable(ctx->oC_Variable())));
  }
  if(ctx->oC_RelationshipTypes())
  {
    node->addElements(any_cast<ASTNode*>(visitOC_RelationshipTypes(ctx->oC_RelationshipTypes())));
  }
  if(ctx->oC_RangeLiteral())
  {
    node->addElements(any_cast<ASTNode*>(visitOC_RangeLiteral(ctx->oC_RangeLiteral())));
  }
  if(ctx->oC_Properties())
  {
    node->addElements(any_cast<ASTNode*>(visitOC_Properties(ctx->oC_Properties())));
  }
  return static_cast<ASTNode*>(node); ;
}


any ASTBuilder::visitOC_Properties(CypherParser::OC_PropertiesContext *ctx)  {
  if(ctx->oC_Parameter())
  {
    return visitOC_Parameter(ctx->oC_Parameter());
  }
  return visitOC_MapLiteral(ctx->oC_MapLiteral());
}


any ASTBuilder::visitOC_RelationshipTypes(CypherParser::OC_RelationshipTypesContext *ctx)  {

  if(ctx->oC_RelTypeName().size()>1)
  {
    auto *node = new ASTInternalNode("RELATIONSHIP_TYPES");
    for(CypherParser::OC_RelTypeNameContext* element : ctx->oC_RelTypeName())
    {
      node->addElements(any_cast<ASTNode*>(visitOC_RelTypeName(element)));
    }
    return static_cast<ASTNode*>(node);
  }
  return visitOC_RelTypeName(ctx->oC_RelTypeName()[0]);
}


any ASTBuilder::visitOC_NodeLabels(CypherParser::OC_NodeLabelsContext *ctx)  {
  if(ctx->oC_NodeLabel().size()>1)
  {
    auto *node = new ASTInternalNode("NODE_LABELS");
    for(CypherParser::OC_NodeLabelContext* element : ctx->oC_NodeLabel())
    {
      node->addElements(any_cast<ASTNode*>(visitOC_NodeLabel(element)));
    }
    return static_cast<ASTNode*>(node);
  }
  return visitOC_NodeLabel(ctx->oC_NodeLabel()[0]);
}


any ASTBuilder::visitOC_NodeLabel(CypherParser::OC_NodeLabelContext *ctx)  {
  auto *node = new ASTInternalNode("NODE_LABEL");
  node->addElements(any_cast<ASTNode*>(visitOC_LabelName(ctx->oC_LabelName())));
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_RangeLiteral(CypherParser::OC_RangeLiteralContext *ctx)  {
  auto *node = new ASTInternalNode("RANGE");
  if(ctx->oC_IntegerLiteral().size()>1)
  {
    node->addElements(any_cast<ASTNode*>(visitOC_IntegerLiteral(ctx->oC_IntegerLiteral()[0])));
    node->addElements(any_cast<ASTNode*>(visitOC_IntegerLiteral(ctx->oC_IntegerLiteral()[1])));
    return static_cast<ASTNode*>(node);
  }
  node->addElements(any_cast<ASTNode*>(visitOC_IntegerLiteral(ctx->oC_IntegerLiteral()[0])));
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_LabelName(CypherParser::OC_LabelNameContext *ctx)  {
  return visitOC_SchemaName(ctx->oC_SchemaName());
}


any ASTBuilder::visitOC_RelTypeName(CypherParser::OC_RelTypeNameContext *ctx)  {
  return visitOC_SchemaName(ctx->oC_SchemaName());
}


any ASTBuilder::visitOC_PropertyExpression(CypherParser::OC_PropertyExpressionContext *ctx)  {
  auto *node = new ASTInternalNode("PROPERTY");
  node->addElements(any_cast<ASTNode*>(visitOC_Atom(ctx->oC_Atom())));
  for(CypherParser::OC_PropertyLookupContext* element : ctx->oC_PropertyLookup())
  {
    node->addElements(any_cast<ASTNode*>(visitOC_PropertyLookup(element)));
  }
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_Expression(CypherParser::OC_ExpressionContext *ctx)  {
  return visitOC_OrExpression(ctx->oC_OrExpression());
}


any ASTBuilder::visitOC_OrExpression(CypherParser::OC_OrExpressionContext *ctx)  {
  if(ctx->oC_XorExpression().size()>1)
  {
    auto *node = new ASTInternalNode("OR");
    for(CypherParser::OC_XorExpressionContext* element : ctx->oC_XorExpression())
    {
      node->addElements(any_cast<ASTNode*>(visitOC_XorExpression(element)));
    }
    return static_cast<ASTNode*>(node);
  }
  return visitOC_XorExpression(ctx->oC_XorExpression()[0]);
}


any ASTBuilder::visitOC_XorExpression(CypherParser::OC_XorExpressionContext *ctx)  {
  if(ctx->oC_AndExpression().size()>1)
  {
    auto *node = new ASTInternalNode("XOR");
    for(CypherParser::OC_AndExpressionContext* element : ctx->oC_AndExpression())
    {
      node->addElements(any_cast<ASTNode*>(visitOC_AndExpression(element)));
    }
  }
  return visitOC_AndExpression(ctx->oC_AndExpression()[0]);
}


any ASTBuilder::visitOC_AndExpression(CypherParser::OC_AndExpressionContext *ctx)  {
  if(ctx->oC_NotExpression().size()>1)
  {
    auto *node = new ASTInternalNode("AND");
    for(CypherParser::OC_NotExpressionContext* element : ctx->oC_NotExpression())
    {
      node->addElements(any_cast<ASTNode*>(visitOC_NotExpression(element)));
    }
    return static_cast<ASTNode*>(node);
  }
  return visitOC_NotExpression(ctx->oC_NotExpression()[0]);
}


any ASTBuilder::visitOC_NotExpression(CypherParser::OC_NotExpressionContext *ctx)  {
  if(!ctx->NOT().empty())
  {
    if(ctx->NOT().size() % 2 != 0)
    {
      auto *node = new ASTInternalNode("NOT");
      node->addElements(any_cast<ASTNode*>(visitOC_ComparisonExpression(ctx->oC_ComparisonExpression())));
      return static_cast<ASTNode*>(node);
    }
  }
  return visitOC_ComparisonExpression(ctx->oC_ComparisonExpression());
}



any ASTBuilder::visitOC_ComparisonExpression(CypherParser::OC_ComparisonExpressionContext *ctx)  {
  if(!ctx->oC_PartialComparisonExpression().empty())
  {
    auto *node = new ASTInternalNode("COMPARISON");
    node->addElements(any_cast<ASTNode*>(visitOC_StringListNullPredicateExpression(ctx->oC_StringListNullPredicateExpression())));
    for(CypherParser::OC_PartialComparisonExpressionContext* element : ctx->oC_PartialComparisonExpression())
    {
      node->addElements(any_cast<ASTNode*>(visitOC_PartialComparisonExpression(element)));
    }
    return static_cast<ASTNode*>(node);
  }
  return visitOC_StringListNullPredicateExpression(ctx->oC_StringListNullPredicateExpression());
}


any ASTBuilder::visitOC_PartialComparisonExpression(CypherParser::OC_PartialComparisonExpressionContext *ctx)  {
  if(ctx->getText().find(">") != string::npos)
  {
    auto *node = new ASTInternalNode(">");
    node->addElements(any_cast<ASTNode*>(visitOC_StringListNullPredicateExpression(ctx->oC_StringListNullPredicateExpression())));
    return static_cast<ASTNode*>(node);
  }else if(ctx->getText().find("<>") != string::npos)
  {
    auto *node = new ASTInternalNode("<>");
    node->addElements(any_cast<ASTNode*>(visitOC_StringListNullPredicateExpression(ctx->oC_StringListNullPredicateExpression())));
    return static_cast<ASTNode*>(node);
  }else if(ctx->getText().find("=") != string::npos)
  {
    auto *node = new ASTInternalNode("==");
    node->addElements(any_cast<ASTNode*>(visitOC_StringListNullPredicateExpression(ctx->oC_StringListNullPredicateExpression())));
    return static_cast<ASTNode*>(node);
  }else if(ctx->getText().find("<") != string::npos)
  {
    auto *node = new ASTInternalNode("<");
    node->addElements(any_cast<ASTNode*>(visitOC_StringListNullPredicateExpression(ctx->oC_StringListNullPredicateExpression())));
    return static_cast<ASTNode*>(node);
  }else if(ctx->getText().find(">=") != string::npos)
  {
    auto *node = new ASTInternalNode(">=");
    node->addElements(any_cast<ASTNode*>(visitOC_StringListNullPredicateExpression(ctx->oC_StringListNullPredicateExpression())));
    return static_cast<ASTNode*>(node);
  }else
  {
    auto *node = new ASTInternalNode("<=");
    node->addElements(any_cast<ASTNode*>(visitOC_StringListNullPredicateExpression(ctx->oC_StringListNullPredicateExpression())));
    return static_cast<ASTNode*>(node);
  }
}


any ASTBuilder::visitOC_StringListNullPredicateExpression(CypherParser::OC_StringListNullPredicateExpressionContext *ctx)  {
  auto *node = new ASTInternalNode("PREDICATE_EXPRESSIONS");
  node->addElements(any_cast<ASTNode*>(visitOC_AddOrSubtractExpression(ctx->oC_AddOrSubtractExpression())));
  if(!ctx->oC_StringPredicateExpression().empty())
  {
    auto *strPredicate = new ASTInternalNode("STRING_PREDICATES");
    for(CypherParser::OC_StringPredicateExpressionContext* element: ctx->oC_StringPredicateExpression())
    {
      strPredicate->addElements(any_cast<ASTNode*>(visitOC_StringPredicateExpression(element)));
    }
    node->addElements(strPredicate);
  }
  if(!ctx->oC_ListPredicateExpression().empty())
  {
    auto *listPredicate = new ASTInternalNode("LIST_PREDICATES");
    for(CypherParser::OC_ListPredicateExpressionContext* element: ctx->oC_ListPredicateExpression())
    {
      listPredicate->addElements(any_cast<ASTNode*>(visitOC_ListPredicateExpression(element)));
    }
    node->addElements(listPredicate);
  }
  if(!ctx->oC_NullPredicateExpression().empty())
  {
    auto *nullPredicate = new ASTInternalNode("NULL_PREDICATES");
    for(CypherParser::OC_NullPredicateExpressionContext* element: ctx->oC_NullPredicateExpression())
    {
      nullPredicate->addElements(any_cast<ASTNode*>(visitOC_NullPredicateExpression(element)));
    }
    node->addElements(nullPredicate);
  }
  if(node->elements.size()>1)
  {
    return static_cast<ASTNode*>(node);
  }else
  {
    return visitOC_AddOrSubtractExpression(ctx->oC_AddOrSubtractExpression());
  }
}


any ASTBuilder::visitOC_StringPredicateExpression(CypherParser::OC_StringPredicateExpressionContext *ctx)  {
  if(ctx->STARTS())
  {
    auto *node = new ASTInternalNode("STARTS_WITH");
    node->addElements(any_cast<ASTNode*>(visitOC_AddOrSubtractExpression(ctx->oC_AddOrSubtractExpression())));
    return static_cast<ASTNode*>(node);
  }else if(ctx->ENDS())
  {
    auto *node = new ASTInternalNode("ENDS_WITH");
    node->addElements(any_cast<ASTNode*>(visitOC_AddOrSubtractExpression(ctx->oC_AddOrSubtractExpression())));
    return static_cast<ASTNode*>(node);
  }else
  {
    auto *node = new ASTInternalNode("CONTAINS");
    node->addElements(any_cast<ASTNode*>(visitOC_AddOrSubtractExpression(ctx->oC_AddOrSubtractExpression())));
    return static_cast<ASTNode*>(node);
  }
}



any ASTBuilder::visitOC_ListPredicateExpression(CypherParser::OC_ListPredicateExpressionContext *ctx)  {
  auto *node = new ASTInternalNode("IN");
  node->addElements(any_cast<ASTNode*>(visitOC_AddOrSubtractExpression(ctx->oC_AddOrSubtractExpression())));
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_NullPredicateExpression(CypherParser::OC_NullPredicateExpressionContext *ctx)  {
  if(ctx->NOT())
  {
    auto *node = new ASTLeafNoValue("IS_NOT_NULL");
    return static_cast<ASTNode*>(node);
  }
  auto *node = new ASTLeafNoValue("IS_NULL");
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_AddOrSubtractExpression(CypherParser::OC_AddOrSubtractExpressionContext *ctx)  {
  if(ctx->oC_MultiplyDivideModuloExpression().size()>1)
  {
    int multIndex =1;
    auto *node = new ASTInternalNode("ADD_OR_SUBSTRACT");
    node->addElements(any_cast<ASTNode*>(visitOC_MultiplyDivideModuloExpression(ctx->oC_MultiplyDivideModuloExpression(0))));

    for(int i=0; i<ctx->children.size();i++)
    {
      if(ctx->children[i]->getText() == "+")
      {
        auto *plus = new ASTInternalNode("+");
        plus->addElements(any_cast<ASTNode*>(visitOC_MultiplyDivideModuloExpression(ctx->oC_MultiplyDivideModuloExpression(multIndex++))));
        node->addElements(plus);
      }else if(ctx->children[i]->getText() == "-")
      {
        auto *minus = new ASTInternalNode("-");
        minus->addElements(any_cast<ASTNode*>(visitOC_MultiplyDivideModuloExpression(ctx->oC_MultiplyDivideModuloExpression(multIndex++))));
        node->addElements(minus);
      }
    }
    return static_cast<ASTNode*>(node);
  }
  return visitOC_MultiplyDivideModuloExpression(ctx->oC_MultiplyDivideModuloExpression(0));
}


any ASTBuilder::visitOC_MultiplyDivideModuloExpression(CypherParser::OC_MultiplyDivideModuloExpressionContext *ctx)  {
  if(ctx->oC_PowerOfExpression().size()>1)
  {
    int powIndex = 1;
    auto *node = new ASTInternalNode("MULTIPLY_DIVID_MODULO");
    node->addElements(any_cast<ASTNode*>(visitOC_PowerOfExpression(ctx->oC_PowerOfExpression(0))));

    for(int i=0; i<ctx->children.size();i++)
    {
      if(ctx->children[i]->getText() == "*")
      {
        auto *plus = new ASTInternalNode("*");
        plus->addElements(any_cast<ASTNode*>(visitOC_PowerOfExpression(ctx->oC_PowerOfExpression(powIndex++))));
        node->addElements(plus);
      }else if(ctx->children[i]->getText() == "/")
      {
        auto *minus = new ASTInternalNode("/");
        minus->addElements(any_cast<ASTNode*>(visitOC_PowerOfExpression(ctx->oC_PowerOfExpression(powIndex++))));
        node->addElements(minus);
      }
    }
    return static_cast<ASTNode*>(node);
  }

  return visitOC_PowerOfExpression(ctx->oC_PowerOfExpression(0));
}


any ASTBuilder::visitOC_PowerOfExpression(CypherParser::OC_PowerOfExpressionContext *ctx)  {
  if(ctx->oC_UnaryAddOrSubtractExpression().size()>1)
  {
    int unaryIndex = 1;
    auto *node = new ASTInternalNode("POWER_OF");
    node->addElements(any_cast<ASTNode*>(visitOC_UnaryAddOrSubtractExpression(ctx->oC_UnaryAddOrSubtractExpression(0))));

    for(int i=0; i<ctx->children.size();i++)
    {
      if(ctx->children[i]->getText() == "^")
      {
        auto *plus = new ASTInternalNode("^");
        plus->addElements(any_cast<ASTNode*>(visitOC_UnaryAddOrSubtractExpression(ctx->oC_UnaryAddOrSubtractExpression(unaryIndex++))));
        node->addElements(plus);
      }
    }
    return static_cast<ASTNode*>(node);
  }
  return visitOC_UnaryAddOrSubtractExpression(ctx->oC_UnaryAddOrSubtractExpression(0));
}



any ASTBuilder::visitOC_UnaryAddOrSubtractExpression(CypherParser::OC_UnaryAddOrSubtractExpressionContext *ctx)  {
  if(ctx->children[0]->getText() == "+")
  {
    auto *node = new ASTInternalNode("UNARY_+");
    node->addElements(any_cast<ASTNode*>(visitOC_NonArithmeticOperatorExpression(ctx->oC_NonArithmeticOperatorExpression())));
    return static_cast<ASTNode*>(node);
  }else if(ctx->children[0]->getText() == "-")
  {
    auto *node = new ASTInternalNode("UNARY_-");
    node->addElements(any_cast<ASTNode*>(visitOC_NonArithmeticOperatorExpression(ctx->oC_NonArithmeticOperatorExpression())));
    return static_cast<ASTNode*>(node);
  }else
  {
    return visitOC_NonArithmeticOperatorExpression(ctx->oC_NonArithmeticOperatorExpression());
  }
}


any ASTBuilder::visitOC_NonArithmeticOperatorExpression(CypherParser::OC_NonArithmeticOperatorExpressionContext *ctx)  {
  auto *node = new ASTInternalNode("NON_ARITHMETIC_OPERATOR");
  if(!ctx->oC_ListOperatorExpression().empty() | !ctx->oC_PropertyLookup().empty())
  {
    int i = 0;
    int j = 0;
    for(auto *child : ctx->children)
    {
      std::string typeName = typeid(*child).name();
      if(typeName.substr(17) == "OC_AtomContextE")
      {
        node->addElements(any_cast<ASTNode*>(visitOC_Atom(ctx->oC_Atom())));
      }else if(typeName.substr(17) == "OC_PropertyLookupContextE")
      {
        node->addElements(any_cast<ASTNode*>(visitOC_PropertyLookup(ctx->oC_PropertyLookup(i++))));
      }else if(typeName.substr(17) == "OC_ListOperatorExpressionContextE")
      {
        node->addElements(any_cast<ASTNode*>(visitOC_ListOperatorExpression(ctx->oC_ListOperatorExpression(j++))));
      }else
      {
        node->addElements(any_cast<ASTNode*>(visitOC_NodeLabels(ctx->oC_NodeLabels())));
      }
    }
    return static_cast<ASTNode*>(node);
  }else if(ctx->oC_NodeLabels())
  {
    node->addElements(any_cast<ASTNode*>(visitOC_Atom(ctx->oC_Atom())));
    node->addElements(any_cast<ASTNode*>(visitOC_NodeLabels(ctx->oC_NodeLabels())));
    return static_cast<ASTNode*>(node);
  }
  return visitOC_Atom(ctx->oC_Atom());
}


any ASTBuilder::visitOC_ListOperatorExpression(CypherParser::OC_ListOperatorExpressionContext *ctx)  {
  if(ctx->oC_Expression().size() == 2)
  {
    auto *node = new ASTInternalNode("LIST_INDEX_RANGE");
    node->addElements(any_cast<ASTNode*>(visitOC_Expression(ctx->oC_Expression(0))));
    node->addElements(any_cast<ASTNode*>(visitOC_Expression(ctx->oC_Expression(1))));
    return static_cast<ASTNode*>(node);
  }
  auto *node = new ASTInternalNode("LIST_INDEX");
  node->addElements(any_cast<ASTNode*>(visitOC_Expression(ctx->oC_Expression(0))));
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_PropertyLookup(CypherParser::OC_PropertyLookupContext *ctx)  {
  auto *node = new ASTInternalNode("PROPERTY_LOOKUP");
  node->addElements(any_cast<ASTNode*>(visitOC_PropertyKeyName(ctx->oC_PropertyKeyName())));
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_Atom(CypherParser::OC_AtomContext *ctx)  {
  if(ctx->oC_Literal())
  {
    return visitOC_Literal(ctx->oC_Literal());
  }else if(ctx->oC_Parameter())
  {
    return visitOC_Parameter(ctx->oC_Parameter());
  }else if(ctx->oC_Quantifier())
  {
    return visitOC_Quantifier(ctx->oC_Quantifier());
  }else if(ctx->oC_Variable())
  {
    return visitOC_Variable(ctx->oC_Variable());
  }else if(ctx->oC_CaseExpression())
  {
    return visitOC_CaseExpression(ctx->oC_CaseExpression());
  }else if(ctx->oC_ExistentialSubquery())
  {
    return visitOC_ExistentialSubquery(ctx->oC_ExistentialSubquery());
  }else if(ctx->oC_FunctionInvocation())
  {
    return visitOC_FunctionInvocation(ctx->oC_FunctionInvocation());
  }else if(ctx->oC_ListComprehension())
  {
    return visitOC_ListComprehension(ctx->oC_ListComprehension());
  }else if(ctx->oC_ParenthesizedExpression())
  {
    return visitOC_ParenthesizedExpression(ctx->oC_ParenthesizedExpression());
  }else if(ctx->oC_PatternComprehension())
  {
    return visitOC_PatternComprehension(ctx->oC_PatternComprehension());
  }else if(ctx->oC_PatternPredicate())
  {
    return visitOC_PatternPredicate(ctx->oC_PatternPredicate());
  }else
  {
    auto *node = new ASTLeafValue("COUNT", "*");
    return static_cast<ASTNode*>(node);
  }
}


any ASTBuilder::visitOC_CaseExpression(CypherParser::OC_CaseExpressionContext *ctx)  {
  auto *caseNode = new ASTInternalNode("CASE_PATTERN");
  int caseAlt = 0;
  for(int i = 0; i<ctx->children.size(); i++)
  {
    string text = ctx->children[i]->getText();
    std::transform(text.begin(), text.end(), text.begin(), ::toupper);
    string type;
    if(i+2 < ctx->children.size())
    {
      type = typeid(*ctx->children[i+2]).name();
    }
    if(text == "CASE" && type.substr(17) == "OC_ExpressionContextE")
    {
      auto *node = new ASTInternalNode("CASE_EXPRESSION");
      node->addElements(any_cast<ASTNode*>(visitOC_Expression(ctx->oC_Expression(0))));
      caseNode->addElements(node);
    }else if(text == "ELSE" && ctx->oC_Expression().size()>1)
    {
      auto *node = new ASTInternalNode("ELSE_EXPRESSION");
      node->addElements(any_cast<ASTNode*>(visitOC_Expression(ctx->oC_Expression(1))));
      caseNode->addElements(node);
    }else if(text == "ELSE")
    {
      auto *node = new ASTInternalNode("ELSE_EXPRESSION");
      node->addElements(any_cast<ASTNode*>(visitOC_Expression(ctx->oC_Expression(0))));
      caseNode->addElements(node);
    }else if(text.find("WHEN") != string::npos)
    {
      caseNode->addElements(any_cast<ASTNode*>(visitOC_CaseAlternative(ctx->oC_CaseAlternative(caseAlt++))));
    }
  }
  return static_cast<ASTNode*>(caseNode);
}


any ASTBuilder::visitOC_CaseAlternative(CypherParser::OC_CaseAlternativeContext *ctx)  {
  auto *node = new ASTInternalNode("CASE");

  auto *when = new ASTInternalNode("WHEN");
  when->addElements(any_cast<ASTNode*>(visitOC_Expression(ctx->oC_Expression(0))));

  auto *then = new ASTInternalNode("THEN");
  then->addElements(any_cast<ASTNode*>(visitOC_Expression(ctx->oC_Expression(1))));

  node->addElements(when);
  node->addElements(then);
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_ListComprehension(CypherParser::OC_ListComprehensionContext *ctx)  {
  auto *node = new ASTInternalNode("LIST_COMPREHENSION");
  node->addElements(any_cast<ASTNode*>(visitOC_FilterExpression(ctx->oC_FilterExpression())));
  if(ctx->getText().find('|') != string::npos)
  {
    auto *result = new ASTInternalNode("FILTER_RESULT");
    result->addElements(any_cast<ASTNode*>(visitOC_Expression(ctx->oC_Expression())));
    node->addElements(result);
  }
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_PatternComprehension(CypherParser::OC_PatternComprehensionContext *ctx)  {
  auto *node = new ASTInternalNode("PATTERN_COMPREHENSION");
  if(ctx->oC_Variable())
  {
    auto *eual = new ASTInternalNode("=");
    eual->addElements(any_cast<ASTNode*>(visitOC_Variable(ctx->oC_Variable())));
    eual->addElements(any_cast<ASTNode*>(visitOC_RelationshipsPattern(ctx->oC_RelationshipsPattern())));
    node->addElements(eual);
  }else
  {
    node->addElements(any_cast<ASTNode*>(visitOC_RelationshipsPattern(ctx->oC_RelationshipsPattern())));
  }
  if(ctx->oC_Where())
  {
    node->addElements(any_cast<ASTNode*>(visitOC_Where(ctx->oC_Where())));
  }
  auto *result = new ASTInternalNode("FILTER_RESULT");
  result->addElements(any_cast<ASTNode*>(visitOC_Expression(ctx->oC_Expression())));
  node->addElements(result);
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_Quantifier(CypherParser::OC_QuantifierContext *ctx)  {
  if(ctx->ALL())
  {
    auto *node = new ASTInternalNode("ALL");
    node->addElements(any_cast<ASTNode*>(visitOC_FilterExpression(ctx->oC_FilterExpression())));
    return static_cast<ASTNode*>(node);
  }else if(ctx->ANY())
  {
    auto *node = new ASTInternalNode("ANY");
    node->addElements(any_cast<ASTNode*>(visitOC_FilterExpression(ctx->oC_FilterExpression())));
    return static_cast<ASTNode*>(node);
  }else if(ctx->NONE())
  {
    auto *node = new ASTInternalNode("NONE");
    node->addElements(any_cast<ASTNode*>(visitOC_FilterExpression(ctx->oC_FilterExpression())));
    return static_cast<ASTNode*>(node);
  }else
  {
    auto *node = new ASTInternalNode("SINGLE");
    node->addElements(any_cast<ASTNode*>(visitOC_FilterExpression(ctx->oC_FilterExpression())));
    return static_cast<ASTNode*>(node);
  }
}


any ASTBuilder::visitOC_FilterExpression(CypherParser::OC_FilterExpressionContext *ctx)  {
  auto *node = new ASTInternalNode("FILTER_EXPRESSION");
  node->addElements(any_cast<ASTNode*>(visitOC_IdInColl(ctx->oC_IdInColl())));
  if(ctx->oC_Where())
  {
    node->addElements(any_cast<ASTNode*>(visitOC_Where(ctx->oC_Where())));
  }
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_PatternPredicate(CypherParser::OC_PatternPredicateContext *ctx)  {
  return visitOC_RelationshipsPattern(ctx->oC_RelationshipsPattern());
}


any ASTBuilder::visitOC_ParenthesizedExpression(CypherParser::OC_ParenthesizedExpressionContext *ctx)  {
  return visitOC_Expression(ctx->oC_Expression());
}


any ASTBuilder::visitOC_IdInColl(CypherParser::OC_IdInCollContext *ctx)  {
  auto *node = new ASTInternalNode("LIST_ITERATE");
  node->addElements(any_cast<ASTNode*>(visitOC_Variable(ctx->oC_Variable())));
  node->addElements(any_cast<ASTNode*>(visitOC_Expression(ctx->oC_Expression())));
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_FunctionInvocation(CypherParser::OC_FunctionInvocationContext *ctx)  {
  auto *node = new ASTInternalNode("FUNCTION_BODY");
  node->addElements(any_cast<ASTNode*>(visitOC_FunctionName(ctx->oC_FunctionName())));

  auto *aug = new ASTInternalNode("ARGUMENTS");
  if(ctx->DISTINCT())
  {
    auto *distinct = new ASTInternalNode("DISTINCT");
    for(CypherParser::OC_ExpressionContext* elements : ctx->oC_Expression())
    {
      distinct->addElements(any_cast<ASTNode*>(visitOC_Expression(elements)));
    }
    aug->addElements(distinct);
    node->addElements(aug);
    return static_cast<ASTNode*>(node);
  }

  for(CypherParser::OC_ExpressionContext* elements : ctx->oC_Expression())
  {
    aug->addElements(any_cast<ASTNode*>(visitOC_Expression(elements)));
  }
  if(!aug->elements.empty())
  {
    node->addElements(aug);
  }
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_FunctionName(CypherParser::OC_FunctionNameContext *ctx)  {
  auto *node = new ASTInternalNode("FUNCTION");
  node->addElements(any_cast<ASTNode*>(visitOC_Namespace(ctx->oC_Namespace())));
  auto *name = new ASTLeafValue("FUNCTION_NAME", any_cast<string>(visitOC_SymbolicName(ctx->oC_SymbolicName())));
  node->addElements(name);
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_ExistentialSubquery(CypherParser::OC_ExistentialSubqueryContext *ctx)  {
  auto *node = new ASTInternalNode("EXISTS");
  if(ctx->oC_RegularQuery())
  {
    node->addElements(any_cast<ASTNode*>(visitOC_RegularQuery(ctx->oC_RegularQuery())));
    return static_cast<ASTNode*>(node);
  }else
  {
    node->addElements(any_cast<ASTNode*>(visitOC_Pattern(ctx->oC_Pattern())));
    if(ctx->oC_Where())
    {
      node->addElements(any_cast<ASTNode*>(visitOC_Where(ctx->oC_Where())));
    }
    return static_cast<ASTNode*>(node);
  }
}


any ASTBuilder::visitOC_ExplicitProcedureInvocation(CypherParser::OC_ExplicitProcedureInvocationContext *ctx)  {
  auto *node = new ASTInternalNode("EXPLICIT_PROCEDURE");
  node->addElements(any_cast<ASTNode*>(visitOC_ProcedureName(ctx->oC_ProcedureName())));
  if(ctx->oC_Expression().size()>1)
  {
    auto *arg = new ASTInternalNode("ARGUMENTS");
    for(CypherParser::OC_ExpressionContext* elements : ctx->oC_Expression())
    {
      arg->addElements(any_cast<ASTNode*>(visitOC_Expression(elements)));
    }
    node->addElements(arg);
    return static_cast<ASTNode*>(node);
  }
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_ImplicitProcedureInvocation(CypherParser::OC_ImplicitProcedureInvocationContext *ctx)  {
  auto *node = new ASTInternalNode("IMPLICIT_PROCEDURE");
  node->addElements(any_cast<ASTNode*>(visitOC_ProcedureName(ctx->oC_ProcedureName())));
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_ProcedureResultField(CypherParser::OC_ProcedureResultFieldContext *ctx)  {
  auto *node = new ASTLeafValue("PROCEDURE_RESULT", any_cast<string>(visitOC_SymbolicName(ctx->oC_SymbolicName())));
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_ProcedureName(CypherParser::OC_ProcedureNameContext *ctx)  {
  auto *node = new ASTInternalNode("PROCEDURE");
  node->addElements(any_cast<ASTNode*>(visitOC_Namespace(ctx->oC_Namespace())));
  auto *name = new ASTLeafValue("PROCEDURE_NAME", any_cast<string>(visitOC_SymbolicName(ctx->oC_SymbolicName())));
  node->addElements(name);
  return static_cast<ASTNode*>(node);
}



any ASTBuilder::visitOC_Namespace(CypherParser::OC_NamespaceContext *ctx)  {
  auto *node = new ASTInternalNode("NAMESPACE");
  for(CypherParser::OC_SymbolicNameContext* element : ctx->oC_SymbolicName())
  {
    auto *name = new ASTLeafNoValue(any_cast<string>(visitOC_SymbolicName(element)));
    node->addElements(name);
  }
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_Variable(CypherParser::OC_VariableContext *ctx)  {
  auto *node = new ASTLeafValue("VARIABLE", any_cast<string>(visitOC_SymbolicName(ctx->oC_SymbolicName())));
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_Literal(CypherParser::OC_LiteralContext *ctx)  {
  if(ctx->oC_BooleanLiteral())
  {
    return visitOC_BooleanLiteral(ctx->oC_BooleanLiteral());
  }else if(ctx->oC_ListLiteral())
  {
    return visitOC_ListLiteral(ctx->oC_ListLiteral());
  }else if(ctx->oC_MapLiteral())
  {
    return visitOC_MapLiteral(ctx->oC_MapLiteral());
  }else if(ctx->oC_NumberLiteral())
  {
    return visitOC_NumberLiteral(ctx->oC_NumberLiteral());
  }else if(ctx->NULL_())
  {
    auto *node = new ASTLeafNoValue("NULL");
    return static_cast<ASTNode*>(node);
  }else
  {
    auto *node = new ASTLeafValue("STRING", ctx->getText());
    return static_cast<ASTNode*>(node);
  }
}


any ASTBuilder::visitOC_BooleanLiteral(CypherParser::OC_BooleanLiteralContext *ctx)  {
  if(ctx->TRUE())
  {
    auto *node = new ASTLeafValue("BOOLEAN", "TRUE");
    return static_cast<ASTNode*>(node);
  } else
  {
    auto *node = new ASTLeafValue("BOOLEAN", "FALSE");
    return static_cast<ASTNode*>(node);
  }
}


any ASTBuilder::visitOC_NumberLiteral(CypherParser::OC_NumberLiteralContext *ctx)  {
  if(ctx->oC_DoubleLiteral())
  {
    return visitOC_DoubleLiteral(ctx->oC_DoubleLiteral());
  }
  return visitOC_IntegerLiteral(ctx->oC_IntegerLiteral());
}


any ASTBuilder::visitOC_IntegerLiteral(CypherParser::OC_IntegerLiteralContext *ctx)  {
  if(ctx->DecimalInteger())
  {
    auto *node = new ASTLeafValue("DECIMAL", ctx->getText());
    return static_cast<ASTNode*>(node);
  } else if(ctx->HexInteger())
  {
    auto *node = new ASTLeafValue("HEX", ctx->getText());
    return static_cast<ASTNode*>(node);
  }else
  {
    auto *node = new ASTLeafValue("OCTAL", ctx->getText());
    return static_cast<ASTNode*>(node);
  }

}


any ASTBuilder::visitOC_DoubleLiteral(CypherParser::OC_DoubleLiteralContext *ctx)  {
  if(ctx->ExponentDecimalReal())
  {
    auto *node = new ASTLeafValue("EXP_DECIMAL", ctx->getText());
    return static_cast<ASTNode*>(node);
  }else
  {
    auto *node = new ASTLeafValue("REGULAR_DECIMAL", ctx->getText());
    return static_cast<ASTNode*>(node);
  }
}


any ASTBuilder::visitOC_ListLiteral(CypherParser::OC_ListLiteralContext *ctx)  {
  auto *node = new ASTInternalNode("LIST");
  for(CypherParser::OC_ExpressionContext* element: ctx->oC_Expression())
  {
    node->addElements(any_cast<ASTNode*>(visitOC_Expression(element)));
  }
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_MapLiteral(CypherParser::OC_MapLiteralContext *ctx)  {
  auto *node = new ASTInternalNode("PROPERIES_MAP");
  for(int i=0; i<ctx->oC_PropertyKeyName().size();i++)
  {
    auto *propNode = new ASTInternalNode("PROPERTY");
    propNode->addElements(any_cast<ASTNode*>(visitOC_PropertyKeyName(ctx->oC_PropertyKeyName()[i])));
    propNode->addElements(any_cast<ASTNode*>(visitOC_Expression(ctx->oC_Expression()[i])));
    node->addElements(propNode);
  }

  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_PropertyKeyName(CypherParser::OC_PropertyKeyNameContext *ctx)  {
  return visitOC_SchemaName(ctx->oC_SchemaName());
}


any ASTBuilder::visitOC_Parameter(CypherParser::OC_ParameterContext *ctx)  {
  if(ctx->oC_SymbolicName())
  {
    auto *node = new ASTLeafValue("PARAMETER", any_cast<string>(visitOC_SymbolicName(ctx->oC_SymbolicName())));
    return static_cast<ASTNode*>(node);
  }else
  {
    auto *node = new ASTLeafValue("PARAMETER", ctx->getText());
    return static_cast<ASTNode*>(node);
  }
}


any ASTBuilder::visitOC_SchemaName(CypherParser::OC_SchemaNameContext *ctx)  {
  if(ctx->oC_ReservedWord())
  {
    return visitOC_ReservedWord(ctx->oC_ReservedWord());
  }
  auto *node = new ASTLeafValue("SYMBOLIC_WORD",any_cast<string>(visitOC_SymbolicName(ctx->oC_SymbolicName())));
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_ReservedWord(CypherParser::OC_ReservedWordContext *ctx)  {
  auto *node = new ASTLeafValue("RESERVED_WORD", ctx->getText());
  return static_cast<ASTNode*>(node);
}


any ASTBuilder::visitOC_SymbolicName(CypherParser::OC_SymbolicNameContext *ctx)  {
  return ctx->getText();
}


any ASTBuilder::visitOC_LeftArrowHead(CypherParser::OC_LeftArrowHeadContext *ctx)  {
  return static_cast<ASTNode*>(new ASTLeafNoValue("LEFT_ARRROW"));
}


any ASTBuilder::visitOC_RightArrowHead(CypherParser::OC_RightArrowHeadContext *ctx)  {
  return static_cast<ASTNode*>(new ASTLeafNoValue("RIGHT_ARROW"));;
}
