/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Apache License, Version 2.0,
* modified with the Commons Clause restriction.
*/

#include <assert.h>
#include "query_executor.h"
#include "graph/graph.h"
#include "graph/entities/node.h"
#include "stores/store.h"
#include "util/arr.h"
#include "util/vector.h"
#include "parser/grammar.h"
#include "arithmetic/agg_ctx.h"
#include "arithmetic/repository.h"
#include "parser/parser_common.h"

static void _inlineProperties(AST *ast) {
    /* Migrate inline filters to WHERE clause. */
    if(!ast->matchNode) return;
    // Vector *entities = ast->matchNode->graphEntities;
    Vector *entities = ast->matchNode->_mergedPatterns;

    char *alias;
    char *property;
    AST_ArithmeticExpressionNode *lhs;
    AST_ArithmeticExpressionNode *rhs;

    /* Foreach entity. */
    for(int i = 0; i < Vector_Size(entities); i++) {
        AST_GraphEntity *entity;
        Vector_Get(entities, i, &entity);

        alias = entity->alias;

        Vector *properties = entity->properties;
        if(properties == NULL) {
            continue;
        }

        /* Foreach property. */
        for(int j = 0; j < Vector_Size(properties); j+=2) {
            SIValue *key;
            SIValue *val;

            // Build the left-hand filter value from the node alias and property
            Vector_Get(properties, j, &key);
            property = key->stringval;
            lhs = New_AST_AR_EXP_VariableOperandNode(alias, property);

            // Build the right-hand filter value from the specified constant
            // TODO can update grammar so that this constant is already an ExpressionNode
            // instead of an SIValue
            Vector_Get(properties, j+1, &val);
            rhs = New_AST_AR_EXP_ConstOperandNode(*val);

            AST_FilterNode *filterNode = New_AST_PredicateNode(lhs, EQ, rhs);
            
            /* Create WHERE clause if missing. */
            if(ast->whereNode == NULL) {
                ast->whereNode = New_AST_WhereNode(filterNode);
            } else {
                /* Introduce filter with AND operation. */
                AST_FilterNode *left = ast->whereNode->filters;
                AST_FilterNode *right = filterNode;
                ast->whereNode->filters = New_AST_ConditionNode(left, AND, right);
            }
        }
    }
}

/* Shares merge pattern with match clause. */
static void _replicateMergeClauseToMatchClause(AST *ast) {    
    assert(ast->mergeNode && !ast->matchNode);

    /* Match node is expecting a vector of vectors,
     * and so we have to wrap merge graph entities vector
     * within another vector
     * wrappedEntities will be freed by match clause. */
    Vector *wrappedEntities = NewVector(Vector*, 1);
    Vector_Push(wrappedEntities, ast->mergeNode->graphEntities);
    ast->matchNode = New_AST_MatchNode(wrappedEntities);
}

/* If we have a "RETURN *" clause, populate it with all aliased entities. */
static void _populateReturnAll(AST *ast) {
    // Do nothing if there is no RETURN or an array of return elements already exists.
    if (ast->returnNode == NULL) return;
    if (ast->returnNode->returnElements != NULL) return;

    // Collect all entities from MATCH and CREATE clauses
    // TODO Add entities from UNWIND
    TrieMap *identifiers = NewTrieMap();
    MatchClause_DefinedEntities(ast->matchNode, identifiers);
    CreateClause_DefinedEntities(ast->createNode, identifiers);

    // Allocate a new return element array to contain all user-provided aliases
    AST_ReturnElementNode **entities = array_new(AST_ReturnElementNode*, identifiers->cardinality);
    char *ptr;
    tm_len_t len;
    void *value;
    TrieMapIterator *it = TrieMap_Iterate(identifiers, "", 0);
    while(TrieMapIterator_Next(it, &ptr, &len, &value)) {
        AST_GraphEntity *entity = value;
        if(entity->anonymous) continue;

        // Copy each alias string to the stack
        char buffer[len + 1];
        memcpy(buffer, ptr, len);
        buffer[len] = '\0';

        // Create and add a return entity for the alias
        AST_ArithmeticExpressionNode *arNode = New_AST_AR_EXP_VariableOperandNode(buffer, NULL);
        AST_ReturnElementNode *returnEntity = New_AST_ReturnElementNode(arNode, NULL);
        entities = array_append(entities, returnEntity);
    }

    TrieMapIterator_Free(it);

    ast->returnNode->returnElements = entities;
}

AST* ParseQuery(const char *query, size_t qLen, char **errMsg) {
    return Query_Parse(query, qLen, errMsg);
}

AST_Validation AST_PerformValidations(RedisModuleCtx *ctx, AST *ast) {
    char *reason;
    if (AST_Validate(ast, &reason) != AST_VALID) {
        RedisModule_ReplyWithError(ctx, reason);
        free(reason);
        return AST_INVALID;
    }
    return AST_VALID;
}

void ModifyAST(GraphContext *gc, AST *ast) {
    if(ast->mergeNode) {
        /* Create match clause which will try to match 
         * against pattern specified within merge clause. */
        _replicateMergeClauseToMatchClause(ast);
    }

    _populateReturnAll(ast);
    AST_NameAnonymousNodes(ast);
    _inlineProperties(ast);
}
