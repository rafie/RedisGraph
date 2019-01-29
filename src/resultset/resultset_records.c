/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Apache License, Version 2.0,
 * modified with the Commons Clause restriction.
 */

#include "resultset_records.h"
#include "../value.h"
#include "../graph/graphcontext.h"
#include "../graph/entities/node.h"
#include "../graph/entities/edge.h"
#include <assert.h>

/* This function has handling for all SIValue types, though not all are used in
 * RedisGraph currently.
 * The current RESP protocol only has unique support for strings, 8-byte integers,
 * and NULL values (doubles are converted to strings in the Redis layer), 
 * but we may as well be forward-thinking. */
static void _emitSIValue(RedisModuleCtx *ctx, const SIValue v) {
    // Emit the actual value, then the value type (to facilitate client-side parsing)
    switch (SI_TYPE(v)) {
        case T_STRING:
        case T_CONSTSTRING:
            RedisModule_ReplyWithStringBuffer(ctx, v.stringval, strlen(v.stringval));
            RedisModule_ReplyWithStringBuffer(ctx, "string", 6);
            return;
        case T_INT32:
            RedisModule_ReplyWithLongLong(ctx, v.intval);
            RedisModule_ReplyWithStringBuffer(ctx, "integer", 7);
            return;
        case T_INT64:
            RedisModule_ReplyWithLongLong(ctx, v.longval);
            RedisModule_ReplyWithStringBuffer(ctx, "integer", 7);
            return;
        case T_UINT:
            RedisModule_ReplyWithLongLong(ctx, v.uintval);
            RedisModule_ReplyWithStringBuffer(ctx, "integer", 7);
            return;
        case T_FLOAT:
            RedisModule_ReplyWithDouble(ctx, (double)v.floatval);
            RedisModule_ReplyWithStringBuffer(ctx, "double", 6);
            return;
        case T_DOUBLE:
            RedisModule_ReplyWithDouble(ctx, v.doubleval);
            RedisModule_ReplyWithStringBuffer(ctx, "double", 6);
            return;
        case T_BOOL:
            if (v.boolval == true) RedisModule_ReplyWithStringBuffer(ctx, "true", 4);
            else RedisModule_ReplyWithStringBuffer(ctx, "false", 5);
            RedisModule_ReplyWithStringBuffer(ctx, "boolean", 7);
            return;
        case T_NULL:
            RedisModule_ReplyWithNull(ctx);
            RedisModule_ReplyWithStringBuffer(ctx, "NULL", 4);
            return;
        default:
            assert("Unhandled value type" && false);
      }
}

static void _enumerateProperties(RedisModuleCtx *ctx, const GraphEntity *e) {
    int prop_count = ENTITY_PROP_COUNT(e);
    RedisModule_ReplyWithArray(ctx, prop_count);
    // Iterate over all properties stored on entity
    for (int i = 0; i < prop_count; i ++) {
        RedisModule_ReplyWithArray(ctx, 3);
        EntityProperty prop = ENTITY_PROPS(e)[i];
        // Emit the string key
        RedisModule_ReplyWithStringBuffer(ctx, prop.name, strlen(prop.name));
        // Emit the value
        _emitSIValue(ctx, prop.value);
    }
}

static void _ResultSet_ReplyWithNode(RedisModuleCtx *ctx, Node *n) {
    // 4 top-level entities in node reply
    RedisModule_ReplyWithArray(ctx, 4);

    // ["type", "node"]
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithStringBuffer(ctx, "type", 4);
    RedisModule_ReplyWithStringBuffer(ctx, "node", 4);

    // ["id", id(int)]
    int id = ENTITY_GET_ID(n);
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithStringBuffer(ctx, "id", 2);
    RedisModule_ReplyWithLongLong(ctx, id);

    // ["labels", [label string]]
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithStringBuffer(ctx, "labels", 6);
    // Print label in nested array for multi-label support
    RedisModule_ReplyWithArray(ctx, 1);
    // Retrieve label
    // TODO Make a more efficient lookup for this string
    GraphContext *gc = GraphContext_GetFromLTS();
    const char *label = GraphContext_GetNodeLabel(gc, id);
    if (label == NULL) {
        RedisModule_ReplyWithNull(ctx);
    } else {
        RedisModule_ReplyWithStringBuffer(ctx, label, strlen(label));
    }

    // [properties, [properties]]
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithStringBuffer(ctx, "properties", 10);
    _enumerateProperties(ctx, (GraphEntity*)n);
}

static void _ResultSet_ReplyWithEdge(RedisModuleCtx *ctx, Edge *e) {
    // 6 top-level entities in node reply
    RedisModule_ReplyWithArray(ctx, 6);

    // ["type", "relation"]
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithStringBuffer(ctx, "type", 4);
    RedisModule_ReplyWithStringBuffer(ctx, "relation", 8);

    // ["id", id(int)]
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithStringBuffer(ctx, "id", 2);
    RedisModule_ReplyWithLongLong(ctx, ENTITY_GET_ID(e));

    // ["relation_type", type string]
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithStringBuffer(ctx, "relation_type", 13);
    // Retrieve relation type
    // TODO Make a more efficient lookup for this string
    GraphContext *gc = GraphContext_GetFromLTS();
    const char *label = GraphContext_GetEdgeRelationType(gc, e);
    RedisModule_ReplyWithStringBuffer(ctx, label, strlen(label));

    // ["src_node", srcNodeID(int)]
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithStringBuffer(ctx, "src_node", 8);
    RedisModule_ReplyWithLongLong(ctx, Edge_GetSrcNodeID(e));

    // ["dest_node", destNodeID(int)]
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithStringBuffer(ctx, "dest_node", 9);
    RedisModule_ReplyWithLongLong(ctx, Edge_GetDestNodeID(e));

    // [properties, [properties]]
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithStringBuffer(ctx, "properties", 10);
    _enumerateProperties(ctx, (GraphEntity*)e);
}

void ResultSet_EmitRecord(RedisModuleCtx *ctx, const Record r, unsigned int numcols) {
    // Prepare return array sized to the number of RETURN entities
    RedisModule_ReplyWithArray(ctx, numcols);

    int written;
    char value[2048];
    for(int i = 0; i < numcols; i++) {
        RecordEntryType t = Record_GetType(r, i);
        switch (t) {
            case REC_TYPE_SCALAR:
                /* TODO This logic can be replaced with an _emitSIValue call,
                 * which would remove string truncation and the snprintf cost. */
                written = SIValue_ToString(Record_GetScalar(r, i), value, 2048);
                RedisModule_ReplyWithStringBuffer(ctx, value, written);
                break;
            case REC_TYPE_NODE:
                _ResultSet_ReplyWithNode(ctx, Record_GetNode(r, i));
                break;
            case REC_TYPE_EDGE:
                _ResultSet_ReplyWithEdge(ctx, Record_GetEdge(r, i));
                break;
            default:
                assert("tried to write unhandled type to result set" && false);
        }
    }
}

