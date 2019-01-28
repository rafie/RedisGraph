/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Apache License, Version 2.0,
 * modified with the Commons Clause restriction.
 */

#ifndef __RESULTSET_RECORDS_H__
#define __RESULTSET_RECORDS_H__

#include "../redismodule.h"
#include "../execution_plan/record.h"

void ResultSet_EmitRecord(RedisModuleCtx *ctx, const Record r, unsigned int numcols);

#endif