/*
 *  direct_paths_explain.c
 *
 *      This file is part of the pg_directpaths module.
 *
 * This program is open source, licensed under the PostgreSQL license.
 * For license terms, see the LICENSE file.
 *
 * Copyright (C) 2022: Bertrand Drouvot
 *
 */

#include "include/pg_directpaths.h"

#if PG_VERSION_NUM >= PG_VERSION_16
#error unsupported PostgreSQL version
#elif PG_VERSION_NUM >= PG_VERSION_15
#include "corepg/explain_15.c"
#elif PG_VERSION_NUM >= PG_VERSION_14
#include "corepg/explain_14.c"
#elif PG_VERSION_NUM >= PG_VERSION_13
#include "corepg/explain_13.c"
#elif PG_VERSION_NUM >= PG_VERSION_12
#include "corepg/explain_12.c"
#elif PG_VERSION_NUM >= PG_VERSION_11
#include "corepg/explain_11.c"
#elif PG_VERSION_NUM >= PG_VERSION_10
#include "corepg/explain_10.c"
#else
#error unsupported PostgreSQL version
#endif

void
IAExplainNode(PlanState *planstate, List *ancestors,
                        const char *relationship, const char *plan_name,
                        ExplainState *es)
{
	ExplainNode(planstate, ancestors, relationship, plan_name, es);
}
