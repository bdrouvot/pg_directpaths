/*
 *  pg_directpaths.c
 *
 *      This file is part of the pg_directpaths module.
 *
 * This program is open source, licensed under the PostgreSQL license.
 * For license terms, see the LICENSE file.
 *
 * Copyright (C) 2022: Bertrand Drouvot
 *
 */

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "optimizer/paths.h"
#include "include/hooks.h"
#include "include/pg_directpaths.h"
#include "include/cscan.h"


#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

void _PG_init(void);
void _PG_fini(void);

void _PG_init(void)
{
	RegisterCustomScanMethods(&insert_append_plan_methods);
    prev_planner_hook = planner_hook;
	planner_hook = InsertAppend_planner;
    prev_post_parse_analyze_hook = post_parse_analyze_hook;
    post_parse_analyze_hook = InsertAppend_post_parse_analyze;
}

void _PG_fini(void)
{
    planner_hook = prev_planner_hook;
    post_parse_analyze_hook = prev_post_parse_analyze_hook;
}