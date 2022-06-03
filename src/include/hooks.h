#ifndef AI_HOOKS_H
#define AI_HOOKS_H

#include "pg_directpaths.h"
#include "optimizer/planner.h"
#include "parser/analyze.h"
#include "funcapi.h"

static planner_hook_type prev_planner_hook = NULL;
static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;

extern void InsertAppendExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count,
                             bool execute_once);

extern void InsertAppend_post_parse_analyze(ParseState *pstate, Query *query
#if PG_VERSION_NUM >= PG_VERSION_14
                                            , JumbleState *jstate
#endif
);

#if PG_VERSION_NUM >= PG_VERSION_13
extern PlannedStmt * InsertAppend_planner(Query *parse,
                                 const char *query_string,
                                 int cursorOptions,
                                 ParamListInfo boundParams);
#else
extern PlannedStmt * InsertAppend_planner(Query *parse,
                                 int cursorOptions,
                                 ParamListInfo boundParams);
#endif
#endif  /* AI_HOOKS_H */