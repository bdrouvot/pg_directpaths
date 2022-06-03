/*
 *  hooks.c
 *
 *      This file is part of the pg_directpaths module.
 *
 * This program is open source, licensed under the PostgreSQL license.
 * For license terms, see the LICENSE file.
 *
 * Copyright (C) 2022: Bertrand Drouvot
 *
 */

#include "include/hooks.h"
#include "include/cscan.h"

#if PG_VERSION_NUM >= PG_VERSION_13
#define standard_planner_compat(a, c, d) standard_planner(a, NULL, c, d)
#else
#define standard_planner_compat(a, c, d) standard_planner(a, c, d)
#endif

bool insert_append_candidate = false;

typedef struct InsertAppendPlanningContext
{
    /* the parsed query */
    Query *query;

    /* the cursor options */
    int cursorOptions;

    /* the ParamListInfo */
    ParamListInfo boundParams;

    /* plan created either by standard_planner or by our planner */
    PlannedStmt *plan;

} InsertAppendPlanningContext;

/* our planner */
static PlannedStmt * PlanInsertAppendStmt(InsertAppendPlanningContext *planContext);

/*
 * Planner hook.
 */
PlannedStmt *
InsertAppend_planner(Query *parse,
	#if PG_VERSION_NUM >= PG_VERSION_13
					const char *query_string,
	#endif
					int cursorOptions,
					ParamListInfo boundParams)
{
    InsertAppendPlanningContext planContext = {
		.query = parse,
		.cursorOptions = cursorOptions,
		.boundParams = boundParams,
	};

    PlannedStmt *result = NULL;

    if (prev_planner_hook)
		planContext.plan = (*prev_planner_hook) (planContext.query,
#if PG_VERSION_NUM >= PG_VERSION_13
                                                query_string,
#endif
                                                planContext.cursorOptions,
                                                planContext.boundParams);
	else
        planContext.plan = standard_planner_compat(planContext.query,
		        								   planContext.cursorOptions,
				        						   planContext.boundParams);

    /*
     * Switch to direct path insert
     * If the insert is done on a non partitioned relation
     * and the APPEND hint is used.
     */

    if (parse->commandType == CMD_INSERT && insert_append_candidate)
    {
        result = PlanInsertAppendStmt(&planContext);
        insert_append_candidate = false;
    }
    else
    {
        result = planContext.plan;
    }

	return result;
}

static PlannedStmt *
PlanInsertAppendStmt(InsertAppendPlanningContext *planContext)
{

	PlannedStmt *resultPlan = NULL;
	CustomScan *customScan = makeNode(CustomScan);

	customScan->methods = &insert_append_plan_methods;
	customScan->flags = CUSTOMPATH_SUPPORT_BACKWARD_SCAN;

    customScan->custom_scan_tlist = planContext->plan->planTree->targetlist;
    customScan->scan.plan.targetlist = customScan->custom_scan_tlist;

    /* save the original plan as we want to use it later on */
    customScan->custom_private = list_make1(planContext->plan);
    
    /* create our new plan */
    resultPlan = makeNode(PlannedStmt);
    
    resultPlan->commandType = planContext->plan->commandType;
    resultPlan->queryId = planContext->plan->queryId;
    resultPlan->hasReturning = planContext->plan->hasReturning;
    resultPlan->hasModifyingCTE = planContext->plan->hasModifyingCTE;
    resultPlan->canSetTag = true;
    resultPlan->transientPlan = planContext->plan->transientPlan;
    resultPlan->dependsOnRole = planContext->plan->dependsOnRole;
    resultPlan->parallelModeNeeded = false;
#if PG_VERSION_NUM >= PG_VERSION_11
    resultPlan->jitFlags = planContext->plan->jitFlags;
    resultPlan->paramExecTypes = planContext->plan->paramExecTypes;
#endif
    resultPlan->planTree = (Plan *) customScan; // Using a custom scan
    resultPlan->rtable = planContext->plan->rtable;
    resultPlan->resultRelations = planContext->plan->resultRelations;
    resultPlan->subplans = planContext->plan->subplans;
    resultPlan->rewindPlanIDs = planContext->plan->rewindPlanIDs;
    resultPlan->rowMarks = planContext->plan->rowMarks;
    resultPlan->relationOids = planContext->plan->relationOids;
    resultPlan->invalItems = planContext->plan->invalItems; 
    resultPlan->utilityStmt = planContext->plan->utilityStmt;
    resultPlan->stmt_location = planContext->plan->stmt_location;
    resultPlan->stmt_len = planContext->plan->stmt_len;

    return resultPlan;
}

void
InsertAppend_post_parse_analyze(ParseState *pstate, Query *query
#if PG_VERSION_NUM >= PG_VERSION_14
                                , JumbleState *jstate
#endif
)
{
    if (prev_post_parse_analyze_hook)
        prev_post_parse_analyze_hook(pstate, query
#if PG_VERSION_NUM >= PG_VERSION_14
,                                   jstate
#endif
        );
    /* check if the target relation is candidate for insert append */
    if (strstr(pstate->p_sourcetext, "/*+ APPEND */")
        && (pstate->p_target_relation)
        && (pstate->p_target_relation->rd_rel->relkind == RELKIND_RELATION))
            insert_append_candidate = true;
}