/*
 *  cscan.c
 *
 *      This file is part of the pg_directpaths module.
 *
 * This program is open source, licensed under the PostgreSQL license.
 * For license terms, see the LICENSE file.
 *
 * Copyright (C) 2022: Bertrand Drouvot
 *
 */

#include "include/cscan.h"
#include "include/direct_paths_explain.h"

/*
 * Define the CustomExecMethods for Insert Append.
 */

static CustomExecMethods DirectAppendExecutorCustomExecMethods = {
    .CustomName = "DirectAppendCustomScan",
    .BeginCustomScan = InsertAppendExecInitCustomScan,
    .ExecCustomScan = InsertAppendExecCustomScan,
    .EndCustomScan = InsertAppendExecEndCustomScan,
    .ReScanCustomScan = InsertAppendExecReScanCustomScan,
    .ExplainCustomScan = InsertAppendExplainCustomScan
};

/* function for creating Insert Append custom scan node */
static Node * InsertAppendExecutorCreateScan(CustomScan *scan);

CustomScanMethods insert_append_plan_methods = {
    "Insert Append",
    InsertAppendExecutorCreateScan
};

void
InsertAppendExecInitCustomScan(CustomScanState *node, EState *estate, int eflags)
{
	AppendScanState	*state = (AppendScanState *) node;

    /* store the Original PlanState as this is the one we want */
    state->OriginalPlanState = ExecInitNode(state->OriginalPlan->planTree, estate, eflags);
}

TupleTableSlot *
InsertAppendExecCustomScan(CustomScanState *node)
{
    AppendScanState  *state = (AppendScanState *) node;

    ExecInsertAppendTable(state->OriginalPlanState);
    
	return NULL;
}		

void
InsertAppendExecEndCustomScan(CustomScanState *node)
{
	AppendScanState  *state = (AppendScanState *) node;

    ExecEndInsertAppendTable(state->OriginalPlanState);
}

void 
InsertAppendExecReScanCustomScan(CustomScanState *node)
{
	elog(ERROR, "InsertAppendExecReScanCustomScan is not implemented");
}

void
InsertAppendExplainCustomScan(CustomScanState *node, List *ancestors, ExplainState *es)
{
    AppendScanState  *state = (AppendScanState *) node;

    resetStringInfo(es->str);
    appendStringInfo(es->str, "INSERT APPEND\n");

    /* now we can simply explain the original plan state */
    IAExplainNode(state->OriginalPlanState, ancestors, "InitPlan", NULL, es);
}

/*
 * InsertAppendExecutorCreateScan creates the scan state for the InsertAppend executor.
 */
static Node *
InsertAppendExecutorCreateScan(CustomScan *scan)
{
    AppendScanState *scanState = palloc0(sizeof(AppendScanState));

    scanState->customScanState.ss.ps.type = T_CustomScanState;

    /* store the Original Plan as this is the one we want */
    scanState->OriginalPlan = (PlannedStmt *) linitial(scan->custom_private);

    scanState->customScanState.methods = &DirectAppendExecutorCustomExecMethods;

    return (Node *) scanState;
}
