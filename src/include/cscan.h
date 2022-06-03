#ifndef CSCAN_H
#define CSCAN_H

#include "pg_directpaths.h"
#include "nodes/extensible.h"
#include "miscadmin.h"
#if PG_VERSION_NUM >= PG_VERSION_11
#include "executor/execPartition.h"
#endif
#include "utils/rel.h"

extern CustomScanMethods insert_append_plan_methods;
extern void  InsertAppendExecInitCustomScan(CustomScanState *node, EState *estate, int eflags);
extern TupleTableSlot *InsertAppendExecCustomScan(CustomScanState *node);
extern void InsertAppendExecEndCustomScan(CustomScanState *node);
extern void InsertAppendExecReScanCustomScan(CustomScanState *node);
extern void InsertAppendExplainCustomScan(CustomScanState *node, List *ancestors, ExplainState *es);
extern TupleTableSlot *ExecInsertAppendTable(PlanState *pstate);
extern void ExecEndInsertAppendTable(PlanState *pstate);

typedef struct AppendScanState
{
    CustomScanState customScanState;
    PlannedStmt  *OriginalPlan;
    PlanState *OriginalPlanState;
} AppendScanState;

#endif  /* CSCAN_H */
