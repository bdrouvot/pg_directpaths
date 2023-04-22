#ifndef DP_H
#define DP_H

#include "postgres.h"
#include "nodes/execnodes.h"
#include "commands/explain.h"

#define PG_VERSION_10 100000
#define PG_VERSION_11 110000
#define PG_VERSION_12 120000
#define PG_VERSION_13 130000
#define PG_VERSION_14 140000
#define PG_VERSION_15 150000
#define PG_VERSION_16 160000

#if PG_VERSION_NUM < PG_VERSION_10
#error pg_directpaths does not support PostgreSQL 9 or earlier versions.
#endif

extern void IAExplainNode(PlanState *planstate, List *ancestors,
                    const char *relationship, const char *plan_name,
                    ExplainState *es);

#endif   /* DP_H */
