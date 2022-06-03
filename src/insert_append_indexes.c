/*
 *  insert_append_indexes.c
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

#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "include/insert_append_indexes.h"

#if PG_VERSION_NUM < PG_VERSION_12
#include "utils/rel.h"
#endif

void
IARebuildIndexes(EState *estate, ResultRelInfo *resultRelInfo)
{
	int				i;
	int				numIndices;
	RelationPtr		indices;
	char			persistence;
	Oid		indexOid;

#if PG_VERSION_NUM >= PG_VERSION_14
	ExecOpenIndices(resultRelInfo, false);
#endif

	numIndices = resultRelInfo->ri_NumIndices;
	indices = resultRelInfo->ri_IndexRelationDescs;

	for (i = 0; i < numIndices; i++)
	{
#if PG_VERSION_NUM >= PG_VERSION_14
		ReindexParams params = {0};
#endif
		indexOid = RelationGetRelid(indices[i]);
		relation_close(indices[i], NoLock);
		persistence = indices[i]->rd_rel->relpersistence;
		indices[i] = NULL;

#if PG_VERSION_NUM >= PG_VERSION_14
		reindex_index(indexOid, false, persistence, &params);
#else
		reindex_index(indexOid, false, persistence, 0);
#endif
		CommandCounterIncrement();
	}
}