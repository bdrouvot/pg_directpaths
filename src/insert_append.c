/*
 *  insert_append.c
 *
 *      This file is part of the pg_directpaths module.
 *
 * This program is open source, licensed under the PostgreSQL license.
 * For license terms, see the LICENSE file.
 *
 * Copyright (C) 2022: Bertrand Drouvot
 *
 */

#include <unistd.h>
#include "include/pg_directpaths.h"
#include "access/xact.h"
#include "commands/trigger.h"
#include "foreign/fdwapi.h"
#include "storage/bufmgr.h"
#include "include/cscan.h"
#include "include/insert_append_indexes.h"

#if PG_VERSION_NUM >= PG_VERSION_15
#error unsupported PostgreSQL version
#elif PG_VERSION_NUM >= PG_VERSION_14
#include "corepg/nodeModifyTable_14.c"
#include "access/heaptoast.h"
#elif PG_VERSION_NUM >= PG_VERSION_13
#include "corepg/nodeModifyTable_13.c"
#include "access/heaptoast.h"
#include "catalog/pg_control.h"
#elif PG_VERSION_NUM >= PG_VERSION_12
#include "corepg/nodeModifyTable_12.c"
#include "access/tuptoaster.h"
#include "catalog/pg_control.h"
#elif PG_VERSION_NUM >= PG_VERSION_11
#include "corepg/nodeModifyTable_11.c"
#include "access/tuptoaster.h"
#include "catalog/catalog.h"
#include "catalog/pg_control.h"
#elif PG_VERSION_NUM >= PG_VERSION_10
#include "corepg/nodeModifyTable_10.c"
#include "access/tuptoaster.h"
#include "catalog/catalog.h"
#include "catalog/pg_control.h"
#else
#error unsupported PostgreSQL version
#endif

#define GetCurrentPage(writer) \
            ((Page) ((writer)->blocks + BLCKSZ * (writer)->curblk))

#define GetTargetPage(writer, blk_offset) \
		((Page) ((writer)->blocks + BLCKSZ * (blk_offset)))

#define PAGES_COUNT		1024

#define BLKS_TOTAL_CNT(writer)	((writer)->blks_initial_cnt + (writer)->blks_append_cnt)

typedef struct InsertAppendWriter
{
	Relation		rel;	/* target relation */
	char           *blocks; /* heap blocks buffer */
	int             curblk; /* current block buffer */
	BlockNumber blks_initial_cnt; /* initial number of blocks part of the relation */
	BlockNumber blks_append_cnt;	/* number of blocks created by Insert Append */
	int				datafd;		/* fd of relation file */
	TransactionId	xid;
	CommandId		cid;
	BlockNumber ready_blknos[PAGES_COUNT]; /* to be used as parameter of log_newpages */
	Page        ready_pages[PAGES_COUNT]; /* to be written in the WAL files */
} InsertAppendWriter;

static void close_relation_file(InsertAppendWriter *writer);
static void flush_pages(InsertAppendWriter *writer);
#if PG_VERSION_NUM < PG_VERSION_14
static void log_newpages(RelFileNode *rnode, ForkNumber forkNum, int num_pages,
             BlockNumber *blknos, Page *pages, bool page_std);
#endif

static void
DirectWriterClose(InsertAppendWriter *writer, EState *estate, ResultRelInfo *resultRelInfo)
{
	Assert(writer != NULL);

	flush_pages(writer);
	close_relation_file(writer);
	IARebuildIndexes(estate, resultRelInfo);

	if (writer->rel)
#if PG_VERSION_NUM >= PG_VERSION_13
	table_close(writer->rel, AccessExclusiveLock);
#else
	heap_close(writer->rel, AccessExclusiveLock);
#endif

	if (writer->blocks)
		pfree(writer->blocks);
		
	pfree(writer);	
}

static InsertAppendWriter *
CreateDirectWriter(EState *estate, Relation rel)
{
    InsertAppendWriter       *writer;

	writer = palloc0(sizeof(InsertAppendWriter));

	/* an access exlusive lock is acquired on the relation */
#if PG_VERSION_NUM >= PG_VERSION_13
	table_open(RelationGetRelid(rel), AccessExclusiveLock);
#else
	heap_open(RelationGetRelid(rel), AccessExclusiveLock);
#endif

	writer->rel = rel;
	writer->blocks = palloc(BLCKSZ * PAGES_COUNT);
	writer->curblk = 0;
	writer->blks_initial_cnt = RelationGetNumberOfBlocks(rel);
	writer->blks_append_cnt = 0;
	writer->datafd = -1;
	writer->xid = GetCurrentTransactionId();
	writer->cid = GetCurrentCommandId(true);

    return writer;
}

static int
open_relation_file(RelFileNode rnode, bool istemp, BlockNumber blknum)
{
	int			ret;
	BlockNumber segno;
	char	   *filename = NULL;
	RelFileNodeBackend	bknode;
	int			fd = -1;

	bknode.node = rnode;
	bknode.backend = istemp ? MyBackendId : InvalidBackendId;
	filename = relpath(bknode, MAIN_FORKNUM);

	segno = blknum / RELSEG_SIZE;

	if (segno > 0)
	{
		char	   *tmpf = palloc(strlen(filename) + 12);

		sprintf(tmpf, "%s.%u", filename, segno);
		pfree(filename);
		filename = tmpf;
	}
#if PG_VERSION_NUM >= PG_VERSION_11
	fd = BasicOpenFilePerm(filename, O_CREAT | O_WRONLY | PG_BINARY, S_IRUSR | S_IWUSR);
#else
	fd = BasicOpenFile(filename, O_CREAT | O_WRONLY | PG_BINARY, S_IRUSR | S_IWUSR);
#endif
	if (fd == -1)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not open file: %m")));

	ret = lseek(fd, BLCKSZ * (blknum % RELSEG_SIZE), SEEK_SET);

	if (ret == -1)
	{
		close(fd);
		ereport(ERROR, (errcode_for_file_access(),
						errmsg
						("could not seek the end of file: %m")));
	}

	pfree(filename);

	return fd;
}

static void
close_relation_file(InsertAppendWriter *writer)
{
	if (writer->datafd != -1)
	{
		if (pg_fsync(writer->datafd) != 0)
			ereport(WARNING, (errcode_for_file_access(),
						errmsg("could not sync file: %m")));
		if (close(writer->datafd) < 0)
			ereport(WARNING, (errcode_for_file_access(),
						errmsg("could not close file: %m")));
		writer->datafd = -1;
	}
}

static void
flush_pages(InsertAppendWriter *writer)
{
	int			i;
	int			num;

	num = writer->curblk;
	if (!PageIsEmpty(GetCurrentPage(writer)))
		num += 1;

	if (num <= 0)
		return;

	/*
	 * Write pages.
	 */
	for (i = 0; i < num;)
	{
		char	   *buffer;
		int			total;
		int			written;
		int			flush_num;
		BlockNumber	relblks = BLKS_TOTAL_CNT(writer);

		/* switch to the next file if the current file has been filled up */
		if (relblks % RELSEG_SIZE == 0)
			close_relation_file(writer);

		if (writer->datafd == -1)
			writer->datafd = open_relation_file(writer->rel->rd_node,
											RELATION_IS_LOCAL(writer->rel),
											relblks);

		/* number of blocks to be added to the current file */
		flush_num = Min(num - i, RELSEG_SIZE - relblks % RELSEG_SIZE);

		Assert(flush_num > 0);

		/*
		 * If the relation is a logged one then write the new pages
		 * in the WAL files.
		 */
		if (!RELATION_IS_LOCAL(writer->rel)
			&& !(writer->rel->rd_rel->relpersistence == RELPERSISTENCE_UNLOGGED))
		{
			log_newpages(&writer->rel->rd_node, MAIN_FORKNUM, flush_num,
							writer->ready_blknos, writer->ready_pages, true);
		}

		if (DataChecksumsEnabled())
		{
			int		j;
			/*
			 * Write checksum for pages that are going to be written to the
			 * current file.
			 */
			for (j = 0; j < flush_num; j++)
				PageSetChecksumInplace(writer->ready_pages[j], writer->ready_blknos[j]);
		}	

		writer->blks_append_cnt += flush_num;

		/*
		 * Write flush_num blocks to the current file starting at block
		 * offset i.  The current file might get full, ie, RELSEG_SIZE blocks
		 * full, after writing that much (see how flush_num is calculated
		 * above to understand why). We write the remaining content of the
		 * block buffer in the new file during the next iteration.
		 */

		buffer = writer->blocks + BLCKSZ * i;
		total = BLCKSZ * flush_num;
		written = 0;

		while (total > 0)
		{
			int	len = write(writer->datafd, buffer + written, total);
			if (len == -1)
			{
				/* fatal error, do not want to write blocks anymore */
				ereport(ERROR, (errcode_for_file_access(),
								errmsg("could not write to file: %m")));
			}
			written += len;
			total -= len;
		}

		i += flush_num;
	}
}

/*
 * Modified version of PostgreSQL core ExecInsert.
 */
static TupleTableSlot *
IAExecInsert(ModifyTableState *mtstate,
		   TupleTableSlot *slot,
		   TupleTableSlot *planSlot,
		   TupleTableSlot *srcSlot,
		   ResultRelInfo *returningRelInfo,
		   EState *estate,
		   bool canSetTag,
		   InsertAppendWriter *writer)
{
	Page           page;
	OffsetNumber    offnum;
    ItemId          itemId;
    Item            item;
	HeapTuple  tuple;

	page = GetCurrentPage(writer);

#if PG_VERSION_NUM >= PG_VERSION_12
	tuple = ExecCopySlotHeapTuple(slot);
#else
	tuple = ExecMaterializeSlot(slot);
#endif

    /* take care of toasted data if needed */
	if (tuple->t_len > TOAST_TUPLE_THRESHOLD)
#if PG_VERSION_NUM >= PG_VERSION_13
        tuple = heap_toast_insert_or_update(writer->rel, tuple, NULL, 0);
#else
        tuple = toast_insert_or_update(writer->rel, tuple, NULL, 0);
#endif

#if PG_VERSION_NUM < PG_VERSION_12
	/* assign oids if needed */
	if (writer->rel->rd_rel->relhasoids)
	{
		Assert(!OidIsValid(HeapTupleGetOid(tuple)));
		HeapTupleSetOid(tuple, GetNewOid(writer->rel));
	}
#endif

	/* assume the tuple has been toasted */
	if (MAXALIGN(tuple->t_len) > MaxHeapTupleSize)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("row is too big: size %lu, maximum size %lu",
						(unsigned long) tuple->t_len,
						(unsigned long) MaxHeapTupleSize)));

	if (PageGetFreeSpace(page) < MAXALIGN(tuple->t_len) +
		RelationGetTargetPageFreeSpace(writer->rel, HEAP_DEFAULT_FILLFACTOR))
	{
		if (writer->curblk < PAGES_COUNT - 1)
			writer->curblk++;
		else
		{
			flush_pages(writer);
			writer->curblk = 0;	/* recycle from first block */
		}

		page = GetCurrentPage(writer);

		/* initialize current block */
		PageInit(page, BLCKSZ, 0);
		writer->ready_blknos[writer->curblk] = writer->blks_initial_cnt + writer->blks_append_cnt + writer->curblk;
		writer->ready_pages[writer->curblk] = page;
	}

	tuple->t_data->t_infomask &= ~(HEAP_XACT_MASK);
	tuple->t_data->t_infomask2 &= ~(HEAP2_XACT_MASK);
	tuple->t_data->t_infomask |= HEAP_XMAX_INVALID;
	HeapTupleHeaderSetXmin(tuple->t_data, writer->xid);
	HeapTupleHeaderSetCmin(tuple->t_data, writer->cid);
	HeapTupleHeaderSetXmax(tuple->t_data, 0);
	
	/* put the tuple on local page */
	offnum = PageAddItem(page, (Item) tuple->t_data,
		tuple->t_len, InvalidOffsetNumber, false, true);

	ItemPointerSet(&(tuple->t_self), BLKS_TOTAL_CNT(writer) + writer->curblk, offnum);
	itemId = PageGetItemId(page, offnum);
	item = PageGetItem(page, itemId);
	((HeapTupleHeader) item)->t_ctid = tuple->t_self;

	if (canSetTag)
		(estate->es_processed)++;
		
	return NULL;
}

/*
 * Modified version of PostgreSQL core ExecModifyTable.
 */
TupleTableSlot *
ExecInsertAppendTable(PlanState *pstate)
{
	ModifyTableState *node = castNode(ModifyTableState, pstate);
	EState	   *estate = node->ps.state;
	CmdType		operation = node->operation;
	ResultRelInfo *resultRelInfo;
	PlanState  *subplanstate;
#if PG_VERSION_NUM < PG_VERSION_14
	JunkFilter *junkfilter;
	ResultRelInfo *saved_resultRelInfo;
#endif
	TupleTableSlot *slot;
	TupleTableSlot *planSlot;
	InsertAppendWriter *writer;
	Page page;

	CHECK_FOR_INTERRUPTS();

	/*
	 * This should NOT get called during EvalPlanQual; we should have passed a
	 * subplan tree to EvalPlanQual, instead.  Use a runtime test not just
	 * Assert because this condition is easy to miss in testing.  (Note:
	 * although ModifyTable should not get executed within an EvalPlanQual
	 * operation, we do have to allow it to be initialized and shut down in
	 * case it is within a CTE subplan.  Hence this test must be here, not in
	 * ExecInitModifyTable.)
	 */
#if PG_VERSION_NUM >= PG_VERSION_12
	if (estate->es_epq_active != NULL)
#else
	if (estate->es_epqTuple != NULL)
#endif
		elog(ERROR, "ExecInsertAppendTable should not be called during EvalPlanQual");

	/*
	 * If we've already completed processing, don't try to do more.  We need
	 * this test because ExecPostprocessPlan might call us an extra time, and
	 * our subplan's nodes aren't necessarily robust against being called
	 * extra times.
	 */
	if (node->mt_done)
		return NULL;

	/* Preload local variables */
#if PG_VERSION_NUM >= PG_VERSION_14
	resultRelInfo = node->resultRelInfo + node->mt_lastResultIndex;
	subplanstate = outerPlanState(node);
#else
	resultRelInfo =  node->resultRelInfo + node->mt_whichplan;
	subplanstate = node->mt_plans[node->mt_whichplan];
	junkfilter = resultRelInfo->ri_junkFilter;
	
	/*
	 * es_result_relation_info must point to the currently active result
	 * relation while we are within this ModifyTable node.  Even though
	 * ModifyTable nodes can't be nested statically, they can be nested
	 * dynamically (since our subplan could include a reference to a modifying
	 * CTE).  So we have to save and restore the caller's value.
	 */
	saved_resultRelInfo = estate->es_result_relation_info;

	estate->es_result_relation_info = resultRelInfo;
#endif

	/*
	 * Fetch rows from subplan(s), and execute the required table modification
	 * for each row.
	 */

	writer = CreateDirectWriter(estate, resultRelInfo->ri_RelationDesc);
	page = GetCurrentPage(writer);
	PageInit(page, BLCKSZ, 0);
	writer->ready_blknos[0] = writer->blks_initial_cnt;
	writer->ready_pages[0] = page;

	for (;;)
	{
		/*
		 * Reset the per-output-tuple exprcontext.  This is needed because
		 * triggers expect to use that context as workspace.  It's a bit ugly
		 * to do this below the top level of the plan, however.  We might need
		 * to rethink this later.
		 */
		ResetPerTupleExprContext(estate);
#if PG_VERSION_NUM >= PG_VERSION_12
		/*
		 * Reset per-tuple memory context used for processing on conflict and
		 * returning clauses, to free any expression evaluation storage
		 * allocated in the previous cycle.
		 */
		if (pstate->ps_ExprContext)
			ResetExprContext(pstate->ps_ExprContext);
#endif

		planSlot = ExecProcNode(subplanstate);

#if PG_VERSION_NUM < PG_VERSION_14
		if (TupIsNull(planSlot))
		{
			/* advance to next subplan if any */
			node->mt_whichplan++;
			if (node->mt_whichplan < node->mt_nplans)
			{
				resultRelInfo++;
				subplanstate = node->mt_plans[node->mt_whichplan];
				junkfilter = resultRelInfo->ri_junkFilter;
				estate->es_result_relation_info = resultRelInfo;
				EvalPlanQualSetPlan(&node->mt_epqstate, subplanstate->plan,
									node->mt_arowmarks[node->mt_whichplan]);
				/* Prepare to convert transition tuples from this child. */
#if PG_VERSION_NUM >= PG_VERSION_11
				if (node->mt_transition_capture != NULL)
				{
					node->mt_transition_capture->tcs_map =
						tupconv_map_for_subplan(node, node->mt_whichplan);
				}
				if (node->mt_oc_transition_capture != NULL)
				{
					node->mt_oc_transition_capture->tcs_map =
						tupconv_map_for_subplan(node, node->mt_whichplan);
				}
#else
                if (node->mt_transition_capture != NULL)
                {
                    Assert(node->mt_transition_tupconv_maps != NULL);
                    node->mt_transition_capture->tcs_map =
                        node->mt_transition_tupconv_maps[node->mt_whichplan];
                }
                if (node->mt_oc_transition_capture != NULL)
                {
                    Assert(node->mt_transition_tupconv_maps != NULL);
                    node->mt_oc_transition_capture->tcs_map =
                        node->mt_transition_tupconv_maps[node->mt_whichplan];
                }
#endif
				continue;
			}
			else
				break;
		}
#if PG_VERSION_NUM >= PG_VERSION_12
		/*
		 * Ensure input tuple is the right format for the target relation.
		 */
		if (node->mt_scans[node->mt_whichplan]->tts_ops != planSlot->tts_ops)
		{
			ExecCopySlot(node->mt_scans[node->mt_whichplan], planSlot);
			planSlot = node->mt_scans[node->mt_whichplan];
		}
#endif
#else
        /* No more tuples to process? */
        if (TupIsNull(planSlot))
            break;

        /*
         * When there are multiple result relations, each tuple contains a
         * junk column that gives the OID of the rel from which it came.
         * Extract it and select the correct result relation.
         */
        if (AttributeNumberIsValid(node->mt_resultOidAttno))
        {
            Datum       datum;
            bool        isNull;
            Oid         resultoid;

            datum = ExecGetJunkAttribute(planSlot, node->mt_resultOidAttno,
                                         &isNull);
            if (isNull)
                elog(ERROR, "tableoid is NULL");
            resultoid = DatumGetObjectId(datum);

            /* If it's not the same as last time, we need to locate the rel */
            if (resultoid != node->mt_lastResultOid)
                resultRelInfo = ExecLookupResultRelByOid(node, resultoid,
                                                         false, true);
        }
#endif

		EvalPlanQualSetSlot(&node->mt_epqstate, planSlot);
		slot = planSlot;

#if PG_VERSION_NUM < PG_VERSION_14
		if (junkfilter != NULL)
			slot = ExecFilterJunk(junkfilter, slot);
#endif

		switch (operation)
		{
			case CMD_INSERT:
#if PG_VERSION_NUM < PG_VERSION_14
				slot = IAExecInsert(node, slot, planSlot,
								  NULL, estate->es_result_relation_info,
								  estate, node->canSetTag, writer);
#else
                /* Initialize projection info if first time for this table */
                if (unlikely(!resultRelInfo->ri_projectNewInfoValid))
                    ExecInitInsertProjection(node, resultRelInfo);
                slot = ExecGetInsertNewTuple(resultRelInfo, planSlot);
                slot = IAExecInsert(node, slot, planSlot, NULL, resultRelInfo,
                                	estate, node->canSetTag, writer);
#endif
				break;
			default:
				elog(ERROR, "unknown direct path operation");
				break;
		}

	}

	DirectWriterClose(writer, estate, resultRelInfo);

#if PG_VERSION_NUM < PG_VERSION_14
	/* Restore es_result_relation_info before exiting */
	estate->es_result_relation_info = saved_resultRelInfo;
#endif

	node->mt_done = true;

	return NULL;
}

void
ExecEndInsertAppendTable(PlanState *pstate)
{
	ModifyTableState *node = castNode(ModifyTableState, pstate);
	ExecEndModifyTable(node);
}

/*
 * Copy of PostgreSQL 14 core log_newpages()
 */
#if PG_VERSION_NUM < PG_VERSION_14
static void
log_newpages(RelFileNode *rnode, ForkNumber forkNum, int num_pages,
             BlockNumber *blknos, Page *pages, bool page_std)
{
    int         flags;
    XLogRecPtr  recptr;
    int         i;
    int         j;

    flags = REGBUF_FORCE_IMAGE;
    if (page_std)
        flags |= REGBUF_STANDARD;

    /*
     * Iterate over all the pages. They are collected into batches of
     * XLR_MAX_BLOCK_ID pages, and a single WAL-record is written for each
     * batch.
     */
    XLogEnsureRecordSpace(XLR_MAX_BLOCK_ID - 1, 0);

    i = 0;
    while (i < num_pages)
    {
        int         batch_start = i;
        int         nbatch;

        XLogBeginInsert();

        nbatch = 0;
        while (nbatch < XLR_MAX_BLOCK_ID && i < num_pages)
        {
            XLogRegisterBlock(nbatch, rnode, forkNum, blknos[i], pages[i], flags);
            i++;
            nbatch++;
        }

        recptr = XLogInsert(RM_XLOG_ID, XLOG_FPI);

        for (j = batch_start; j < i; j++)
        {
            /*
             * The page may be uninitialized. If so, we can't set the LSN
             * because that would corrupt the page.
             */
            if (!PageIsNew(pages[j]))
            {
                PageSetLSN(pages[j], recptr);
            }
        }
    }
}
#endif