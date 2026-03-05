/*-------------------------------------------------------------------------
 *
 * merklebuild.c
 *    Merkle index build and initialization
 *
 * This file implements the index build functions that create a new
 * Merkle index from existing table data.
 *
 * IDENTIFICATION
 *    src/backend/access/merkle/merklebuild.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/merkle.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"
#include "catalog/pg_am_d.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "lib/stringinfo.h"

/*
 * Per-tuple callback state for index build
 */
typedef struct
{
    Relation    indexRel;
    Relation    heapRel;
    double      indtuples;
    int         nkeys;          /* Number of index key columns */
    int         numPartitions;
    int         leavesPerPartition;
    int         nodesPerPartition;
    int         fanout;
    int         internalNodes;  /* nodesPerPartition - leavesPerPartition */
    int         leafStart;      /* 1-indexed start position of leaves */
    int         totalLeaves;    /* numPartitions * leavesPerPartition */
    int         totalNodes;     /* numPartitions * nodesPerPartition */
    int         nodesPerPage;
    int         numTreePages;
    MerkleHash *nodeHashes;     /* per-node accumulated hashes (0-based) */
} MerkleBuildState;

static void merkle_emit_build_nodes_report(Relation indexRel,
                                          MerkleBuildState *buildstate);

static void
merkle_emit_build_nodes_report(Relation indexRel, MerkleBuildState *buildstate)
{
    bool saved_is_bcdb_worker;
    int  partition;

    if (!merkle_update_detection)
        return;
    if (merkle_update_detection_suppress)
        return;
    if (buildstate == NULL || buildstate->nodeHashes == NULL)
        return;

    saved_is_bcdb_worker = is_bcdb_worker;

    PG_TRY();
    {
        StringInfoData out;
        bool first = true;

        is_bcdb_worker = false;

        initStringInfo(&out);

        for (partition = 0; partition < buildstate->numPartitions; partition++)
        {
            int base = partition * buildstate->nodesPerPartition;
            MerkleHash *h = &buildstate->nodeHashes[base];
            char       *hex;

            if (merkle_hash_is_zero(h))
                continue;

            hex = merkle_hash_to_hex(h);

            if (!first)
                appendStringInfoString(&out, " ");
            appendStringInfo(&out, "(%d, %s)", partition, hex);
            first = false;

            pfree(hex);
        }

        if (!first)
            ereport(NOTICE,
                    (errmsg("BCDB_MERKLE_ROOTS: %s", out.data)));

        pfree(out.data);

        is_bcdb_worker = saved_is_bcdb_worker;
    }
    PG_CATCH();
    {
        is_bcdb_worker = saved_is_bcdb_worker;
        FlushErrorState();
    }
    PG_END_TRY();
}

/*
 * merkle_build_callback() - Process one tuple during index build
 */
static void
merkle_build_callback(Relation indexRel,
                      ItemPointer tid,
                      Datum *values,
                      bool *isnull,
                      bool tupleIsAlive,
                      void *state)
{
    MerkleBuildState *buildstate = (MerkleBuildState *) state;
    MerkleHash      hash;
    int             leafId;
    TupleDesc       tupdesc;
    
    /* Only process live tuples */
    if (!tupleIsAlive)
        return;
    
    tupdesc = RelationGetDescr(indexRel);
    
    /* Compute leaf ID using multi-column support and dynamic leaf count */
    leafId = merkle_compute_partition_id(values, isnull,
                                         buildstate->nkeys,
                                         tupdesc,
                                         buildstate->totalLeaves);
    
    /* Compute hash of the full row */
    merkle_compute_row_hash(buildstate->heapRel, tid, &hash);
    
    /*
     * Build optimization: during CREATE INDEX/REINDEX, avoid per-tuple buffer
     * traffic by accumulating XOR only at the leaf node in memory, then
     * constructing internal nodes once at the end.
     */
    {
        int partitionId = leafId / buildstate->leavesPerPartition;
        int leafPos = leafId % buildstate->leavesPerPartition;
        int nodeInPartition = buildstate->leafStart + leafPos;
        int nodeIdx = partitionId * buildstate->nodesPerPartition + (nodeInPartition - 1);

        merkle_hash_xor(&buildstate->nodeHashes[nodeIdx], &hash);
    }
    
    buildstate->indtuples += 1;
}

/*
 * merkleBuild() - Build a new Merkle index
 *
 * This is called when CREATE INDEX is executed. We scan the entire
 * heap table and build the Merkle tree from all existing rows.
 */
IndexBuildResult *
merkleBuild(Relation heapRel, Relation indexRel, struct IndexInfo *indexInfo)
{
    IndexBuildResult   *result;
    MerkleBuildState    buildstate;
    double              reltuples;
    MerkleOptions      *opts;
    int                 totalLeaves;
    bool                saved_undo_suppress;
    
    /*
     * During an index build, the Merkle index is new and will be dropped on
     * error or transaction abort. Recording per-tuple undo state is unnecessary
     * and can consume large amounts of memory for big tables.
     */
    saved_undo_suppress = merkle_undo_suppress;
    merkle_undo_suppress = true;

    PG_TRY();
    {
    /* Get user-specified options or defaults */
    opts = merkle_get_options(indexRel);
    totalLeaves = opts->partitions * opts->leaves_per_partition;
    
    /*
     * Enforce single Merkle index per table
     */
    {
        List       *indexList;
        ListCell   *lc;
        Oid         currentIndexOid = RelationGetRelid(indexRel);

        indexList =    RelationGetIndexList(heapRel);
        foreach(lc, indexList)
        {
            Oid         indexOid = lfirst_oid(lc);
            Relation    otherIndexRel;

            /* Skip the index we are currently building */
            if (indexOid == currentIndexOid)
                continue;

            otherIndexRel = index_open(indexOid, AccessShareLock);
            if (otherIndexRel->rd_rel->relam == MERKLE_AM_OID)
            {
                index_close(otherIndexRel, AccessShareLock);
                list_free(indexList);
                ereport(ERROR,
                        (errcode(ERRCODE_DUPLICATE_OBJECT),
                         errmsg("table \"%s\" already has a Merkle index",
                                RelationGetRelationName(heapRel)),
                         errhint("Only one Merkle index is allowed per table as it hashes the entire row.")));
            }
            index_close(otherIndexRel, AccessShareLock);
        }
        list_free(indexList);
    }

    /*
     * Initialize the index storage with user-specified tree dimensions
     */
    merkle_init_tree(indexRel, RelationGetRelid(heapRel), opts);
    
    /*
     * Prepare in-memory node hash array for build accumulation.
     */
    buildstate.numPartitions = opts->partitions;
    buildstate.leavesPerPartition = opts->leaves_per_partition;
    buildstate.fanout = opts->fanout;
    buildstate.nodesPerPartition = (int) (((int64) buildstate.fanout * (int64) buildstate.leavesPerPartition - 1) /
                                          (buildstate.fanout - 1));
    buildstate.internalNodes = buildstate.nodesPerPartition - buildstate.leavesPerPartition;
    buildstate.leafStart = buildstate.internalNodes + 1;
    buildstate.totalLeaves = totalLeaves;
    buildstate.totalNodes = buildstate.numPartitions * buildstate.nodesPerPartition;
    buildstate.nodesPerPage = (int) MERKLE_MAX_NODES_PER_PAGE;
    buildstate.numTreePages = (buildstate.totalNodes + buildstate.nodesPerPage - 1) / buildstate.nodesPerPage;
    buildstate.nodeHashes = (MerkleHash *) palloc0(sizeof(MerkleHash) * buildstate.totalNodes);

    /* Free options after use */
    pfree(opts);
    
    /*
     * Prepare build state
     */
    buildstate.indexRel = indexRel;
    buildstate.heapRel = heapRel;
    buildstate.indtuples = 0;
    buildstate.nkeys = indexInfo->ii_NumIndexKeyAttrs;
    
    /*
     * Scan the heap and build the index
     */
    reltuples = table_index_build_scan(heapRel, indexRel, indexInfo,
                                       true,   /* allow_sync */
                                       false,  /* progress */
                                       merkle_build_callback,
                                       (void *) &buildstate,
                                       NULL);  /* use heap scan */

    /*
     * Finalize: compute internal nodes from leaves, then write the completed
     * Merkle tree to the index pages.
     */
    {
        int partition;
        int nodeIdx = 0;
        int pageNum;

        /* Construct internal nodes per partition (children first). */
        for (partition = 0; partition < buildstate.numPartitions; partition++)
        {
            int base = partition * buildstate.nodesPerPartition;
            int i;

            for (i = buildstate.internalNodes; i >= 1; i--)
            {
                int child;
                int firstChildIdx = base + buildstate.fanout * (i - 1) + 1;
                MerkleHash h = buildstate.nodeHashes[firstChildIdx];

                for (child = 2; child <= buildstate.fanout; child++)
                    merkle_hash_xor(&h, &buildstate.nodeHashes[base + buildstate.fanout * (i - 1) + child]);

                buildstate.nodeHashes[base + (i - 1)] = h;
            }
        }

        /* Write nodes to index pages in on-disk layout order. */
        for (pageNum = 0; pageNum < buildstate.numTreePages; pageNum++)
        {
            Buffer      buf;
            Page        page;
            MerkleNode *nodes;
            int         nodesThisPage;
            int         i;
            int         pageContentBytes = BLCKSZ - MAXALIGN(SizeOfPageHeaderData);

            nodesThisPage = Min(buildstate.nodesPerPage, buildstate.totalNodes - nodeIdx);

            buf = ReadBuffer(indexRel, MERKLE_TREE_START_BLKNO + pageNum);
            LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
            page = BufferGetPage(buf);
            nodes = (MerkleNode *) PageGetContents(page);

            for (i = 0; i < nodesThisPage; i++)
            {
                nodes[i].nodeId = nodeIdx + i;
                nodes[i].hash = buildstate.nodeHashes[nodeIdx + i];
            }

            if (nodesThisPage * (int)sizeof(MerkleNode) < pageContentBytes)
            {
                memset(((char *) nodes) + nodesThisPage * sizeof(MerkleNode), 0,
                       pageContentBytes - nodesThisPage * sizeof(MerkleNode));
            }

            MarkBufferDirty(buf);
            UnlockReleaseBuffer(buf);

            nodeIdx += nodesThisPage;
        }
    }
    
    /*
     * Return statistics
     */
    result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
    result->heap_tuples = reltuples;
    result->index_tuples = buildstate.indtuples;
    
    merkle_emit_build_nodes_report(indexRel, &buildstate);
    }
    PG_CATCH();
    {
        merkle_undo_suppress = saved_undo_suppress;
        PG_RE_THROW();
    }
    PG_END_TRY();

    merkle_undo_suppress = saved_undo_suppress;
    return result;
}

/*
 * merkleBuildempty() - Build an empty Merkle index
 *
 * This function is part of the PostgreSQL Index AM interface. It is specifically
 * called for UNLOGGED tables to create the 'initial fork' (INIT_FORKNUM).
 *
 * Unlogged tables do not write WAL, so on a crash/restart, PostgreSQL truncates
 * the index to the state created by this function. While AriaBC (blockchain) 
 * typically uses logged durable tables, this function is required for completeness
 * and to support UNLOGGED relations.
 */
void
merkleBuildempty(Relation indexRel)
{
    Page        metapage;
    MerkleMetaPageData *meta;
    MerkleOptions *opts;
    int         numPartitions;
    int         leavesPerPartition;
    int         fanout;
    int         nodesPerPartition;
    int         totalNodes;
    int         nodesPerPage;
    int         numTreePages;
    int         nodeIdx;
    int         pageNum;

    /*
     * Construct metadata page using defaults
     */
    metapage = (Page) palloc(BLCKSZ);
    PageInit(metapage, BLCKSZ, 0);
    
    /* Respect reloptions for INIT_FORKNUM on UNLOGGED relations */
    opts = merkle_get_options(indexRel);
    numPartitions = opts->partitions;
    leavesPerPartition = opts->leaves_per_partition;
    fanout = opts->fanout;

    if (fanout < 2 || fanout > 1024)
        fanout = MERKLE_DEFAULT_FANOUT;

    nodesPerPartition = (int) (((int64) fanout * (int64) leavesPerPartition - 1) / (fanout - 1));

    nodesPerPage = (int)MERKLE_MAX_NODES_PER_PAGE;
    totalNodes = numPartitions * nodesPerPartition;
    numTreePages = (totalNodes + nodesPerPage - 1) / nodesPerPage;

    meta = MerklePageGetMeta(metapage);
    meta->version = MERKLE_VERSION;
    meta->heapRelid = InvalidOid;  /* Will be set on first insert */
    meta->numPartitions = numPartitions;
    meta->leavesPerPartition = leavesPerPartition;
    meta->nodesPerPartition = nodesPerPartition;
    meta->totalNodes = totalNodes;
    meta->nodesPerPage = nodesPerPage;
    meta->numTreePages = numTreePages;
    meta->fanout = fanout;

    pfree(opts);
    
    /*
     * Make sure we have the smgr relation open
     */
    RelationOpenSmgr(indexRel);

    /*
     * Write metadata page
     */
    PageSetChecksumInplace(metapage, MERKLE_METAPAGE_BLKNO);
    smgrwrite(indexRel->rd_smgr, INIT_FORKNUM, MERKLE_METAPAGE_BLKNO,
              (char *) metapage, true);
    log_newpage(&indexRel->rd_smgr->smgr_rnode.node, INIT_FORKNUM,
                MERKLE_METAPAGE_BLKNO, metapage, true);
    
    /*
     * Construct and write tree node pages
     */
    nodeIdx = 0;
    for (pageNum = 0; pageNum < numTreePages; pageNum++)
    {
        Page        treepage;
        MerkleNode *nodes;
        int         nodesThisPage;
        int         i;

        treepage = (Page) palloc(BLCKSZ);
        PageInit(treepage, BLCKSZ, 0);
        
        nodes = (MerkleNode *) PageGetContents(treepage);
        memset(nodes, 0, BLCKSZ - MAXALIGN(SizeOfPageHeaderData));
        
        nodesThisPage = Min(nodesPerPage, totalNodes - nodeIdx);
        
        for (i = 0; i < nodesThisPage; i++)
        {
            nodes[i].nodeId = nodeIdx + i;
            merkle_hash_zero(&nodes[i].hash);
        }
        
        nodeIdx += nodesThisPage;

        PageSetChecksumInplace(treepage, MERKLE_TREE_START_BLKNO + pageNum);
        smgrwrite(indexRel->rd_smgr, INIT_FORKNUM, MERKLE_TREE_START_BLKNO + pageNum,
                  (char *) treepage, true);
        log_newpage(&indexRel->rd_smgr->smgr_rnode.node, INIT_FORKNUM,
                    MERKLE_TREE_START_BLKNO + pageNum, treepage, true);
        
        pfree(treepage);
    }
    
    /*
     * Sync to disk
     */
    smgrimmedsync(indexRel->rd_smgr, INIT_FORKNUM);
    
    pfree(metapage);
}
