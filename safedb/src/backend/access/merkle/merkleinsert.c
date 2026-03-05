/*-------------------------------------------------------------------------
 *
 * merkleinsert.c
 *    Merkle index insert and delete operations
 *
 * This file implements the index modification functions that are called
 * automatically by PostgreSQL when rows are inserted, updated, or deleted.
 *
 * Copyright (c) 2026, Neel Parekh
 *
 * IDENTIFICATION
 *    src/backend/access/merkle/merkleinsert.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/merkle.h"
#include "access/heapam.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"

/*
 * merkleInsert() - Insert a new entry into the Merkle index
 *
 * This is called by PostgreSQL's executor after every INSERT into the
 * indexed table. We:
 * 1. Fetch the full tuple from the heap
 * 2. Compute its hash
 * 3. Determine which leaf it maps to
 * 4. XOR the hash into the leaf and propagate up the partition-root
 *
 * Note: For UPDATE, PostgreSQL calls ambulkdelete for the old row
 * and aminsert for the new row (or just aminsert if the key didn't change).
 */
bool
merkleInsert(Relation indexRel, Datum *values, bool *isnull,
             ItemPointer ht_ctid, Relation heapRel,
             IndexUniqueCheck checkUnique,
             struct IndexInfo *indexInfo)
{
    MerkleHash  hash;
    int         partitionId;
    TupleDesc   tupdesc;
    int         nkeys;
    int         totalLeaves;
    
    /* GUC: Check if Merkle index updates are enabled */
    if (!enable_merkle_index)
        return false;

    tupdesc = RelationGetDescr(indexRel);
    nkeys = indexInfo->ii_NumIndexKeyAttrs;

    /* Read tree configuration from metadata */
    merkle_read_meta(indexRel, NULL, NULL, NULL, NULL, &totalLeaves, NULL, NULL, NULL);
    
    /*
     * Compute partition ID from the indexed key values (supports multi-column)
     */
    partitionId = merkle_compute_partition_id(values, isnull,
                                                     nkeys, tupdesc,
                                                     totalLeaves);
    
    /*
     * Compute hash of the full row data
     * 
     * We fetch the actual tuple from the heap because we want to hash
     * ALL columns, not just the indexed column. This provides full row
     * integrity verification.
     * 
     * CRITICAL FIX: Skip if TID is invalid to avoid warnings and failures.
     */
    if (!ItemPointerIsValid(ht_ctid) || 
        ItemPointerGetBlockNumberNoCheck(ht_ctid) == InvalidBlockNumber)
    {
        return false;
    }

    merkle_compute_row_hash(heapRel, ht_ctid, &hash);
    
    /*
     * Update the Merkle tree path from leaf to root
     */
    merkle_update_tree_path(indexRel, partitionId, &hash, true);
    
    /*
     * We don't detect duplicates - merkle index doesn't enforce uniqueness
     */
    return false;
}

/*
 * merkleBulkdelete() - Statistics-only stub for VACUUM operations
 *
 * IMPORTANT: This function does NOT actually delete anything from the Merkle index!
 *
 * PostgreSQL's index AM API requires this callback, but for Merkle indexes,
 * the actual hash removal (XOR-out) happens BEFORE deletion in the executor
 * layer (see ExecDeleteMerkleIndexes in nodeModifyTable.c). This is necessary
 * because:
 *
 * 1. By the time VACUUM runs, deleted tuples may be physically gone from the heap
 * 2. We need the full row data to compute its hash and partition ID
 * 3. Synchronous deletion in the executor guarantees we always have tuple access
 *
 * This function only collects and reports index statistics for VACUUM's benefit.
 * The callback function tells us which heap TIDs are dead, but we cannot act on
 * them since we cannot retrieve the original row data to compute hashes.
 *
 * Design note: A more sophisticated implementation could store (tid, hash, partition)
 * tuples in the index to enable proper VACUUM cleanup, but that would significantly
 * increase storage overhead and complexity.
 */
IndexBulkDeleteResult *
merkleBulkdelete(IndexVacuumInfo *info,
                 IndexBulkDeleteResult *stats,
                 IndexBulkDeleteCallback callback,
                 void *callback_state)
{
    Relation        indexRel = info->index;
    Relation        heapRel;
    int             i;
    int             totalNodes;
    int             nodesPerPage;
    int             numTreePages;
    int             nodeIdx;
    int             pageNum;
    
    /* Allocate stats if not provided */
    if (stats == NULL)
        stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));
    
    /*
     * We only collect statistics here. The actual Merkle tree updates happen in:
     * - ExecDeleteMerkleIndexes() - called during DELETE/UPDATE to XOR out old hash
     * - ExecInsertMerkleIndexes() - called during UPDATE to XOR in new hash
     * Both are in src/backend/executor/nodeModifyTable.c
     *
     * This ensures hash removal happens BEFORE the tuple is deleted from the heap,
     * guaranteeing we can always access the full row data needed for hashing.
     */
    
    heapRel = table_open(IndexGetRelation(RelationGetRelid(indexRel), false),
                         AccessShareLock);

    /* Read tree configuration from metadata */
    merkle_read_meta(indexRel, NULL, NULL, NULL, &totalNodes, NULL,
                     &nodesPerPage, &numTreePages, NULL);

    /* Count non-zero nodes across all tree pages */
    nodeIdx = 0;
    for (pageNum = 0; pageNum < numTreePages; pageNum++)
    {
        Buffer      treebuf;
        Page        treepage;
        MerkleNode *nodes;
        int         nodesThisPage;

        treebuf = ReadBuffer(indexRel, MERKLE_TREE_START_BLKNO + pageNum);
        LockBuffer(treebuf, BUFFER_LOCK_SHARE);
        treepage = BufferGetPage(treebuf);
        nodes = (MerkleNode *) PageGetContents(treepage);

        nodesThisPage = Min(nodesPerPage, totalNodes - nodeIdx);
        for (i = 0; i < nodesThisPage; i++)
        {
            if (!merkle_hash_is_zero(&nodes[i].hash))
                stats->num_index_tuples++;
        }

        nodeIdx += nodesThisPage;
        UnlockReleaseBuffer(treebuf);
    }

    table_close(heapRel, AccessShareLock);
    
    return stats;
}

/*
 * merkleVacuumcleanup() - Post-VACUUM index statistics update
 *
 * Called after bulk deletion to perform any necessary cleanup and update
 * index statistics. Like merkleBulkdelete(), this function does NOT modify
 * the Merkle tree structure - it only reports current index statistics.
 *
 * The Merkle tree is always kept up-to-date by synchronous insert/delete
 * operations in the executor layer, so no cleanup is needed here.
 */
IndexBulkDeleteResult *
merkleVacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
    Relation    indexRel = info->index;
    int         nodesPerPartition;
    int         totalNodes;
    int         nodesPerPage;
    int         numTreePages;
    int         nodeIdx;
    int         pageNum;
    
    /* Allocate stats if not provided (no deletions occurred) */
    if (stats == NULL)
        stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));
    
    /*
     * Update index statistics
     */
    merkle_read_meta(indexRel, NULL, NULL, &nodesPerPartition, &totalNodes, NULL,
                     &nodesPerPage, &numTreePages, NULL);

    stats->num_pages = numTreePages + 1;  /* metadata + tree pages */
    stats->num_index_tuples = 0;

    nodeIdx = 0;
    for (pageNum = 0; pageNum < numTreePages; pageNum++)
    {
        Buffer      buf;
        Page        page;
        MerkleNode *nodes;
        int         nodesThisPage;
        int         j;

        buf = ReadBuffer(indexRel, MERKLE_TREE_START_BLKNO + pageNum);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);
        nodes = (MerkleNode *) PageGetContents(page);

        nodesThisPage = Min(nodesPerPage, totalNodes - nodeIdx);
        for (j = 0; j < nodesThisPage; j++)
        {
            int globalIdx = nodeIdx + j;

            /* Root of each partition is the first node in the partition */
            if (globalIdx % nodesPerPartition == 0 &&
                !merkle_hash_is_zero(&nodes[j].hash))
                stats->num_index_tuples++;
        }

        nodeIdx += nodesThisPage;
        UnlockReleaseBuffer(buf);
    }
    
    return stats;
}
