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
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
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
	MerkleRoute route;
    int         nkeys;
    
	/* The executor briefly suppresses the generic AM callback for UPDATE so
	 * it can apply the full-row Merkle delta exactly once below.  A normal
	 * INSERT/DELETE with maintenance disabled must fail closed instead of
	 * silently leaving the aggregate stale. */
    if (!enable_merkle_index)
	{
		if (merkle_index_maintenance_suppress)
			return false;
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("Merkle maintenance is disabled for index \"%s\"",
						RelationGetRelationName(indexRel)),
				 errhint("Set enable_merkle_index=on before modifying a Merkle-indexed table.")));
	}

    nkeys = indexInfo->ii_NumIndexKeyAttrs;
	merkle_compute_route(indexRel, values, isnull, nkeys, &route);
    
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
	 * Stage the Merkle delta event for this insert.
	 */
	merkle_stage_delta_event(indexRel, MERKLE_DELTA_INSERT, NULL, route.route_digest, &hash);
    
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
static void
merkle_populate_index_stats(Oid index_oid, IndexBulkDeleteResult *stats)
{
    if (SPI_connect() == SPI_OK_CONNECT)
    {
        Oid argtypes[1] = {OIDOID};
        Datum values[1] = {ObjectIdGetDatum(index_oid)};
        int spi_rc = SPI_execute_with_args(
            "SELECT COALESCE(sum(tuple_count), 0)::float8, count(*)::float8 "
            "  FROM ariabc_internal.merkle_node "
            " WHERE index_oid = $1 AND is_leaf = true",
            1, argtypes, values, NULL, true, 0);

        if (spi_rc == SPI_OK_SELECT && SPI_processed > 0 && SPI_tuptable != NULL)
        {
            bool isnull;
            Datum tuples_datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
            Datum count_datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull);

            if (!isnull)
                stats->num_index_tuples = DatumGetFloat8(tuples_datum);
            if (!isnull)
                stats->num_pages = (int) Max(1.0, DatumGetFloat8(count_datum));
            SPI_freetuptable(SPI_tuptable);
        }
        SPI_finish();
    }
}

IndexBulkDeleteResult *
merkleBulkdelete(IndexVacuumInfo *info,
                 IndexBulkDeleteResult *stats,
                 IndexBulkDeleteCallback callback,
                 void *callback_state)
{
    (void) callback;
    (void) callback_state;

    if (stats == NULL)
        stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

    if (info && info->index)
        merkle_populate_index_stats(RelationGetRelid(info->index), stats);

    return stats;
}

IndexBulkDeleteResult *
merkleVacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
    if (stats == NULL)
        stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

    if (info && info->index)
        merkle_populate_index_stats(RelationGetRelid(info->index), stats);
    else
        stats->num_pages = 1;

    return stats;
}
