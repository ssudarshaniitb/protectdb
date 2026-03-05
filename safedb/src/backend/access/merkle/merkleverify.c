/*-------------------------------------------------------------------------
 *
 * merkleverify.c
 *    SQL-callable verification functions for Merkle index
 *
 * These functions allow users to verify the integrity of their data
 * by recomputing the Merkle tree from table data and comparing with
 * the stored tree.
 *
 * Copyright (c) 2026, Neel Parekh
 *
 * IDENTIFICATION
 *    src/backend/access/merkle/merkleverify.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/merkle.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/indexing.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_class.h"
#include "catalog/pg_index.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "parser/parse_coerce.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"


PG_FUNCTION_INFO_V1(merkle_leaf_id);
PG_FUNCTION_INFO_V1(merkle_verify);
PG_FUNCTION_INFO_V1(merkle_root_hash);
PG_FUNCTION_INFO_V1(merkle_tree_stats);
PG_FUNCTION_INFO_V1(merkle_node_hash);
PG_FUNCTION_INFO_V1(merkle_leaf_tuples);

/*
 * find_merkle_index() - Find the Merkle index on a table
 *
 * Returns the OID of the first Merkle index found, or InvalidOid if none.
 */
static Oid
find_merkle_index(Oid relid)
{
    Relation    rel;
    List       *indexList;
    ListCell   *lc;
    Oid         result = InvalidOid;
    char        relkind;
    
    /*
     * Verify that the OID refers to a regular table, not an index or other
     * relation type. This prevents confusing errors when users accidentally
     * pass an index OID instead of a table OID.
     */
    relkind = get_rel_relkind(relid);
    if (relkind == '\0')
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                 errmsg("relation with OID %u does not exist", relid)));
    
    if (relkind != RELKIND_RELATION && relkind != RELKIND_PARTITIONED_TABLE)
        ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("\"%.128s\" is not a table",
                        get_rel_name(relid)),
                 errhint("Merkle verification functions expect a table OID, not an index or other object.")));
    
    rel = table_open(relid, AccessShareLock);
    indexList = RelationGetIndexList(rel);
    
    foreach(lc, indexList)
    {
        Oid         indexOid = lfirst_oid(lc);
        Relation    indexRel;
        
        indexRel = index_open(indexOid, AccessShareLock);
        
        /* Check if this is a Merkle index by checking access method */
        if (indexRel->rd_rel->relam == MERKLE_AM_OID)
        {
            result = indexOid;
            index_close(indexRel, AccessShareLock);
            break;
        }
        
        index_close(indexRel, AccessShareLock);
    }
    
    list_free(indexList);
    table_close(rel, AccessShareLock);
    
    return result;
}

/*
 * merkle_verify() - Verify Merkle tree integrity.
 *
 * This is the main user-facing verification tool. It performs a full audit:
 * 1. Scans the entire heap table (the actual data).
 * 2. Recomputes what the Merkle tree *should* look like based on that data.
 * 3. Compares this recomputed tree with the stored Merkle index.
 * 
 * Returns TRUE if everything matches exactly.
 * Logs WARNINGS for any specific node mismatches found.
 * 
 * Usage: SELECT merkle_verify('tablename');
 */
Datum
merkle_verify(PG_FUNCTION_ARGS)
{
    Oid             relid = PG_GETARG_OID(0);
    Oid             indexOid;
    Relation        heapRel;
    Relation        indexRel;
    TableScanDesc   scan;
    TupleTableSlot *slot;
    MerkleHash     *computedTree;
    bool            match = true;
    int             i;
    TupleDesc       indexTupdesc;
    int             nkeys;
    int16          *indkey;         /* Heap column numbers for indexed keys */
    Datum          *keyValues;      /* Temporary storage for key values */
    bool           *keyNulls;       /* Temporary storage for null flags */
    int             numPartitions;
    int             leavesPerPartition;
    int             nodesPerPartition;
    int             totalNodes;
    int             totalLeaves;
    int             fanout;
    int             internalNodes;
    int             leafStart;
    
    /* Find the Merkle index on this table */
    indexOid = find_merkle_index(relid);
    if (!OidIsValid(indexOid))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("no merkle index found on table %s",
                        get_rel_name(relid))));
    
    /* Open relations */
    heapRel = table_open(relid, AccessShareLock);
    indexRel = index_open(indexOid, AccessShareLock);
    
    /* Read tree configuration from metadata */
    merkle_read_meta(indexRel, &numPartitions, &leavesPerPartition, &nodesPerPartition,
                     &totalNodes, &totalLeaves, NULL, NULL, &fanout);

    internalNodes = nodesPerPartition - leavesPerPartition;
    leafStart = internalNodes + 1;
    
    /* Get index key information */
    indexTupdesc = RelationGetDescr(indexRel);
    nkeys = indexRel->rd_index->indnkeyatts;
    indkey = indexRel->rd_index->indkey.values;
    
    /* Allocate key value arrays */
    keyValues = (Datum *) palloc(nkeys * sizeof(Datum));
    keyNulls = (bool *) palloc(nkeys * sizeof(bool));
    
    /* Allocate space for computed tree using dynamic size */
    computedTree = (MerkleHash *) palloc0(totalNodes * sizeof(MerkleHash));
    
    /* Scan the heap table and recompute the tree */
    slot = table_slot_create(heapRel, NULL);
    /*
     * Use GetActiveSnapshot() to see only committed, live tuples.
     * SnapshotAny would include dead tuples that were deleted, which
     * would cause verification mismatches.
     * 
     * NOTE: GetActiveSnapshot() returns a snapshot that is already managed
     * by the active snapshot stack. We should NOT call RegisterSnapshot() on it
     * as that would cause double registration and snapshot reference leaks.
     */
    scan = table_beginscan(heapRel, GetActiveSnapshot(), 0, NULL);
    
    while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
    {
        MerkleHash  hash;
        int         leafId;
        int         partitionId;
        int         leafPos;
        int         nodeInPartition;
        int         nodeIdx;
        
        /* Extract all indexed column values from heap tuple */
        for (i = 0; i < nkeys; i++)
        {
            int heapAttr = indkey[i];  /* 1-based heap column number */
            keyValues[i] = slot_getattr(slot, heapAttr, &keyNulls[i]);
        }
        
        /* Compute partition ID using multi-column function */
        leafId = merkle_compute_partition_id(keyValues, keyNulls,
                                             nkeys, indexTupdesc,
                                             totalLeaves);
        
        /* Compute hash of this row (slot already contains the tuple) */
        merkle_compute_slot_hash(heapRel, slot, &hash);
        
        /*
         * Verification optimization: accumulate XOR only at the leaf node in
         * memory, then construct internal nodes bottom-up after the scan.
         *
         * This is equivalent to XORing the row hash into every ancestor on the
         * path, but avoids O(rows * log(leaves)) updates.
         */
        partitionId = leafId / leavesPerPartition;
        leafPos = leafId % leavesPerPartition;
        nodeInPartition = leafStart + leafPos; /* 1-indexed */
        nodeIdx = partitionId * nodesPerPartition + (nodeInPartition - 1);
        merkle_hash_xor(&computedTree[nodeIdx], &hash);
    }
    
    table_endscan(scan);
    ExecDropSingleTupleTableSlot(slot);
    
    /*
     * Construct internal nodes bottom-up within each partition:
     * parent = XOR of all children
     */
    {
        int partition;
        
        for (partition = 0; partition < numPartitions; partition++)
        {
            int base = partition * nodesPerPartition;
            int nodeInPartition;
            
            for (nodeInPartition = internalNodes; nodeInPartition >= 1; nodeInPartition--)
            {
                int parentIdx = base + (nodeInPartition - 1);
                int child;
                int firstChildIdx = base + fanout * (nodeInPartition - 1) + 1;
                MerkleHash h = computedTree[firstChildIdx];
                
                for (child = 2; child <= fanout; child++)
                    merkle_hash_xor(&h, &computedTree[base + fanout * (nodeInPartition - 1) + child]);

                computedTree[parentIdx] = h;
            }
        }
    }
    
    /* Compare computed tree with stored tree - supports multi-page storage */
    {
        int nodesPerPage = (int)MERKLE_MAX_NODES_PER_PAGE;
        int numTreePages = (totalNodes + nodesPerPage - 1) / nodesPerPage;
        int nodeIdx = 0;
        int pageNum;
        
        for (pageNum = 0; pageNum < numTreePages; pageNum++)
        {
            Buffer      buf;
            Page        page;
            MerkleNode *storedNodes;
            int         nodesThisPage;
            int         j;
            
            buf = ReadBuffer(indexRel, MERKLE_TREE_START_BLKNO + pageNum);
            LockBuffer(buf, BUFFER_LOCK_SHARE);
            page = BufferGetPage(buf);
            storedNodes = (MerkleNode *) PageGetContents(page);
            
            nodesThisPage = Min(nodesPerPage, totalNodes - nodeIdx);
            
            for (j = 0; j < nodesThisPage; j++)
            {
                if (memcmp(computedTree[nodeIdx + j].data, storedNodes[j].hash.data,
                           MERKLE_HASH_BYTES) != 0)
                {
                    match = false;
                    ereport(WARNING,
                            (errmsg("merkle tree mismatch at node %d: computed %s, stored %s",
                                    nodeIdx + j,
                                    merkle_hash_to_hex(&computedTree[nodeIdx + j]),
                                    merkle_hash_to_hex(&storedNodes[j].hash))));
                }
            }
            
            nodeIdx += nodesThisPage;
            UnlockReleaseBuffer(buf);
        }
    }
    
    /* Cleanup */
    pfree(computedTree);
    pfree(keyValues);
    pfree(keyNulls);
    index_close(indexRel, AccessShareLock);
    table_close(heapRel, AccessShareLock);
    
    PG_RETURN_BOOL(match);
}

/*
 * merkle_root_hash() - Get combined root hash of all partitions
 *
 * Returns the XOR of all partition root hashes as a hex string.
 * This provides a single hash representing the entire table's integrity state.
 *
 * Optimized to iterate page-wise to minimize buffer lock/unlock overhead.
 *
 * Usage: SELECT merkle_root_hash('tablename');
 */
Datum
merkle_root_hash(PG_FUNCTION_ARGS)
{
    Oid             relid = PG_GETARG_OID(0);
    Oid             indexOid;
    Relation        indexRel;
    MerkleHash      combinedHash;
    char           *result;
    int             numPartitions;
    int             nodesPerPartition;
    int             nodesPerPage;
    int             numTreePages;
    int             totalNodes;
    int             pageNum;
    int             nodeIdx;
    
    /* Find the Merkle index on this table */
    indexOid = find_merkle_index(relid);
    if (!OidIsValid(indexOid))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("no merkle index found on table %s",
                        get_rel_name(relid))));
    
    /* Open index */
    indexRel = index_open(indexOid, AccessShareLock);
    
    /* Read tree configuration from metadata */
    merkle_read_meta(indexRel, &numPartitions, NULL, &nodesPerPartition, &totalNodes, NULL,
                     &nodesPerPage, &numTreePages, NULL);
    
    /* 
     * Combine all partition roots by XOR - page-wise iteration.
     * Root of partition i is at global index (i * nodesPerPartition).
     */
    merkle_hash_zero(&combinedHash);
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
            
            /* Check if this node is a partition root (first node of each partition) */
            if (globalIdx % nodesPerPartition == 0)
            {
                merkle_hash_xor(&combinedHash, &nodes[j].hash);
            }
        }
        
        nodeIdx += nodesThisPage;
        UnlockReleaseBuffer(buf);
    }
    
    index_close(indexRel, AccessShareLock);
    
    /* Convert to hex string */
    result = merkle_hash_to_hex(&combinedHash);
    
    PG_RETURN_TEXT_P(cstring_to_text(result));
}

/*
 * merkle_tree_stats() - Return statistics about the Merkle tree
 *
 * Returns JSON with tree configuration and statistics.
 *
 * Usage: SELECT merkle_tree_stats('tablename');
 */
Datum
merkle_tree_stats(PG_FUNCTION_ARGS)
{
    StringInfoData  keybuf;
    int             nonZeroNodes = 0;
    int             totalNodes;
    int             nodesPerPage;
    int             numTreePages;
    Oid             relid = PG_GETARG_OID(0);
    Oid             indexOid;
    Relation        heapRel;
    Relation        indexRel;
    Buffer          metabuf;
    Page            metapage;
    MerkleMetaPageData *meta;
    StringInfoData  buf;
    int             nodeIdx;
    int             pageNum;
    int             nkeys;
    int             i;
    TupleDesc       heapTupdesc;
    int             fanout;
    
    /* Find the Merkle index on this table */
    indexOid = find_merkle_index(relid);
    if (!OidIsValid(indexOid))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("no merkle index found on table %s",
                        get_rel_name(relid))));
    
    /* Open heap and index */
    heapRel = table_open(relid, AccessShareLock);
    indexRel = index_open(indexOid, AccessShareLock);
    heapTupdesc = RelationGetDescr(heapRel);
    
    /* Read metadata page */
    metabuf = ReadBuffer(indexRel, MERKLE_METAPAGE_BLKNO);
    LockBuffer(metabuf, BUFFER_LOCK_SHARE);
    metapage = BufferGetPage(metabuf);
    meta = MerklePageGetMeta(metapage);
    
    /* Get values we need before releasing the meta buffer */
    totalNodes = meta->totalNodes;
    nodesPerPage = meta->nodesPerPage;
    numTreePages = meta->numTreePages;
    fanout = (meta->version >= 5) ? meta->fanout : MERKLE_DEFAULT_FANOUT;
    
    /* Count non-zero nodes across all tree pages */
    nodeIdx = 0;
    for (pageNum = 0; pageNum < numTreePages; pageNum++)
    {
        Buffer      treebuf;
        Page        treepage;
        MerkleNode *nodes;
        int         nodesThisPage;
        int         j;
        
        treebuf = ReadBuffer(indexRel, MERKLE_TREE_START_BLKNO + pageNum);
        LockBuffer(treebuf, BUFFER_LOCK_SHARE);
        treepage = BufferGetPage(treebuf);
        nodes = (MerkleNode *) PageGetContents(treepage);
        
        nodesThisPage = Min(nodesPerPage, totalNodes - nodeIdx);
        
        for (j = 0; j < nodesThisPage; j++)
        {
            if (!merkle_hash_is_zero(&nodes[j].hash))
                nonZeroNodes++;
        }
        
        nodeIdx += nodesThisPage;
        UnlockReleaseBuffer(treebuf);
    }
    
    /* Get indexed column names from pg_index */
    nkeys = indexRel->rd_index->indnkeyatts;
    initStringInfo(&keybuf);
    appendStringInfoChar(&keybuf, '[');
    for (i = 0; i < nkeys; i++)
    {
        int16 attnum = indexRel->rd_index->indkey.values[i];
        const char *colname;
        
        if (i > 0)
            appendStringInfoString(&keybuf, ", ");
        
        if (attnum > 0 && attnum <= heapTupdesc->natts)
            colname = NameStr(TupleDescAttr(heapTupdesc, attnum - 1)->attname);
        else
            colname = "?";
        
        appendStringInfo(&keybuf, "\"%s\"", colname);
    }
    appendStringInfoChar(&keybuf, ']');
    
    /* Build JSON result */
    initStringInfo(&buf);
    appendStringInfo(&buf, 
                     "{\"version\": %u, "
                     "\"num_partitions\": %d, "
                     "\"leaves_per_partition\": %d, "
                     "\"fanout\": %d, "
                     "\"nodes_per_partition\": %d, "
                     "\"total_nodes\": %d, "
                     "\"num_pages\": %d, "
                     "\"non_zero_nodes\": %d, "
                     "\"hash_bits\": %d, "
                     "\"index_keys\": %s}",
                     meta->version,
                     meta->numPartitions,
                     meta->leavesPerPartition,
                     fanout,
                     meta->nodesPerPartition,
                     meta->totalNodes,
                     meta->numTreePages,
                     nonZeroNodes,
                     MERKLE_HASH_BITS,
                     keybuf.data);
    
    UnlockReleaseBuffer(metabuf);
    index_close(indexRel, AccessShareLock);
    table_close(heapRel, AccessShareLock);
    
    PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

/*
 * merkle_node_hash() - Set-returning function to view all node hashes
 *
 * Returns a table of all nodes in the Merkle tree with their hashes.
 * Can be filtered with WHERE clause on nodeid column.
 *
 * Usage:
 *   SELECT * FROM merkle_node_hash('tablename'::regclass);
 *   SELECT * FROM merkle_node_hash('tablename'::regclass) WHERE nodeid = '10_4';
 *
 * Output columns: nodeid, partition, node_in_partition, is_leaf, hash
 */
/*
 * Data structure to hold pre-computed node information for SRF iteration.
 * Defined at file scope to avoid duplicate typedef in per-call section.
 */
typedef struct NodeHashData
{
    Datum  *nodeids;        /* "partition_nodeInPartition" formatted string */
    Datum  *partitions;     /* partition index */
    Datum  *nodeinpartitions;    /* 1-indexed node position within partition */
    bool   *isleafs;        /* true if this is a leaf node */
    Datum  *leafids;        /* global leaf ID (NULL for non-leaves) */
    bool   *leafidnulls;    /* true if leafid should be NULL */
    Datum  *hashes;         /* hex-formatted hash value */
} NodeHashData;

/*
 * merkle_node_hash() - Debugging tool to inspect tree state.
 *
 * This function returns the raw internal state of the Merkle tree.
 * It dumps every node, its ID, location, and current hash value.
 * 
 * Use this to pinpoint exactly *where* the tree is corrupt or to understand
 * the tree structure (partitions, leaves, parents).
 * 
 * It is a Set Returning Function (SRF), so query it like a table.
 *
 * Usage: SELECT * FROM merkle_node_hash('tablename');
 */
Datum
merkle_node_hash(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    MemoryContext    oldcontext;
    
    /* First call setup - read all data and cache in memory */
    if (SRF_IS_FIRSTCALL())
    {
        Oid             relid = PG_GETARG_OID(0);
        Oid             indexOid;
        Relation        indexRel;
        TupleDesc       tupdesc;
        NodeHashData   *data;
        int             numPartitions;
        int             nodesPerPartition;
        int             leavesPerPartition;
        int             totalNodes;
        int             nodesPerPage;
        int             numTreePages;
        int             nodeIdx;
        int             pageNum;
        int             leafStart;
        
        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        
        /* Find Merkle index */
        indexOid = find_merkle_index(relid);
        if (!OidIsValid(indexOid))
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                     errmsg("no merkle index found on table %s",
                            get_rel_name(relid))));
        
        indexRel = index_open(indexOid, AccessShareLock);
        
        /* Read tree configuration from metadata */
        merkle_read_meta(indexRel, &numPartitions, &leavesPerPartition, &nodesPerPartition,
                         &totalNodes, NULL, &nodesPerPage, &numTreePages, NULL);

        leafStart = nodesPerPartition - leavesPerPartition + 1;
        
        /* Allocate result arrays in multi-call context (will persist across calls) */
        data = palloc(sizeof(NodeHashData));
        data->nodeids = palloc(totalNodes * sizeof(Datum));
        data->partitions = palloc(totalNodes * sizeof(Datum));
        data->nodeinpartitions = palloc(totalNodes * sizeof(Datum));
        data->isleafs = palloc(totalNodes * sizeof(bool));
        data->hashes = palloc(totalNodes * sizeof(Datum));
        data->leafids = palloc(totalNodes * sizeof(Datum));
        data->leafidnulls = palloc(totalNodes * sizeof(bool));
        
        /*
         * Read all node data from tree pages.
         * We iterate page-wise and deep-copy all data into our arrays
         * so we can safely release the buffer locks before returning.
         */
        nodeIdx = 0;
        for (pageNum = 0; pageNum < numTreePages && nodeIdx < totalNodes; pageNum++)
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
                int partition = globalIdx / nodesPerPartition;
                int nodeInPartition = (globalIdx % nodesPerPartition) + 1;  /* 1-indexed */
                bool isLeaf = (nodeInPartition >= leafStart);
                int leafId = -1;
                char nodeid_str[32];
                
                /* Format node ID string */
                snprintf(nodeid_str, sizeof(nodeid_str), "%d_%d", partition, nodeInPartition);
                
                /* Compute global leaf ID for leaf nodes */
                if (isLeaf)
                {
                    int leafInPartition = nodeInPartition - leafStart;
                    leafId = partition * leavesPerPartition + leafInPartition;
                }
                
                /* Deep copy all values into the persistent memory context */
                data->nodeids[globalIdx] = CStringGetTextDatum(nodeid_str);
                data->partitions[globalIdx] = Int32GetDatum(partition);
                data->nodeinpartitions[globalIdx] = Int32GetDatum(nodeInPartition);
                data->isleafs[globalIdx] = isLeaf;
                data->hashes[globalIdx] = CStringGetTextDatum(merkle_hash_to_hex(&nodes[j].hash));
                data->leafids[globalIdx] = Int32GetDatum(leafId);
                data->leafidnulls[globalIdx] = !isLeaf;
            }
            
            nodeIdx += nodesThisPage;
            UnlockReleaseBuffer(buf);
        }
        
        index_close(indexRel, AccessShareLock);
        
        /* Build tuple descriptor for result set */
        tupdesc = CreateTemplateTupleDesc(6);
        TupleDescInitEntry(tupdesc, 1, "nodeid", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 2, "partition", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, 3, "node_in_partition", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, 4, "is_leaf", BOOLOID, -1, 0);
        TupleDescInitEntry(tupdesc, 5, "leaf_id", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, 6, "hash", TEXTOID, -1, 0);
        
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->max_calls = totalNodes;
        funcctx->user_fctx = data;
        
        MemoryContextSwitchTo(oldcontext);
    }
    
    /* Per-call setup - return one row at a time */
    funcctx = SRF_PERCALL_SETUP();
    
    if (funcctx->call_cntr < funcctx->max_calls)
    {
        NodeHashData *data = (NodeHashData *) funcctx->user_fctx;
        int idx = funcctx->call_cntr;
        Datum values[6];
        bool nulls[6] = {false, false, false, false, false, false};
        HeapTuple tuple;
        
        /* Set leaf_id to NULL for non-leaf nodes */
        nulls[4] = data->leafidnulls[idx];
        
        values[0] = data->nodeids[idx];
        values[1] = data->partitions[idx];
        values[2] = data->nodeinpartitions[idx];
        values[3] = BoolGetDatum(data->isleafs[idx]);
        values[4] = data->leafids[idx];
        values[5] = data->hashes[idx];
        
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    
    SRF_RETURN_DONE(funcctx);
}

/*
 * merkle_leaf_tuples() - Show tuple-to-leaf mapping (bucketing view)
 *
 * Scans the table and groups tuples by their Merkle leaf partition.
 * Supports multi-column indexes - shows composite keys.
 *
 * Usage:
 *   SELECT * FROM merkle_leaf_tuples('tablename'::regclass);
 *   SELECT * FROM merkle_leaf_tuples('tablename'::regclass) WHERE leaf_id = 50;
 *
 * Output columns: leaf_id, tuple_count, keys
 */
Datum
merkle_leaf_tuples(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    MemoryContext    oldcontext;
    
    if (SRF_IS_FIRSTCALL())
    {
        Oid             relid = PG_GETARG_OID(0);
        Oid             indexOid;
        Relation        heapRel;
        Relation        indexRel;
        TableScanDesc   scan;
        TupleTableSlot *slot;
        Buffer          metabuf;
        Page            metapage;
        MerkleMetaPageData *meta;
        TupleDesc       tupdesc;
        TupleDesc       indexTupdesc;
        int             totalLeaves;
        int            *counts;
        StringInfo     *keyLists;  /* Store full key lists as strings */
        int             i;
        int             nkeys;
        int16          *indkey;
        Oid            *keytypes;
        FmgrInfo       *keyoutfuncs;
        
        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        
        /* Find Merkle index */
        indexOid = find_merkle_index(relid);
        if (!OidIsValid(indexOid))
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                     errmsg("no merkle index found on table %s",
                            get_rel_name(relid))));
        
        heapRel = table_open(relid, AccessShareLock);
        indexRel = index_open(indexOid, AccessShareLock);
        
        /* Read metadata */
        metabuf = ReadBuffer(indexRel, MERKLE_METAPAGE_BLKNO);
        LockBuffer(metabuf, BUFFER_LOCK_SHARE);
        metapage = BufferGetPage(metabuf);
        meta = MerklePageGetMeta(metapage);
        totalLeaves = meta->numPartitions * meta->leavesPerPartition;
        UnlockReleaseBuffer(metabuf);
        
        /* Get index key info */
        indexTupdesc = RelationGetDescr(indexRel);
        nkeys = indexRel->rd_index->indnkeyatts;
        indkey = indexRel->rd_index->indkey.values;
        
        /* Cache output functions for key display */
        keytypes = palloc(nkeys * sizeof(Oid));
        keyoutfuncs = palloc(nkeys * sizeof(FmgrInfo));
        for (i = 0; i < nkeys; i++)
        {
            Oid typoutput;
            bool typIsVarlena;
            
            keytypes[i] = TupleDescAttr(indexTupdesc, i)->atttypid;
            getTypeOutputInfo(keytypes[i], &typoutput, &typIsVarlena);
            fmgr_info(typoutput, &keyoutfuncs[i]);
        }
        
        /* Allocate arrays for counting and collecting keys */
        counts = palloc0(totalLeaves * sizeof(int));
        keyLists = palloc0(totalLeaves * sizeof(StringInfo));
        for (i = 0; i < totalLeaves; i++)
        {
            keyLists[i] = palloc(sizeof(StringInfoData));
            initStringInfo(keyLists[i]);
            appendStringInfoChar(keyLists[i], '{');
        }
        
        /* Scan heap and collect stats */
        slot = table_slot_create(heapRel, NULL);
        scan = table_beginscan(heapRel, GetActiveSnapshot(), 0, NULL);
        
        /* Reuse per-tuple scratch buffers to avoid per-row palloc/pfree churn */
        {
            Datum      *keyValues;
            bool       *keyNulls;
            StringInfoData keyStr;
            
            keyValues = palloc(nkeys * sizeof(Datum));
            keyNulls = palloc(nkeys * sizeof(bool));
            initStringInfo(&keyStr);
        
            while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
            {
                int         leafId;
                int         k;
            
                /* Get all key values */
                for (k = 0; k < nkeys; k++)
                {
                    int heapAttr = indkey[k];
                    keyValues[k] = slot_getattr(slot, heapAttr, &keyNulls[k]);
                }
            
                /* Build key string for this tuple and compute leaf mapping */
                resetStringInfo(&keyStr);
                if (nkeys == 1)
                {
                    /* Single key - just show value */
                    int64 keyval;
                    
                    if (keyNulls[0])
                    {
                        leafId = 0;
                        appendStringInfoString(&keyStr, "NULL");
                    }
                    else
                    {
                        char *str = OutputFunctionCall(&keyoutfuncs[0], keyValues[0]);
                        appendStringInfoString(&keyStr, str);
                        
                        switch (keytypes[0])
                        {
                            case INT2OID:
                                keyval = (int64) DatumGetInt16(keyValues[0]);
                                break;
                            case INT4OID:
                                keyval = (int64) DatumGetInt32(keyValues[0]);
                                break;
                            case INT8OID:
                                keyval = DatumGetInt64(keyValues[0]);
                                break;
                            default:
                                {
                                    uint32 h = 0;
                                    const unsigned char *p = (const unsigned char *) str;
                                    
                                    while (*p)
                                        h = h * 31 + *p++;
                                    
                                    leafId = (int) (h % totalLeaves);
                                    keyval = 0; /* keep compiler quiet */
                                }
                                break;
                        }
                        
                        if (keytypes[0] == INT2OID || keytypes[0] == INT4OID || keytypes[0] == INT8OID)
                        {
                            if (keyval < 0)
                                keyval = -keyval;
                            leafId = (int) (keyval % totalLeaves);
                        }
                        
                        pfree(str);
                    }
                }
                else
                {
                    /* Multi-key - show as tuple (val1,val2,...) */
                    uint64 hash = 0;
                    static const char null_marker[] = "*null*";
                    
                    appendStringInfoChar(&keyStr, '(');
                    for (k = 0; k < nkeys; k++)
                    {
                        if (k > 0)
                            appendStringInfoChar(&keyStr, ',');
                        
                        if (keyNulls[k])
                        {
                            const unsigned char *p = (const unsigned char *) null_marker;
                            
                            appendStringInfoString(&keyStr, "NULL");
                            while (*p)
                                hash = hash * 33 + *p++;
                        }
                        else
                        {
                            char *str = OutputFunctionCall(&keyoutfuncs[k], keyValues[k]);
                            const unsigned char *p = (const unsigned char *) str;
                            
                            appendStringInfoString(&keyStr, str);
                            
                            hash = hash * 33 + (unsigned char) '*';
                            while (*p)
                                hash = hash * 33 + *p++;
                            hash = hash * 33 + (unsigned char) '*';
                            
                            pfree(str);
                        }
                    }
                    appendStringInfoChar(&keyStr, ')');
                    
                    leafId = (int) (hash % totalLeaves);
                }
            
                /* Add to key list - no limit, show all keys */
                if (counts[leafId] > 0)
                    appendStringInfoString(keyLists[leafId], ", ");
                appendStringInfoString(keyLists[leafId], keyStr.data);
            
                counts[leafId]++;
            }
            
            pfree(keyStr.data);
            pfree(keyValues);
            pfree(keyNulls);
        }
        
        /* Close key lists */
        for (i = 0; i < totalLeaves; i++)
            appendStringInfoChar(keyLists[i], '}');
        
        table_endscan(scan);
        ExecDropSingleTupleTableSlot(slot);
        index_close(indexRel, AccessShareLock);
        table_close(heapRel, AccessShareLock);
        
        /* Build tuple descriptor */
        tupdesc = CreateTemplateTupleDesc(3);
        TupleDescInitEntry(tupdesc, 1, "leaf_id", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, 2, "tuple_count", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, 3, "keys", TEXTOID, -1, 0); 
        
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->max_calls = totalLeaves;
        
        /* Store data */
        {
            typedef struct {
                int *counts;
                StringInfo *keyLists;
                int totalLeaves;
            } LeafData;
            
            LeafData *data = palloc(sizeof(LeafData));
            data->counts = counts;
            data->keyLists = keyLists;
            data->totalLeaves = totalLeaves;
            funcctx->user_fctx = data;
        }
        
        MemoryContextSwitchTo(oldcontext);
    }
    
    funcctx = SRF_PERCALL_SETUP();
    
    if (funcctx->call_cntr < funcctx->max_calls)
    {
        typedef struct {
            int *counts;
            StringInfo *keyLists;
            int totalLeaves;
        } LeafData;
        
        LeafData *data = (LeafData *) funcctx->user_fctx;
        int leafId = funcctx->call_cntr;
        Datum values[3];
        bool nulls[3] = {false, false, false};
        HeapTuple tuple;
        
        values[0] = Int32GetDatum(leafId);
        values[1] = Int32GetDatum(data->counts[leafId]);
        values[2] = CStringGetTextDatum(data->keyLists[leafId]->data);
        
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    
    SRF_RETURN_DONE(funcctx);
}

/*
 * merkle_leaf_id() - Compute leaf bucket ID for key value(s)
 *
 * Auto-detects configuration from the merkle index on the table.
 * Supports any number of key columns (determined by the index).
 *
 * Usage:
 *   SELECT merkle_leaf_id('usertable', 1200);           -- 1 key
 *   SELECT merkle_leaf_id('usertable', 1200, 'text');   -- 2 keys
 *   SELECT merkle_leaf_id('usertable', a, b, c, d, e);  -- 5 keys
 */
Datum
merkle_leaf_id(PG_FUNCTION_ARGS)
{
    Oid             relid;
    Oid             indexOid;
    Relation        indexRel;
    int             leavesPerPartition;
    int             nodesPerPartition;
    int             leafId, partition, nodeInPartition;
    TupleDesc       indexTupdesc;
    int             totalLeaves;
    int             nkeys;
    int             nargs;
    Datum          *keyValues;
    bool           *keyNulls;
    int             i;
    
    /* First arg must be table OID */
    if (PG_ARGISNULL(0))
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("table name cannot be null")));
    
    relid = PG_GETARG_OID(0);
    
    /* Find Merkle index */
    indexOid = find_merkle_index(relid);
    if (!OidIsValid(indexOid))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("no merkle index found on table %s",
                        get_rel_name(relid))));
    
    indexRel = index_open(indexOid, AccessShareLock);
    indexTupdesc = RelationGetDescr(indexRel);
    nkeys = indexRel->rd_index->indnkeyatts;
    
    /* Read tree configuration from metadata */
    merkle_read_meta(indexRel, NULL, &leavesPerPartition, &nodesPerPartition, NULL,
                     &totalLeaves, NULL, NULL, NULL);
    
    /* Check number of arguments provided (fcinfo->nargs includes table) */
    nargs = PG_NARGS() - 1;  /* Subtract table arg */
    
    if (nargs != nkeys)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("merkle_leaf_id expects %d key argument(s), got %d",
                        nkeys, nargs),
                 errhint("The merkle index on %s has %d key column(s).",
                         get_rel_name(relid), nkeys)));
    
    /* Dynamically allocate arrays based on actual number of keys */
    keyValues = palloc(nkeys * sizeof(Datum));
    keyNulls = palloc(nkeys * sizeof(bool));
    
    /* 
     * Get key values and coerce types if necessary.
     */
    for (i = 0; i < nkeys; i++)
    {
        keyNulls[i] = PG_ARGISNULL(i + 1);
        if (!keyNulls[i])
        {
            Datum argValue = PG_GETARG_DATUM(i + 1);
            Oid argType = get_fn_expr_argtype(fcinfo->flinfo, i + 1);
            Oid expectedType = TupleDescAttr(indexTupdesc, i)->atttypid;
            
            if (argType == UNKNOWNOID)
            {
                Oid typInput;
                Oid typIOParam;
                getTypeInputInfo(expectedType, &typInput, &typIOParam);
                keyValues[i] = OidInputFunctionCall(typInput, 
                                                    DatumGetCString(argValue),
                                                    typIOParam, -1);
            }
            else if (argType != expectedType)
            {
                Oid castFunc;
                CoercionPathType pathtype;
                pathtype = find_coercion_pathway(expectedType, argType, 
                                                 COERCION_IMPLICIT, &castFunc);
                if (pathtype == COERCION_PATH_FUNC && OidIsValid(castFunc))
                    keyValues[i] = OidFunctionCall1(castFunc, argValue);
                else if (pathtype == COERCION_PATH_RELABELTYPE)
                    keyValues[i] = argValue;
                else
                    ereport(ERROR,
                            (errcode(ERRCODE_DATATYPE_MISMATCH),
                             errmsg("argument %d has type %s, expected %s",
                                    i + 1, format_type_be(argType),
                                    format_type_be(expectedType))));
            }
            else
                keyValues[i] = argValue;
        }
        else
            keyValues[i] = (Datum) 0;
    }
    
    /* Compute global leaf ID */
    leafId = merkle_compute_partition_id(keyValues, keyNulls,
                                                nkeys, indexTupdesc,
                                                totalLeaves);
    
    /* Calculate partition components */
    partition = leafId / leavesPerPartition;
    nodeInPartition = (leafId % leavesPerPartition) + (nodesPerPartition - leavesPerPartition + 1);

    /* Build result tuple */
    {
        TupleDesc tupdesc;
        Datum     values[3];
        bool      nulls[3] = {false, false, false};
        HeapTuple tuple;

        get_call_result_type(fcinfo, NULL, &tupdesc);
        /* NOTE: get_call_result_type already returns a blessed descriptor,
         * so we should NOT call BlessTupleDesc() again to avoid reference leaks */

        values[0] = Int32GetDatum(leafId);
        values[1] = Int32GetDatum(partition);
        values[2] = Int32GetDatum(nodeInPartition);

        tuple = heap_form_tuple(tupdesc, values, nulls);
        
        pfree(keyValues);
        pfree(keyNulls);
        index_close(indexRel, AccessShareLock);
        
        PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
    }
}
