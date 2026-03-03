/*-------------------------------------------------------------------------
 *
 * merkle.h
 *    Merkle tree integrity index access method definitions
 *
 * This implements a Merkle tree as a PostgreSQL index type for data
 * integrity verification. On every INSERT/UPDATE/DELETE, the Merkle
 * tree is automatically updated by PostgreSQL's index infrastructure.
 *
 * src/include/access/merkle.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MERKLE_H
#define MERKLE_H

#include "access/amapi.h"
#include "access/itup.h"
#include "access/sdir.h"
#include "access/tableam.h"
#include "nodes/execnodes.h"
#include "storage/bufmgr.h"
#include "utils/relcache.h"

/* GUC: Enable/disable Merkle index updates */
extern bool enable_merkle_index;
/* GUC: Emit NOTICE lines for touched Merkle nodes on commit */
extern bool merkle_update_detection;
/*
 * GUC: Suppress Merkle update-detection output during Merkle index builds
 * (CREATE INDEX / REINDEX).
 */
extern bool merkle_update_detection_suppress;
/*
 * Internal: temporarily suppress per-tuple undo tracking in non-DML contexts
 * where the Merkle index relation is being built (CREATE INDEX / REINDEX).
 *
 * During an index build, the Merkle index is new and will be dropped on error
 * or transaction abort, so recording per-row undo state is unnecessary and
 * can exhaust memory for large tables.
 */
extern bool merkle_undo_suppress;

/*
 * Merkle tree configuration constants
 * 
 * The tree is organized as multiple partitions for better distribution:
 * - NUM_PARTITIONS: Number of independent partitions
 * - LEAVES_PER_PARTITION: Leaf buckets in each partition
 * - DEFAULT_FANOUT: Branching factor (children per internal node)
 * - NODES_PER_PARTITION (default): Total nodes per partition for a perfect
 *   k-ary tree with k=DEFAULT_FANOUT (2 * leaves - 1 = 7)
 * - TOTAL_LEAVES (default): NUM_PARTITIONS * LEAVES_PER_PARTITION
 * - TOTAL_NODES (default): NUM_PARTITIONS * NODES_PER_PARTITION
 *
 * NOTE: With 256-bit (32-byte) hashes, the default tree spans multiple pages.
 */
#define MERKLE_NUM_PARTITIONS       58
#define MERKLE_LEAVES_PER_PARTITION 4
#define MERKLE_DEFAULT_FANOUT       2   /* branching factor */
#define MERKLE_NODES_PER_PARTITION  7   /* 2 * LEAVES_PER_PARTITION - 1 */
#define MERKLE_TOTAL_LEAVES         232 /* NUM_PARTITIONS * LEAVES_PER_PARTITION */
#define MERKLE_TOTAL_NODES          406 /* NUM_PARTITIONS * NODES_PER_PARTITION */

/*
 * Hash configuration
 * Using 256-bit (32-byte) hashes from BLAKE3
 * 
 * NOTE: Upgraded to 256-bit for maximum security and performance.
 * Each node now takes 36 bytes (4 nodeId + 32 hash).
 * 
 * Security: Collision threshold is now 2^128 (astronomical)
 * Performance: BLAKE3 is faster than MD5 and cryptographically secure
 */
#define MERKLE_HASH_BITS            256
#define MERKLE_HASH_BYTES           32
#define MERKLE_BLAKE3_LEN           32  /* BLAKE3 output = 32 bytes */

/*
 * Page layout constants
 */
#define MERKLE_METAPAGE_BLKNO       0
#define MERKLE_TREE_START_BLKNO     1
#define MERKLE_VERSION              5   /* Bump version for fanout in metadata */

/*
 * Calculate how many nodes fit per page
 * Each node: 4 bytes (nodeId) + 32 bytes (hash) = 36 bytes
 * Page size 8192, minus header ~24 bytes = ~8168 usable
 * Max ~226 nodes per page.
 */
#define MERKLE_MAX_NODES_PER_PAGE   ((BLCKSZ - MAXALIGN(SizeOfPageHeaderData)) / sizeof(MerkleNode))

/*
 * MerkleHash - 256-bit hash value stored in 32 bytes
 */
typedef struct MerkleHash
{
    uint8       data[MERKLE_HASH_BYTES];
} MerkleHash;

/*
 * MerkleNode - A single node in the Merkle tree
 */
typedef struct MerkleNode
{
    int32       nodeId;     /* node identifier (1-indexed) */
    MerkleHash  hash;       /* XOR-aggregated hash value */
} MerkleNode;

/*
 * MerkleMetaPageData - Metadata stored on page 0
 */
typedef struct MerkleMetaPageData
{
    uint32          version;            /* format version */
    Oid             heapRelid;          /* OID of indexed table */
    int32           numPartitions;      /* number of partitions */
    int32           leavesPerPartition; /* leaves per partition */
    int32           nodesPerPartition;  /* nodes per partition */
    int32           totalNodes;         /* total nodes in tree */
    int32           nodesPerPage;       /* how many nodes fit per page */
    int32           numTreePages;       /* number of pages for tree nodes */
    int32           fanout;             /* branching factor (children per internal node) */
} MerkleMetaPageData;

#define MerklePageGetMeta(page) \
    ((MerkleMetaPageData *) PageGetContents(page))

/*
 * MerkleOptions - User-configurable options for Merkle index
 * Parsed from CREATE INDEX ... WITH (partitions=X, leaves_per_partition=Y, fanout=Z)
 */
typedef struct MerkleOptions
{
    int32       vl_len_;        /* varlena header (required) */
    int         partitions;
    int         leaves_per_partition;
    int         fanout;
} MerkleOptions;

/*
 * Handler function - returns IndexAmRoutine
 */
extern Datum merklehandler(PG_FUNCTION_ARGS);

/*
 * Reloptions parsing
 */
extern bytea *merkle_options(Datum reloptions, bool validate);
extern MerkleOptions *merkle_get_options(Relation indexRel);

/*
 * Helper to read tree config from metadata
 * (nodesPerPage and numTreePages can be NULL if not needed)
 */
extern void merkle_read_meta(Relation indexRel, int *numPartitions,
                             int *leavesPerPartition, int *nodesPerPartition,
                             int *totalNodes, int *totalLeaves,
                             int *nodesPerPage, int *numTreePages,
                             int *fanout);

/*
 * Index build functions
 */
extern IndexBuildResult *merkleBuild(Relation heapRel, Relation indexRel,
                                     struct IndexInfo *indexInfo);
extern void merkleBuildempty(Relation indexRel);

/*
 * Index modification functions
 */
extern bool merkleInsert(Relation indexRel, Datum *values, bool *isnull,
                         ItemPointer ht_ctid, Relation heapRel,
                         IndexUniqueCheck checkUnique,
                         struct IndexInfo *indexInfo);

extern IndexBulkDeleteResult *merkleBulkdelete(IndexVacuumInfo *info,
                                               IndexBulkDeleteResult *stats,
                                               IndexBulkDeleteCallback callback,
                                               void *callback_state);

extern IndexBulkDeleteResult *merkleVacuumcleanup(IndexVacuumInfo *info,
                                                  IndexBulkDeleteResult *stats);

/*
 * Cost estimation
 */
extern void merkleCostEstimate(struct PlannerInfo *root,
                               struct IndexPath *path,
                               double loop_count,
                               Cost *indexStartupCost,
                               Cost *indexTotalCost,
                               Selectivity *indexSelectivity,
                               double *indexCorrelation,
                               double *indexPages);

/*
 * Core Merkle tree operations
 * (merkle_compute_partition_id handles both single and multi-column keys)
 */
extern void merkle_compute_row_hash(Relation heapRel, ItemPointer tid,
                                    MerkleHash *result);
extern void merkle_compute_slot_hash(Relation heapRel, TupleTableSlot *slot,
                                     MerkleHash *result);
extern int  merkle_compute_partition_id(Datum *values, bool *isnull,
                                        int nkeys, TupleDesc tupdesc,
                                        int numLeaves);
extern void merkle_update_tree_path(Relation indexRel, int leafId,
                                    MerkleHash *hash, bool isXorIn);
extern void merkle_init_tree(Relation indexRel, Oid heapOid,
                             MerkleOptions *opts);

/*
 * XOR operations on hashes
 */
extern void merkle_hash_xor(MerkleHash *dest, const MerkleHash *src);
extern void merkle_hash_zero(MerkleHash *hash);
extern bool merkle_hash_is_zero(const MerkleHash *hash);
extern char *merkle_hash_to_hex(const MerkleHash *hash);

/*
 * SQL-callable verification functions
 */
extern Datum merkle_verify(PG_FUNCTION_ARGS);
extern Datum merkle_root_hash(PG_FUNCTION_ARGS);
extern Datum merkle_tree_stats(PG_FUNCTION_ARGS);
extern Datum merkle_node_hash(PG_FUNCTION_ARGS);
extern Datum merkle_leaf_tuples(PG_FUNCTION_ARGS);
extern Datum merkle_leaf_id(PG_FUNCTION_ARGS);

#endif /* MERKLE_H */
