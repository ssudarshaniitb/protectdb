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
#include "portability/instr_time.h"
#include "storage/bufmgr.h"
#include "storage/relfilenode.h"
#include "utils/relcache.h"

/* GUC: Enable/disable Merkle index updates */
extern bool enable_merkle_index;
/* GUC: Apply Merkle deltas synchronously within transaction */
extern bool merkle_apply_synchronous_direct;
/* Internal executor guard while generic UPDATE index insertion is running. */
extern bool merkle_index_maintenance_suppress;
/* GUC: Emit NOTICE lines for Merkle build roots */
extern bool merkle_update_detection;
extern bool merkle_recovery_profile_enabled;
/* GUC: reject stale reads or wait for the synchronous applier. */
extern int merkle_read_lag_policy;
#define MERKLE_APPLY_DEFAULT_BATCH_ITEMS 256
#define MERKLE_APPLY_DEFAULT_BATCH_BYTES (1024 * 1024)
#define MERKLE_APPLY_DEFAULT_BATCH_PAGES 128
#define MERKLE_APPLY_DEFAULT_BATCH_TIME_MS 1
extern int merkle_apply_batch_items;
extern int merkle_apply_batch_bytes;
extern int merkle_apply_batch_pages;
extern int merkle_apply_batch_time_ms;
#define MERKLE_READ_LAG_ERROR 0
#define MERKLE_READ_LAG_WAIT  1
/*
 * GUC: Suppress Merkle update-detection output during Merkle index builds
 * (CREATE INDEX / REINDEX).
 */
extern bool merkle_update_detection_suppress;
typedef struct MerkleRecoveryProfileStats
{
	uint64		root_hash_helper_calls;
	uint64		root_hash_nodes_returned;
	uint64		root_hash_helper_us;
	uint64		child_hash_helper_calls;
	uint64		child_hash_nodes_returned;
	uint64		child_hash_helper_us;
	uint64		row_hash_compute_calls;
	instr_time	row_hash_compute_time;
	uint64		tree_path_update_calls;
	uint64		tree_path_nodes_touched;
	instr_time	tree_path_update_time;

	/* Granular Commit Teardown Subroutine Microsecond Timings */
	uint64		commit_total_us;
	uint64		commit_pre_commit_us;
	uint64		commit_wal_flush_us;
	uint64		commit_proc_array_us;
	uint64		commit_buffers_us;
	uint64		commit_relcache_us;
	uint64		commit_inval_us;
	uint64		commit_locks_us;
	uint64		commit_guc_us;
	uint64		commit_memory_us;
	uint64		commit_remaining_us;

	/* Microsecond Breakdown of RecordTransactionCommit() */
	uint64		commit_rec_prep_us;
	uint64		commit_rec_bufmgr_us;
	uint64		commit_rec_xlog_us;
	uint64		commit_rec_ts_us;
	uint64		commit_rec_sync_us;
	uint64		commit_rec_clog_us;
	uint64		commit_rec_cleanup_us;
} MerkleRecoveryProfileStats;

extern uint64 merkle_recovery_profile_reset_generation;
extern MerkleRecoveryProfileStats merkle_recovery_profile_state;

/*
 * Hash configuration
 * Using 256-bit (32-byte) hashes from BLAKE3
 */
#define MERKLE_HASH_BITS            256
#define MERKLE_HASH_BYTES           32
#define MERKLE_BLAKE3_LEN           32  /* BLAKE3 output = 32 bytes */

/*
 * Page layout constants
 */
#define MERKLE_METAPAGE_BLKNO       0
#define MERKLE_VERSION              10  /* Partitioned dynamic Merkle index format */
#define MERKLE_ROUTE_FORMAT_VERSION 4
#define MERKLE_ROW_HASH_FORMAT_VERSION 1

/* Dynamic Merkle tree configuration constants */
#define DYNAMIC_MERKLE_FANOUT       4
#define MERKLE_DEFAULT_FANOUT       4
#define BITS_PER_SPLIT              2
#define SPLIT_THRESHOLD             32
#define MERKLE_MERGE_THRESHOLD      8
#define MAX_PREFIX_LEN              60
#define MERKLE_DEFAULT_PARTITIONS   200

typedef enum MerkleDeltaEventType
{
	MERKLE_DELTA_INSERT             = 0,  /* new_key_hash valid */
	MERKLE_DELTA_DELETE              = 1,  /* old_key_hash valid */
	MERKLE_DELTA_UPDATE_SAME_LEAF    = 2   /* old_key_hash == new_key_hash */
} MerkleDeltaEventType;

typedef struct MerkleDeltaKey
{
	Oid			index_oid;
	RelFileNode index_rnode;
	uint8		event_type;      /* MerkleDeltaEventType */
	uint8		old_key_hash[8]; /* valid if event_type != MERKLE_DELTA_INSERT */
	uint8		new_key_hash[8]; /* valid if event_type != MERKLE_DELTA_DELETE */
} MerkleDeltaKey;

/* Versioned, endian-stable durable transaction-delta encoding. */
#define MERKLE_DELTA_MAGIC          ((uint32) 0x4D444C54) /* "MDLT" */
#define MERKLE_DELTA_VERSION        1
#define MERKLE_DELTA_HEADER_BYTES   40
#define MERKLE_DELTA_ENTRY_BYTES    72

/*
 * MerkleHash - 256-bit hash value stored in 32 bytes
 */
typedef struct MerkleHash
{
    uint8       data[MERKLE_HASH_BYTES];
} MerkleHash;

typedef struct MerkleDeltaEntry
{
	MerkleDeltaKey key;
	MerkleHash	xor_delta;
} MerkleDeltaEntry;

/*
 * MerkleMetaPageData - Metadata stored on page 0
 */
typedef struct MerkleMetaPageData
{
    uint32          version;            /* format version */
    Oid             heapRelid;          /* OID of indexed table */
    int32           fanout;             /* branching factor (children per internal node) */
	int32           split_threshold;    /* max tuples before node split */
	int32           merge_threshold;    /* min tuples before node merge */
	int32           num_partitions;     /* independent top-level Merkle roots */
	uint32          routeFormatVersion; /* deterministic key-routing format */
	uint32          rowHashFormatVersion; /* canonical row serialization format */
	uint64          baselineApplySeq;   /* heap snapshot represented at build */
} MerkleMetaPageData;

#define MerklePageGetMeta(page) \
    ((MerkleMetaPageData *) PageGetContents(page))

/*
 * MerkleOptions - User-configurable options for Merkle index
 * Parsed from CREATE INDEX ... WITH (fanout=X, split_threshold=Y,
 * merge_threshold=Z, partitions=N)
 */
typedef struct MerkleOptions
{
	int32		vl_len_;		/* varlena header (required) */
	int			fanout;
	int			split_threshold;
	int			merge_threshold;
	int			num_partitions;
} MerkleOptions;

/*
 * Authoritative output of key routing.
 */
typedef struct MerkleRoute
{
	uint8		route_digest[MERKLE_HASH_BYTES];
	uint64		static_route_value;
	int			partition_id;
} MerkleRoute;

typedef enum MerkleRecoveryState
{
	MERKLE_STATE_READY = 0,
	MERKLE_STATE_CATCHING_UP,
	MERKLE_STATE_REBUILD_REQUIRED,
	MERKLE_STATE_INVALID,
	MERKLE_STATE_BLOCKED_ON_GAP
} MerkleRecoveryState;

typedef struct MerkleRecoveryStatusData
{
	MerkleRecoveryState state;
	uint64		applied_seq;
	uint64		target_seq;
	uint64		terminal_prefix_seq;
	uint64		highest_terminal_seq;
	uint64		blocked_seq;
	bool		managed;
	char		error_text[256];
} MerkleRecoveryStatusData;

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
 */
extern void merkle_read_meta(Relation indexRel, int *fanout,
							 int *split_threshold, int *merge_threshold,
							 int *num_partitions);

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

/* Core Merkle tree operations. */
extern void merkle_compute_row_hash(Relation heapRel, ItemPointer tid,
                                    MerkleHash *result);
extern void merkle_compute_slot_hash(Relation heapRel, TupleTableSlot *slot,
                                     MerkleHash *result);
extern void merkle_compute_route(Relation indexRel, Datum *values,
								 bool *isnull, int nkeys,
								 MerkleRoute *result);
extern int merkle_partition_for_routing_key(Oid index_oid,
										const uint8 *routing_key);
extern bool merkle_relation_has_index(Relation rel);
extern void merkle_reject_ddl(Relation rel, const char *command);
extern void merkle_reject_concurrent_ddl(Oid index_oid, const char *command);
extern void merkle_stage_delta_event(Relation indexRel, MerkleDeltaEventType event_type,
									 const uint8 *old_key_hash, const uint8 *new_key_hash,
									 const MerkleHash *hash);
extern bytea *merkle_serialize_staged_delta(uint64 raft_log_index,
										 uint32 item_ordinal);
extern void merkle_mark_staged_delta_persisted(void);
extern bool merkle_has_staged_delta(void);
extern void merkle_apply_staged_synchronous_safe(HTAB *combined_delta_map);
extern void merkle_apply_staged_deltas_synchronously(void);
extern void merkle_clear_split_ranges(void);
extern void merkle_crash_failpoint(const char *name);
extern void merkle_init_tree(Relation indexRel, Oid heapOid,
							 MerkleOptions *opts, uint64 baseline_apply_seq);

/* Ordered committed-delta applier and freshness gates. */
extern uint64 merkle_apply_until_internal(uint64 required_seq);
extern uint64 merkle_raft_apply_target(const uint8 *epoch_id,
									   uint64 raft_log_index,
									   uint32 item_ordinal);
extern void merkle_get_recovery_status(MerkleRecoveryStatusData *status);
extern void merkle_require_fresh(void);
extern void merkle_mark_recovery_state(MerkleRecoveryState state,
									 const char *reason);
extern uint64 merkle_advance_terminal_prefix_spi(void);
extern Datum merkle_rebuild_legacy_indexes(PG_FUNCTION_ARGS);

/*
 * XOR operations on hashes
 */
extern void merkle_hash_xor(MerkleHash *dest, const MerkleHash *src);
extern void merkle_hash_zero(MerkleHash *hash);
extern bool merkle_hash_is_zero(const MerkleHash *hash);
extern char *merkle_hash_to_hex(const MerkleHash *hash);

static inline int
merkle_bits_per_split_for_fanout(int fanout)
{
	int bits = 0;
	while ((1 << bits) < fanout && bits < 8)
		bits++;
	return bits > 0 ? bits : 2;
}

extern void merkle_require_fresh(void);
extern void do_split(Oid index_oid, int partition_id, const uint8 *node_id,
					 int prefix_len, int64 target_count);

/* Bit manipulation helpers for Dynamic Merkle tree prefix traversal */
static inline uint8
merkle_next_bits(const uint8 *key_hash, int prefix_len, int w)
{
	uint32 res = 0;
	int i;
	for (i = 0; i < w; i++)
	{
		int bit_idx = prefix_len + i;
		int byte_pos = bit_idx / 8;
		int bit_pos = 7 - (bit_idx % 8);
		uint8 bit = (key_hash[byte_pos] >> bit_pos) & 1;
		res = (res << 1) | bit;
	}
	return (uint8) res;
}

static inline void
merkle_bytea_extend(uint8 *result_node_id, const uint8 *node_id, int prefix_len, uint8 bits, int w)
{
	int i;
	memcpy(result_node_id, node_id, 8);
	for (i = 0; i < w; i++)
	{
		int bit_idx = prefix_len + i;
		int byte_pos = bit_idx / 8;
		int bit_pos = 7 - (bit_idx % 8);
		uint8 bit = (bits >> (w - 1 - i)) & 1;
		if (bit)
			result_node_id[byte_pos] |= (1 << bit_pos);
		else
			result_node_id[byte_pos] &= ~(1 << bit_pos);
	}
}

static inline void
merkle_bytea_upper_bound(uint8 *result_upper, const uint8 *node_id, int prefix_len)
{
	int full_bytes = prefix_len / 8;
	int rem = prefix_len % 8;
	int first_free;
	int i;

	memcpy(result_upper, node_id, 8);
	if (rem > 0)
	{
		uint8 mask = 0xFF >> rem;
		result_upper[full_bytes] |= mask;
		first_free = full_bytes + 1;
	}
	else
	{
		first_free = full_bytes;
	}
	for (i = first_free; i < 8; i++)
		result_upper[i] = 0xFF;
}

static inline int
merkle_parent_of(uint8 *parent_node_id, const uint8 *node_id, int prefix_len, int w)
{
	int parent_prefix_len = prefix_len - w;
	int i;

	memcpy(parent_node_id, node_id, 8);
	if (parent_prefix_len < 0)
		parent_prefix_len = 0;

	for (i = parent_prefix_len; i < 64; i++)
	{
		int byte_pos = i / 8;
		int bit_pos = 7 - (i % 8);
		parent_node_id[byte_pos] &= ~(1 << bit_pos);
	}
	return parent_prefix_len;
}

/*
 * In-memory node splitting state
 */
typedef struct MerkleTupleHashEntry
{
	uint8		key_hash[8];
	uint32		partition_id;
	MerkleHash	tuple_hash;
} MerkleTupleHashEntry;

extern void merkle_do_split_in_memory(Oid index_oid, int partition_id,
									  const uint8 *node_id, int prefix_len,
									  MerkleTupleHashEntry *entries, int num_entries,
									  int fanout, int bits_per_split, int split_threshold);

/*
 * SQL-callable verification functions
 */
extern Datum merkle_verify(PG_FUNCTION_ARGS);
extern Datum merkle_verify_index(PG_FUNCTION_ARGS);
extern Datum merkle_root_hash(PG_FUNCTION_ARGS);
extern Datum merkle_root_hash_index(PG_FUNCTION_ARGS);
extern Datum merkle_tree_stats(PG_FUNCTION_ARGS);
extern Datum merkle_node_hash(PG_FUNCTION_ARGS);
extern Datum merkle_leaf_tuples(PG_FUNCTION_ARGS);
extern Datum merkle_leaf_id(PG_FUNCTION_ARGS);
extern Datum merkle_bucket_for_key(PG_FUNCTION_ARGS);
extern Datum merkle_get_node_hash(PG_FUNCTION_ARGS);
extern Datum merkle_get_child_hashes(PG_FUNCTION_ARGS);
extern Datum merkle_get_node_hashes(PG_FUNCTION_ARGS);
extern Datum merkle_get_children_batch(PG_FUNCTION_ARGS);
extern Datum merkle_get_descendants_batch(PG_FUNCTION_ARGS);
extern Datum merkle_get_descendants_batch_array(PG_FUNCTION_ARGS);
extern Datum merkle_get_leaf_members(PG_FUNCTION_ARGS);
extern Datum merkle_get_partition_root_hash(PG_FUNCTION_ARGS);
extern Datum merkle_get_partition_root_hashes(PG_FUNCTION_ARGS);
extern Datum merkle_recovery_profile_reset(PG_FUNCTION_ARGS);
extern Datum merkle_recovery_profile_stats(PG_FUNCTION_ARGS);
extern Datum merkle_recovery_status(PG_FUNCTION_ARGS);
extern void merkle_hash_slot_canonical_desc(TupleDesc tupdesc, TupleTableSlot *slot,
											 MerkleHash *result);
extern Datum merkle_key_hash_sql(PG_FUNCTION_ARGS);
extern Datum merkle_node_upper_bound_sql(PG_FUNCTION_ARGS);
extern Datum merkle_tuple_hash_sql(PG_FUNCTION_ARGS);
extern Datum merkle_partition_for_hash(PG_FUNCTION_ARGS);
extern Datum merkle_find_spurious_key_sql(PG_FUNCTION_ARGS);
extern Datum merkle_apply_until_sql(PG_FUNCTION_ARGS);

#endif /* MERKLE_H */
