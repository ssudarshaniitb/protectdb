/*-------------------------------------------------------------------------
 *
 * merkleutil.c
 *    Utility functions for Merkle tree operations
 *
 * This file contains helper functions for hash computation, XOR operations,
 * tree traversal, and page access.
 *
 * IDENTIFICATION
 *    src/backend/access/merkle/merkleutil.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/merkle.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/tableam.h"
#include "catalog/pg_type.h"
#include "common/blake3.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/snapshot.h"
#include "utils/typcache.h"
#include "parser/parse_coerce.h"
#include "funcapi.h"
#include "access/xact.h"
#include "access/xloginsert.h"
#include "portability/instr_time.h"



/*
 * Per-backend, per-transaction cache of merkle metapage values.
 *
 * merkle_read_meta() pins + share-locks the metapage (block 0) on every call.
 * During apply_optim_writes(), we can call merkle_update_tree_path() dozens
 * of times on the SAME index within one tx — each call doing the metapage
 * round-trip. Once the tree geometry (partitions, fanout, etc.) is read,
 * it's stable for the lifetime of the relation (geometry changes require
 * AccessExclusiveLock, so no concurrent DML can change it).
 *
 * Cache is cleared on XactCallback so a new txn always re-reads at least
 * once (protects against REINDEX-induced geometry changes between txns).
 */
#define MERKLE_META_CACHE_SLOTS 4
typedef struct MerkleMetaCacheEntry {
    Oid  relid;              /* InvalidOid => empty slot */
    int  fanout;
    int  split_threshold;
    int  merge_threshold;
	int  num_partitions;
} MerkleMetaCacheEntry;
static MerkleMetaCacheEntry merkle_meta_cache[MERKLE_META_CACHE_SLOTS];
static bool merkle_meta_cache_registered = false;

static void
merkle_meta_cache_clear(void)
{
    int i;
    for (i = 0; i < MERKLE_META_CACHE_SLOTS; ++i)
        merkle_meta_cache[i].relid = InvalidOid;
}

static void
merkle_meta_cache_xact_callback(XactEvent event, void *arg)
{
    (void) arg;
    if (event == XACT_EVENT_COMMIT || event == XACT_EVENT_ABORT ||
        event == XACT_EVENT_PARALLEL_COMMIT || event == XACT_EVENT_PARALLEL_ABORT ||
        event == XACT_EVENT_PREPARE)
    {
        merkle_meta_cache_clear();
    }
}

static const MerkleMetaCacheEntry *
merkle_meta_cache_lookup(Oid relid)
{
    int i;
    for (i = 0; i < MERKLE_META_CACHE_SLOTS; ++i)
        if (merkle_meta_cache[i].relid == relid)
            return &merkle_meta_cache[i];
    return NULL;
}

static void
merkle_meta_cache_store(Oid relid, int fanout, int split_threshold,
							int merge_threshold, int num_partitions)
{
    int i;
    /* LRU-ish: replace first empty slot, else replace slot 0. */
    int target = 0;
    for (i = 0; i < MERKLE_META_CACHE_SLOTS; ++i)
    {
        if (merkle_meta_cache[i].relid == InvalidOid) { target = i; break; }
        if (merkle_meta_cache[i].relid == relid)      { target = i; break; }
    }
    if (!merkle_meta_cache_registered)
    {
        RegisterXactCallback(merkle_meta_cache_xact_callback, NULL);
        merkle_meta_cache_registered = true;
    }
    merkle_meta_cache[target].relid           = relid;
    merkle_meta_cache[target].fanout          = fanout;
    merkle_meta_cache[target].split_threshold = split_threshold;
    merkle_meta_cache[target].merge_threshold = merge_threshold;
	merkle_meta_cache[target].num_partitions = num_partitions;
}

/*
 * merkle_hash_xor() - XOR two hashes together
 *
 * dest = dest XOR src
 * This is the core operation for Merkle tree updates.
 */
void
merkle_hash_xor(MerkleHash *dest, const MerkleHash *src)
{
    int i;
    
    for (i = 0; i < MERKLE_HASH_BYTES; i++)
        dest->data[i] ^= src->data[i];
}

/*
 * merkle_hash_zero() - Set hash to all zeros
 */
void
merkle_hash_zero(MerkleHash *hash)
{
    memset(hash->data, 0, MERKLE_HASH_BYTES);
}

/*
 * merkle_hash_is_zero() - Check if hash is all zeros
 */
bool
merkle_hash_is_zero(const MerkleHash *hash)
{
    int i;
    
    for (i = 0; i < MERKLE_HASH_BYTES; i++)
    {
        if (hash->data[i] != 0)
            return false;
    }
    return true;
}

/*
 * merkle_hash_to_hex() - Convert hash to hex string for display
 *
 * Returns a palloc'd string.
 */
char *
merkle_hash_to_hex(const MerkleHash *hash)
{
    char *result = palloc(MERKLE_HASH_BYTES * 2 + 1);
    int i;
    
    for (i = 0; i < MERKLE_HASH_BYTES; i++)
        sprintf(result + (i * 2), "%02x", hash->data[i]);
    
    result[MERKLE_HASH_BYTES * 2] = '\0';
    return result;
}

static void
merkle_hash_uint32(blake3_hasher *hasher, uint32 value)
{
	uint8		bytes[4];

	bytes[0] = (uint8) (value >> 24);
	bytes[1] = (uint8) (value >> 16);
	bytes[2] = (uint8) (value >> 8);
	bytes[3] = (uint8) value;
	blake3_hasher_update(hasher, bytes, sizeof(bytes));
}

/*
 * Hash one materialized row using a versioned, length-prefixed binary format.
 * Type send functions produce PostgreSQL's canonical wire representation and
 * therefore do not depend on TimeZone, DateStyle, locale, or output GUCs.
 */
void
merkle_hash_slot_canonical_desc_fast(TupleDesc tupdesc, TupleTableSlot *slot,
									 FmgrInfo *send_functions, MerkleHash *result)
{
	blake3_hasher	hasher;
	int				i;
	uint32			live_attributes = 0;
	static const uint8 magic[] = {'A', 'R', 'I', 'A', 'M', 'R', 'K', 'L'};

	if (slot == NULL || TTS_EMPTY(slot))
	{
		merkle_hash_zero(result);
		return;
	}

	for (i = 0; i < tupdesc->natts; i++)
		if (!TupleDescAttr(tupdesc, i)->attisdropped)
			live_attributes++;

	blake3_hasher_init(&hasher);
	blake3_hasher_update(&hasher, magic, sizeof(magic));
	merkle_hash_uint32(&hasher, MERKLE_ROW_HASH_FORMAT_VERSION);
	merkle_hash_uint32(&hasher, live_attributes);

	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		Datum		val;
		bool		isnull;
		uint8		null_flag;

		if (attr->attisdropped)
			continue;

		/* Schema descriptor: physical attribute, type identity, and typmod. */
		merkle_hash_uint32(&hasher, (uint32) attr->attnum);
		merkle_hash_uint32(&hasher, (uint32) attr->atttypid);
		merkle_hash_uint32(&hasher, (uint32) attr->atttypmod);

		val = slot_getattr(slot, i + 1, &isnull);
		null_flag = isnull ? 1 : 0;
		blake3_hasher_update(&hasher, &null_flag, sizeof(null_flag));

		if (!isnull)
		{
			bytea	   *encoded;
			uint32		length;

			if (send_functions != NULL && OidIsValid(send_functions[i].fn_oid))
			{
				encoded = DatumGetByteaP(FunctionCall1(&send_functions[i], val));
			}
			else
			{
				Oid			typsend;
				bool		typisvarlena;
				getTypeBinaryOutputInfo(attr->atttypid, &typsend, &typisvarlena);
				encoded = OidSendFunctionCall(typsend, val);
			}
			length = (uint32) VARSIZE_ANY_EXHDR(encoded);
			merkle_hash_uint32(&hasher, length);
			if (length > 0)
				blake3_hasher_update(&hasher, VARDATA_ANY(encoded), length);
			pfree(encoded);
		}
	}

	blake3_hasher_finalize(&hasher, result->data, MERKLE_HASH_BYTES);
}

void
merkle_hash_slot_canonical_desc(TupleDesc tupdesc, TupleTableSlot *slot,
								MerkleHash *result)
{
	merkle_hash_slot_canonical_desc_fast(tupdesc, slot, NULL, result);
}

static void
merkle_hash_slot_canonical(Relation heapRel, TupleTableSlot *slot,
						   MerkleHash *result)
{
	TupleDesc tupdesc = RelationGetDescr(heapRel);
	merkle_hash_slot_canonical_desc(tupdesc, slot, result);
}

/*
 * merkle_compute_row_hash() - Fetch and canonically hash one heap row.
 */
void
merkle_compute_row_hash(Relation heapRel, ItemPointer tid, MerkleHash *result)
{
	TupleDesc		tupdesc;
	TupleTableSlot *slot = NULL;
	bool			profile_enabled = merkle_recovery_profile_enabled;
	instr_time		start_time;
	instr_time		elapsed_time;

	if (profile_enabled)
		INSTR_TIME_SET_CURRENT(start_time);
	if (profile_enabled)
		merkle_recovery_profile_state.row_hash_compute_calls++;

	/*
	 * CRITICAL FIX: Validate ItemPointer before attempting to fetch tuple.
	 * Invalid TIDs (offset=0 or block=Invalid) can occur during BCDB operations
	 * and will cause fetch failures. Return zero hash for these cases.
	 */
	if (!ItemPointerIsValid(tid) ||
		ItemPointerGetBlockNumberNoCheck(tid) == InvalidBlockNumber)
	{
		elog(DEBUG1, "merkle_compute_row_hash: skipping invalid tid (blk=%u, off=%u)",
			 ItemPointerGetBlockNumberNoCheck(tid),
			 ItemPointerGetOffsetNumberNoCheck(tid));
		merkle_hash_zero(result);
		goto profile_done;
	}

	tupdesc = RelationGetDescr(heapRel);
	slot = MakeSingleTupleTableSlot(tupdesc, &TTSOpsBufferHeapTuple);

	PG_TRY();
	{
		/*
		 * Use SnapshotSelf to see our own uncommitted changes.
		 * During INSERT, the tuple is in the heap but not yet committed,
		 * so GetActiveSnapshot() won't see it.
		 */
		bool fetch_ok = table_tuple_fetch_row_version(heapRel, tid, SnapshotSelf, slot);
		if (!fetch_ok)
		{
			merkle_hash_zero(result);
		}
		else
			merkle_hash_slot_canonical(heapRel, slot, result);
	}
	PG_CATCH();
	{
		if (slot != NULL)
			ExecDropSingleTupleTableSlot(slot);
		if (profile_enabled)
		{
			INSTR_TIME_SET_CURRENT(elapsed_time);
			INSTR_TIME_SUBTRACT(elapsed_time, start_time);
			INSTR_TIME_ADD(merkle_recovery_profile_state.row_hash_compute_time,
						   elapsed_time);
		}
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (slot != NULL)
		ExecDropSingleTupleTableSlot(slot);

profile_done:
	if (profile_enabled)
	{
		INSTR_TIME_SET_CURRENT(elapsed_time);
		INSTR_TIME_SUBTRACT(elapsed_time, start_time);
		INSTR_TIME_ADD(merkle_recovery_profile_state.row_hash_compute_time,
					   elapsed_time);
	}
}

/*
 * merkle_compute_slot_hash() - Compute integrity hash from an already-fetched slot.
 *
 * This variant avoids heap re-fetch by hashing the visible values currently
 * present in `slot`. It is used when we must defer Merkle mutation until after
 * heap operation success, while still hashing the OLD row image.
 */
void
merkle_compute_slot_hash_fast(Relation heapRel, TupleTableSlot *slot,
						   FmgrInfo *send_functions, MerkleHash *result)
{
	bool			profile_enabled = merkle_recovery_profile_enabled;
	instr_time		start_time;
	instr_time		elapsed_time;

	if (profile_enabled)
	{
		INSTR_TIME_SET_CURRENT(start_time);
		merkle_recovery_profile_state.row_hash_compute_calls++;
	}

	merkle_hash_slot_canonical_desc_fast(RelationGetDescr(heapRel), slot, send_functions, result);

	if (profile_enabled)
	{
		INSTR_TIME_SET_CURRENT(elapsed_time);
		INSTR_TIME_SUBTRACT(elapsed_time, start_time);
		INSTR_TIME_ADD(merkle_recovery_profile_state.row_hash_compute_time,
					   elapsed_time);
	}
}

void
merkle_compute_slot_hash(Relation heapRel, TupleTableSlot *slot, MerkleHash *result)
{
	merkle_compute_slot_hash_fast(heapRel, slot, NULL, result);
}

/*
 * merkle_compute_canonical_route_digest() - Uniform BLAKE3-256 routing digest.
 *
 * All key types (integers, text, composite, NULL) are routed through a
 * versioned, length-prefixed binary-send stream hashed with BLAKE3.  This
 * produces a uniform 256-bit digest from which:
 *
 *   static leaf  = uint64(first 8 digest bytes) % total_leaves
 *   dynamic bits = full 256-bit digest consumed in fixed groups
 *
 * INTEGER KEYS: earlier versions used abs(key) % total_leaves directly, which
 * was incompatible with a future dynamic prefix tree (sequential keys share
 * high bits).  Route format version 2 removes that special path.
 */
void
merkle_compute_canonical_route_digest_fast(Datum *values, bool *isnull, int nkeys,
											TupleDesc tupdesc, FmgrInfo *send_functions,
											uint8 digest[MERKLE_HASH_BYTES])
{
	blake3_hasher hasher;
	int			i;
	static const uint8 magic[] = {'A', 'R', 'I', 'A', 'R', 'O', 'U', 'T'};

	blake3_hasher_init(&hasher);
	blake3_hasher_update(&hasher, magic, sizeof(magic));
	merkle_hash_uint32(&hasher, MERKLE_ROUTE_FORMAT_VERSION);
	merkle_hash_uint32(&hasher, (uint32) nkeys);

	for (i = 0; i < nkeys; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		uint8		null_flag = isnull[i] ? 1 : 0;

		merkle_hash_uint32(&hasher, (uint32) (i + 1));
		merkle_hash_uint32(&hasher, (uint32) attr->atttypid);
		merkle_hash_uint32(&hasher, (uint32) attr->atttypmod);
		blake3_hasher_update(&hasher, &null_flag, sizeof(null_flag));

		if (!isnull[i])
		{
			bytea	   *encoded;
			uint32		length;

			if (send_functions != NULL && OidIsValid(send_functions[i].fn_oid))
			{
				encoded = DatumGetByteaP(FunctionCall1(&send_functions[i], values[i]));
			}
			else
			{
				Oid			typsend;
				bool		typisvarlena;
				getTypeBinaryOutputInfo(attr->atttypid, &typsend, &typisvarlena);
				encoded = OidSendFunctionCall(typsend, values[i]);
			}
			length = (uint32) VARSIZE_ANY_EXHDR(encoded);
			merkle_hash_uint32(&hasher, length);
			if (length > 0)
			{
				blake3_hasher_update(&hasher, VARDATA_ANY(encoded), length);
			}
			pfree(encoded);
		}
	}

	blake3_hasher_finalize(&hasher, digest, MERKLE_HASH_BYTES);
}

static void
merkle_compute_canonical_route_digest(Datum *values, bool *isnull, int nkeys,
									  TupleDesc tupdesc, uint8 digest[MERKLE_HASH_BYTES])
{
	merkle_compute_canonical_route_digest_fast(values, isnull, nkeys, tupdesc, NULL, digest);
}

/*
 * merkle_compute_route() - Single relation-aware routing entry point.
 *
 * All key types use uniform BLAKE3-256 routing (route format version 4).
 */
void
merkle_compute_route(Relation indexRel, Datum *values, bool *isnull, int nkeys,
						 MerkleRoute *result)
{
	TupleDesc		tupdesc;
	uint64			static_route_value = 0;
	int				i;

	if (result == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("merkle route output cannot be null")));

	tupdesc = RelationGetDescr(indexRel);

	/* Uniform BLAKE3 routing for all key types including integers. */
	merkle_compute_canonical_route_digest(values, isnull, nkeys, tupdesc, result->route_digest);

	for (i = 0; i < 8; i++)
		static_route_value = (static_route_value << 8) | result->route_digest[i];

	result->static_route_value = static_route_value;
	{
		int num_partitions = MERKLE_DEFAULT_PARTITIONS;
		merkle_read_meta(indexRel, NULL, NULL, NULL, &num_partitions);
		result->partition_id = (int) (static_route_value % (uint64) num_partitions);
	}
}

int
merkle_partition_for_routing_key(Oid index_oid, const uint8 *routing_key)
{
	Relation index_rel;
	int num_partitions = MERKLE_DEFAULT_PARTITIONS;
	uint64 route_value = 0;
	int i;

	if (routing_key == NULL)
		elog(ERROR, "NULL Merkle routing key");
	for (i = 0; i < 8; i++)
		route_value = (route_value << 8) | routing_key[i];

	index_rel = index_open(index_oid, AccessShareLock);
	merkle_read_meta(index_rel, NULL, NULL, NULL, &num_partitions);
	index_close(index_rel, AccessShareLock);
	return (int) (route_value % (uint64) num_partitions);
}

/*
 * merkle_read_meta() - Read tree configuration from index metadata
 */
void
merkle_read_meta(Relation indexRel, int *fanout,
				 int *split_threshold, int *merge_threshold,
				 int *num_partitions)
{
	Buffer              buf;
	Page                page;
	MerkleMetaPageData *meta;
	Oid                 cache_relid = RelationGetRelid(indexRel);
	const MerkleMetaCacheEntry *cached;

	/*
	 * Tree geometry is immutable while a DML transaction can hold the index
	 * lock.  The cache is transaction-local and is invalidated at xact end, so
	 * reusing it avoids a metapage pin/lock for every staged delta and every
	 * ancestor propagation step without allowing a REINDEX geometry change to
	 * leak into a later transaction.
	 */
	cached = merkle_meta_cache_lookup(cache_relid);
	if (cached != NULL)
	{
		if (fanout)          *fanout          = cached->fanout;
		if (split_threshold) *split_threshold = cached->split_threshold;
		if (merge_threshold) *merge_threshold = cached->merge_threshold;
		if (num_partitions) *num_partitions = cached->num_partitions;
		return;
	}

	buf = ReadBuffer(indexRel, MERKLE_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	meta = MerklePageGetMeta(page);

	if (meta->version != MERKLE_VERSION ||
		meta->routeFormatVersion != MERKLE_ROUTE_FORMAT_VERSION ||
		meta->rowHashFormatVersion != MERKLE_ROW_HASH_FORMAT_VERSION)
	{
		uint32 stored_version = meta->version;
		uint32 stored_route = meta->routeFormatVersion;
		uint32 stored_row_hash = meta->rowHashFormatVersion;

		UnlockReleaseBuffer(buf);
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("Merkle index \"%s\" uses an incompatible format",
						RelationGetRelationName(indexRel)),
				 errdetail("index version=%u route format=%u row-hash format=%u; server requires version=%u route format=%u row-hash format=%u",
						   stored_version, stored_route, stored_row_hash,
						   MERKLE_VERSION, MERKLE_ROUTE_FORMAT_VERSION,
						   MERKLE_ROW_HASH_FORMAT_VERSION),
				 errhint("REINDEX the Merkle index before using it.")));
	}

	if (fanout)          *fanout          = meta->fanout;
	if (split_threshold) *split_threshold = meta->split_threshold;
	if (merge_threshold) *merge_threshold = meta->merge_threshold;
	if (num_partitions) *num_partitions = meta->num_partitions;

	merkle_meta_cache_store(cache_relid, meta->fanout, meta->split_threshold,
							meta->merge_threshold, meta->num_partitions);

	UnlockReleaseBuffer(buf);
}

/*
 * merkle_init_tree() - Initialize Merkle tree metadata page
 */
void
merkle_init_tree(Relation indexRel, Oid heapOid, MerkleOptions *opts,
				 uint64 baseline_apply_seq)
{
	Buffer          metabuf;
	Page            metapage;
	MerkleMetaPageData *meta;
	int             fanout;
	int             split_threshold;
	int             merge_threshold;
	int             num_partitions;

	if (opts != NULL)
	{
		fanout = opts->fanout;
		split_threshold = opts->split_threshold;
		merge_threshold = opts->merge_threshold;
		num_partitions = opts->num_partitions;
	}
	else
	{
		fanout = MERKLE_DEFAULT_FANOUT;
		split_threshold = SPLIT_THRESHOLD;
		merge_threshold = MERKLE_MERGE_THRESHOLD;
		num_partitions = MERKLE_DEFAULT_PARTITIONS;
	}

	metabuf = ReadBuffer(indexRel, P_NEW);
	Assert(BufferGetBlockNumber(metabuf) == MERKLE_METAPAGE_BLKNO);
	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
	metapage = BufferGetPage(metabuf);
	PageInit(metapage, BLCKSZ, 0);

	meta = MerklePageGetMeta(metapage);
	meta->version = MERKLE_VERSION;
	meta->heapRelid = heapOid;
	meta->fanout = fanout;
	meta->split_threshold = split_threshold;
	meta->merge_threshold = merge_threshold;
	meta->num_partitions = num_partitions;
	meta->routeFormatVersion = MERKLE_ROUTE_FORMAT_VERSION;
	meta->rowHashFormatVersion = MERKLE_ROW_HASH_FORMAT_VERSION;
	meta->baselineApplySeq = baseline_apply_seq;

	MarkBufferDirty(metabuf);
	if (RelationNeedsWAL(indexRel))
		log_newpage_buffer(metabuf, true);
	UnlockReleaseBuffer(metabuf);
}

typedef struct MerkleKeyHashCache
{
	Oid			argtype;
	TupleDesc	tupdesc;
	FmgrInfo	send_fn;
} MerkleKeyHashCache;

/*
 * merkle_node_upper_bound_sql - SQL-callable wrapper around merkle_bytea_upper_bound.
 *
 * Computes the inclusive upper-bound bytea for a Merkle prefix range, exactly
 * as do_split() and do_merge_check() do internally.  This eliminates the need
 * for a hand-rolled plpgsql duplicate in test/utility scripts.
 *
 * Usage:  merkle_node_upper_bound(node_id bytea, prefix_len int) -> bytea
 */
PG_FUNCTION_INFO_V1(merkle_node_upper_bound_sql);

Datum
merkle_node_upper_bound_sql(PG_FUNCTION_ARGS)
{
	bytea	   *node_id_arg = PG_GETARG_BYTEA_PP(0);
	int			prefix_len  = PG_GETARG_INT32(1);
	uint8		node_id[8];
	uint8		upper[8];
	bytea	   *result;

	if (VARSIZE_ANY_EXHDR(node_id_arg) != 8)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("merkle_node_upper_bound: node_id must be exactly 8 bytes")));

	if (prefix_len < 0 || prefix_len > 64)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("merkle_node_upper_bound: prefix_len must be in [0, 64]")));

	memcpy(node_id, VARDATA_ANY(node_id_arg), 8);
	merkle_bytea_upper_bound(upper, node_id, prefix_len);

	result = (bytea *) palloc(VARHDRSZ + 8);
	SET_VARSIZE(result, VARHDRSZ + 8);
	memcpy(VARDATA(result), upper, 8);

	PG_RETURN_BYTEA_P(result);
}

PG_FUNCTION_INFO_V1(merkle_key_hash_sql);
PG_FUNCTION_INFO_V1(merkle_partition_for_hash);
PG_FUNCTION_INFO_V1(merkle_find_spurious_key_sql);

Datum
merkle_partition_for_hash(PG_FUNCTION_ARGS)
{
	bytea *hash = PG_GETARG_BYTEA_PP(0);
	int32 num_partitions = PG_GETARG_INT32(1);
	uint64 route_value = 0;
	int i;

	if (VARSIZE_ANY_EXHDR(hash) != 8 || num_partitions < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("merkle_partition_for_hash requires an 8-byte hash and positive partition count")));
	for (i = 0; i < 8; i++)
		route_value = (route_value << 8) | ((const uint8 *) VARDATA_ANY(hash))[i];

	PG_RETURN_INT32((int32) (route_value % (uint64) num_partitions));
}

Datum
merkle_find_spurious_key_sql(PG_FUNCTION_ARGS)
{
	bytea	   *lower_arg = PG_GETARG_BYTEA_PP(0);
	bytea	   *upper_arg = PG_GETARG_BYTEA_PP(1);
	int32		partition_id = PG_GETARG_INT32(2);
	int32		partitions = PG_GETARG_INT32(3);
	int64		base_offset = PG_GETARG_INT64(4);
	int32		max_attempts = PG_GETARG_INT32(5);

	uint8		lower[8];
	uint8		upper[8];
	blake3_hasher base_hasher;
	static const uint8 magic[] = {'A', 'R', 'I', 'A', 'R', 'O', 'U', 'T'};
	int32		i;

	if (VARSIZE_ANY_EXHDR(lower_arg) != 8 || VARSIZE_ANY_EXHDR(upper_arg) != 8)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("merkle_find_spurious_key requires 8-byte lower and upper bounds")));

	if (partitions < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("merkle_find_spurious_key requires positive partition count")));

	memcpy(lower, VARDATA_ANY(lower_arg), 8);
	memcpy(upper, VARDATA_ANY(upper_arg), 8);

	/* Pre-initialize fixed header of BLAKE3 hasher for int8 canonical route digest */
	blake3_hasher_init(&base_hasher);
	blake3_hasher_update(&base_hasher, magic, sizeof(magic));
	merkle_hash_uint32(&base_hasher, MERKLE_ROUTE_FORMAT_VERSION); /* 4 */
	merkle_hash_uint32(&base_hasher, 1); /* nkeys = 1 */

	/* Key 1 schema descriptor */
	merkle_hash_uint32(&base_hasher, 1); /* attrnum = 1 */
	merkle_hash_uint32(&base_hasher, 20); /* INT8OID = 20 */
	merkle_hash_uint32(&base_hasher, -1); /* atttypmod = -1 */
	{
		uint8 null_flag = 0;
		blake3_hasher_update(&base_hasher, &null_flag, 1);
	}
	merkle_hash_uint32(&base_hasher, 8); /* key length = 8 bytes */

	for (i = 0; i < max_attempts; i++)
	{
		blake3_hasher hasher = base_hasher;
		uint8 key_bytes[8];
		uint8 digest[MERKLE_HASH_BYTES];
		uint64 route_value = 0;
		int64 candidate = base_offset - i;
		int p;

		key_bytes[0] = (uint8) ((uint64) candidate >> 56);
		key_bytes[1] = (uint8) ((uint64) candidate >> 48);
		key_bytes[2] = (uint8) ((uint64) candidate >> 40);
		key_bytes[3] = (uint8) ((uint64) candidate >> 32);
		key_bytes[4] = (uint8) ((uint64) candidate >> 24);
		key_bytes[5] = (uint8) ((uint64) candidate >> 16);
		key_bytes[6] = (uint8) ((uint64) candidate >> 8);
		key_bytes[7] = (uint8) candidate;

		blake3_hasher_update(&hasher, key_bytes, 8);
		blake3_hasher_finalize(&hasher, digest, MERKLE_HASH_BYTES);

		for (p = 0; p < 8; p++)
			route_value = (route_value << 8) | digest[p];

		if (partition_id >= 0 && (int32) (route_value % (uint64) partitions) != partition_id)
			continue;

		if (memcmp(digest, lower, 8) >= 0 && memcmp(digest, upper, 8) <= 0)
		{
			PG_RETURN_INT64(candidate);
		}
	}

	PG_RETURN_NULL();
}



Datum
merkle_key_hash_sql(PG_FUNCTION_ARGS)
{
	Datum		val;
	Oid			argtype;
	uint8		digest[MERKLE_HASH_BYTES];
	bytea	   *result;
	bool        isnull;
	MerkleKeyHashCache *cache;

	if (PG_ARGISNULL(0))
	{
		isnull = true;
		val = (Datum) 0;
	}
	else
	{
		isnull = false;
		val = PG_GETARG_DATUM(0);
	}

	argtype = get_fn_expr_argtype(fcinfo->flinfo, 0);
	if (!OidIsValid(argtype))
		argtype = INT8OID;

	cache = (MerkleKeyHashCache *) fcinfo->flinfo->fn_extra;
	if (cache == NULL || cache->argtype != argtype)
	{
		Oid typsend;
		bool typisvarlena;
		MemoryContext oldcxt;

		if (cache != NULL && cache->tupdesc != NULL)
			FreeTupleDesc(cache->tupdesc);

		oldcxt = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
		cache = (MerkleKeyHashCache *) palloc0(sizeof(MerkleKeyHashCache));
		cache->argtype = argtype;
		cache->tupdesc = CreateTemplateTupleDesc(1);
		TupleDescInitEntry(cache->tupdesc, (AttrNumber) 1, "key", argtype, -1, 0);

		getTypeBinaryOutputInfo(argtype, &typsend, &typisvarlena);
		fmgr_info_cxt(typsend, &cache->send_fn, fcinfo->flinfo->fn_mcxt);

		fcinfo->flinfo->fn_extra = (void *) cache;
		MemoryContextSwitchTo(oldcxt);
	}

	merkle_compute_canonical_route_digest_fast(&val, &isnull, 1, cache->tupdesc, &cache->send_fn, digest);

	result = (bytea *) palloc(VARHDRSZ + 8);
	SET_VARSIZE(result, VARHDRSZ + 8);
	memcpy(VARDATA(result), digest, 8);

	PG_RETURN_BYTEA_P(result);
}

typedef struct MerkleTupleHashCache
{
	Oid			tup_type;
	int32		tup_typmod;
	TupleDesc	tupdesc;
	TupleTableSlot *slot;
	FmgrInfo   *send_functions;
} MerkleTupleHashCache;

PG_FUNCTION_INFO_V1(merkle_tuple_hash_sql);

Datum
merkle_tuple_hash_sql(PG_FUNCTION_ARGS)
{
	HeapTupleHeader tuple_header;
	Oid			tup_type;
	int32		tup_typmod;
	TupleDesc	tupdesc;
	HeapTupleData tuple;
	MerkleHash	hash;
	bytea	   *result;
	MerkleTupleHashCache *cache;

	if (PG_ARGISNULL(0))
	{
		result = (bytea *) palloc(VARHDRSZ + MERKLE_HASH_BYTES);
		SET_VARSIZE(result, VARHDRSZ + MERKLE_HASH_BYTES);
		memset(VARDATA(result), 0, MERKLE_HASH_BYTES);
		PG_RETURN_BYTEA_P(result);
	}

	tuple_header = PG_GETARG_HEAPTUPLEHEADER(0);
	tup_type = HeapTupleHeaderGetTypeId(tuple_header);
	tup_typmod = HeapTupleHeaderGetTypMod(tuple_header);

	cache = (MerkleTupleHashCache *) fcinfo->flinfo->fn_extra;
	if (cache == NULL || cache->tup_type != tup_type || cache->tup_typmod != tup_typmod)
	{
		int i;

		if (cache != NULL)
		{
			if (cache->slot != NULL)
				ExecDropSingleTupleTableSlot(cache->slot);
			if (cache->tupdesc != NULL)
				FreeTupleDesc(cache->tupdesc);
		}

		cache = (MerkleTupleHashCache *) MemoryContextAllocZero(fcinfo->flinfo->fn_mcxt, sizeof(MerkleTupleHashCache));
		cache->tup_type = tup_type;
		cache->tup_typmod = tup_typmod;
		{
			MemoryContext oldcxt = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
			cache->tupdesc = lookup_rowtype_tupdesc_copy(tup_type, tup_typmod);
			cache->slot = MakeSingleTupleTableSlot(cache->tupdesc, &TTSOpsHeapTuple);
			MemoryContextSwitchTo(oldcxt);
		}
		cache->send_functions = (FmgrInfo *) MemoryContextAllocZero(fcinfo->flinfo->fn_mcxt, cache->tupdesc->natts * sizeof(FmgrInfo));

		for (i = 0; i < cache->tupdesc->natts; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(cache->tupdesc, i);
			if (!attr->attisdropped)
			{
				Oid typsend;
				bool typisvarlena;
				getTypeBinaryOutputInfo(attr->atttypid, &typsend, &typisvarlena);
				fmgr_info_cxt(typsend, &cache->send_functions[i], fcinfo->flinfo->fn_mcxt);
			}
		}

		fcinfo->flinfo->fn_extra = (void *) cache;
	}

	tupdesc = cache->tupdesc;
	memset(&tuple, 0, sizeof(tuple));
	tuple.t_len = HeapTupleHeaderGetDatumLength(tuple_header);
	ItemPointerSetInvalid(&(tuple.t_self));
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = tuple_header;

	ExecClearTuple(cache->slot);
	ExecStoreHeapTuple(&tuple, cache->slot, false);

	merkle_hash_slot_canonical_desc_fast(tupdesc, cache->slot, cache->send_functions, &hash);

	result = (bytea *) palloc(VARHDRSZ + MERKLE_HASH_BYTES);
	SET_VARSIZE(result, VARHDRSZ + MERKLE_HASH_BYTES);
	memcpy(VARDATA(result), hash.data, MERKLE_HASH_BYTES);

	PG_RETURN_BYTEA_P(result);
}

PG_FUNCTION_INFO_V1(merkle_hash_xor_sql);

Datum
merkle_hash_xor_sql(PG_FUNCTION_ARGS)
{
	bytea	   *arg1;
	bytea	   *arg2;
	bytea	   *result;
	MerkleHash	h1;
	MerkleHash	h2;
	MerkleHash	res_h;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("merkle_hash_xor_sql arguments cannot be null")));

	arg1 = PG_GETARG_BYTEA_PP(0);
	arg2 = PG_GETARG_BYTEA_PP(1);

	if (VARSIZE_ANY_EXHDR(arg1) != MERKLE_HASH_BYTES ||
		VARSIZE_ANY_EXHDR(arg2) != MERKLE_HASH_BYTES)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("merkle_hash_xor_sql requires exact %d-byte inputs", MERKLE_HASH_BYTES)));

	memcpy(h1.data, VARDATA_ANY(arg1), MERKLE_HASH_BYTES);
	memcpy(h2.data, VARDATA_ANY(arg2), MERKLE_HASH_BYTES);

	memcpy(&res_h, &h1, sizeof(MerkleHash));
	merkle_hash_xor(&res_h, &h2);

	result = (bytea *) palloc(VARHDRSZ + MERKLE_HASH_BYTES);
	SET_VARSIZE(result, VARHDRSZ + MERKLE_HASH_BYTES);
	memcpy(VARDATA(result), res_h.data, MERKLE_HASH_BYTES);

	PG_RETURN_BYTEA_P(result);
}

/* End of file */
