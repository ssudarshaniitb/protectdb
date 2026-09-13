/*-------------------------------------------------------------------------
 *
 * merklebuild.c
 *    Build routine for Merkle access method.
 *
 * This file contains the implementation of building a new Merkle index
 * from scratch, including heap table scanning and initial Merkle tree
 * construction.
 *
 * Copyright (c) 2026, AriaBC PostgreSQL Extensions
 *
 * IDENTIFICATION
 *    src/backend/access/merkle/merklebuild.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include <pthread.h>
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/merkle.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_type.h"
#include "common/blake3.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "port/pg_bswap.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#define MERKLE_ENTRY_CHUNK_SIZE 4194304 /* fixed-size chunk; entries include partition routing */

typedef struct
{
	MerkleTupleHashEntry **chunks;
	int			num_chunks;
	int			max_chunks;
	size_t		num_entries;
	size_t		chunk_size;
} MerkleEntryArray;

static inline MerkleTupleHashEntry *
merkle_entry_at(const MerkleEntryArray *ea, size_t idx)
{
	size_t c = idx / ea->chunk_size;
	size_t o = idx % ea->chunk_size;
	return &ea->chunks[c][o];
}

typedef struct
{
	uint8		node_id[8];
	int		partition_id;
	int16		prefix_len;
	bool		is_leaf;
	int64		tuple_count;
	MerkleHash	hash;
} MerkleNodeRecord;

typedef struct
{
	MerkleNodeRecord *records;
	int			num_records;
	int			max_records;
} MerkleBulkNodeSet;

static void
bulk_node_set_init(MerkleBulkNodeSet *bs, int initial_capacity)
{
	bs->records = (MerkleNodeRecord *) palloc((size_t) initial_capacity * sizeof(MerkleNodeRecord));
	bs->num_records = 0;
	bs->max_records = initial_capacity;
}

static void
bulk_node_set_add(MerkleBulkNodeSet *bs, int partition_id, const uint8 *node_id, int prefix_len,
				  bool is_leaf, int64 tuple_count, const MerkleHash *hash)
{
	MerkleNodeRecord *rec;

	if (bs->num_records >= bs->max_records)
	{
		bs->max_records *= 2;
		bs->records = (MerkleNodeRecord *) repalloc(bs->records,
							(size_t) bs->max_records * sizeof(MerkleNodeRecord));
	}
	rec = &bs->records[bs->num_records++];
	rec->partition_id = partition_id;
	memcpy(rec->node_id, node_id, 8);
	rec->prefix_len   = (int16) prefix_len;
	rec->is_leaf      = is_leaf;
	rec->tuple_count  = tuple_count;
	memcpy(&rec->hash, hash, sizeof(MerkleHash));
}

/*
 * merkle_build_tree_pass1 - compute full tree in C, emit records into bs.
 *
 * Zero-copy slice recursion over sorted MerkleEntryArray.
 * Returns the XOR-hash of the current subtree.
 */
static MerkleHash
merkle_build_tree_pass1(MerkleBulkNodeSet *bs,
						int partition_id,
						const uint8 *node_id, int prefix_len,
						const MerkleEntryArray *ea, size_t start_idx, size_t num_entries,
						int fanout, int bits_per_split, int split_threshold,
						int max_prefix_len)
{
	int			bucket_counts[16] = {0};
	int			bucket_offsets[16] = {0};
	MerkleHash	bucket_hashes[16];
	MerkleHash	node_hash;
	int64		total_count = (int64) num_entries;
	int			i;
	int			b;
	const MerkleTupleHashEntry *entries_base;

	merkle_hash_zero(&node_hash);
	if (num_entries <= 0)
		return node_hash;

	if (fanout > 16)
		elog(ERROR, "merkle tree fanout %d exceeds maximum stack capacity 16", fanout);

	memset(bucket_hashes, 0, fanout * sizeof(MerkleHash));

	entries_base = (ea->num_chunks == 1) ? &ea->chunks[0][start_idx] : NULL;

	b = 0;
	bucket_offsets[0] = 0;
	for (i = 0; i < (int) num_entries; i++)
	{
		const MerkleTupleHashEntry *e = entries_base ? &entries_base[i] : merkle_entry_at(ea, start_idx + (size_t) i);
		uint8 val = merkle_next_bits(e->key_hash, prefix_len, bits_per_split);
		while (b < (int) val && b < fanout)
		{
			b++;
			bucket_offsets[b] = i;
		}
		bucket_counts[b]++;
		merkle_hash_xor(&bucket_hashes[b], &e->tuple_hash);
	}
	while (b + 1 < fanout)
	{
		b++;
		bucket_offsets[b] = (int) num_entries;
	}

	for (i = 0; i < fanout; i++)
	{
		uint8	child_node_id[8];
		int		child_prefix_len = prefix_len + bits_per_split;

		merkle_bytea_extend(child_node_id, node_id, prefix_len, (uint8) i, bits_per_split);

		if (bucket_counts[i] > split_threshold && child_prefix_len < max_prefix_len)
		{
			MerkleHash child_hash = merkle_build_tree_pass1(
				bs, partition_id, child_node_id, child_prefix_len,
				ea, start_idx + (size_t) bucket_offsets[i], (size_t) bucket_counts[i],
				fanout, bits_per_split, split_threshold, max_prefix_len);
			merkle_hash_xor(&node_hash, &child_hash);
		}
		else
		{
			bulk_node_set_add(bs, partition_id, child_node_id, child_prefix_len,
							  true, (int64) bucket_counts[i], &bucket_hashes[i]);
			merkle_hash_xor(&node_hash, &bucket_hashes[i]);
		}
	}

	/* Emit internal node record after children */
	if (prefix_len > 0)
		bulk_node_set_add(bs, partition_id, node_id, prefix_len, false, total_count, &node_hash);

	return node_hash;
}

static double g_phase3_catalog_flush_ms = 0.0;
static double g_phase3_tree_build_ms = 0.0;
static double g_phase3_slice_scan_ms = 0.0;

#define BULK_INSERT_BATCH 1000

typedef struct MerkleCatalogFlushState
{
	Relation		catalog_rel;
	TupleDesc		tupdesc;
	ResultRelInfo  *result_rel_info;
	EState		   *estate;
	BulkInsertState	bistate;
	CommandId		mycid;
	TupleTableSlot **slots;
	int				batch_size;
	int				nslots;
	MemoryContext	batch_cxt;
	int				att_index_oid;
	int				att_partition_id;
	int				att_node_id;
	int				att_prefix_len;
	int				att_is_leaf;
	int				att_tuple_count;
	int				att_hash;
} MerkleCatalogFlushState;

static MerkleCatalogFlushState *
merkle_catalog_flush_init(Oid index_oid, int batch_size)
{
	MerkleCatalogFlushState *state = (MerkleCatalogFlushState *) palloc0(sizeof(MerkleCatalogFlushState));
	RangeVar   *rv = makeRangeVar("ariabc_internal", "merkle_node", -1);
	int			i;

	state->catalog_rel = table_openrv(rv, RowExclusiveLock);
	state->tupdesc = RelationGetDescr(state->catalog_rel);
	state->batch_size = batch_size;
	state->nslots = 0;
	state->mycid = GetCurrentCommandId(true);
	state->bistate = GetBulkInsertState();

	state->estate = CreateExecutorState();
	state->result_rel_info = makeNode(ResultRelInfo);
	InitResultRelInfo(state->result_rel_info, state->catalog_rel, 1, NULL, 0);
	ExecOpenIndices(state->result_rel_info, false);
	state->estate->es_result_relations = state->result_rel_info;
	state->estate->es_num_result_relations = 1;
	state->estate->es_result_relation_info = state->result_rel_info;

	/* Pre-allocate slot array */
	state->slots = (TupleTableSlot **) palloc(batch_size * sizeof(TupleTableSlot *));
	for (i = 0; i < batch_size; i++)
		state->slots[i] = MakeTupleTableSlot(state->tupdesc, &TTSOpsHeapTuple);

	/* Separate memory context for per-batch allocations */
	state->batch_cxt = AllocSetContextCreate(CurrentMemoryContext,
											 "MerkleCatalogBatchContext",
											 ALLOCSET_DEFAULT_SIZES);

	/* Resolve column attribute numbers dynamically */
	state->att_index_oid = -1;
	state->att_partition_id = -1;
	state->att_node_id = -1;
	state->att_prefix_len = -1;
	state->att_is_leaf = -1;
	state->att_tuple_count = -1;
	state->att_hash = -1;

	for (i = 0; i < state->tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(state->tupdesc, i);

		if (attr->attisdropped)
			continue;
		if (strcmp(NameStr(attr->attname), "index_oid") == 0)
			state->att_index_oid = i;
		else if (strcmp(NameStr(attr->attname), "partition_id") == 0)
			state->att_partition_id = i;
		else if (strcmp(NameStr(attr->attname), "node_id") == 0)
			state->att_node_id = i;
		else if (strcmp(NameStr(attr->attname), "prefix_len") == 0)
			state->att_prefix_len = i;
		else if (strcmp(NameStr(attr->attname), "is_leaf") == 0)
			state->att_is_leaf = i;
		else if (strcmp(NameStr(attr->attname), "tuple_count") == 0)
			state->att_tuple_count = i;
		else if (strcmp(NameStr(attr->attname), "hash") == 0)
			state->att_hash = i;
	}

	if (state->att_index_oid < 0 || state->att_partition_id < 0 ||
		state->att_node_id < 0 || state->att_prefix_len < 0 ||
		state->att_is_leaf < 0 || state->att_tuple_count < 0 ||
		state->att_hash < 0)
	{
		elog(ERROR, "ariabc_internal.merkle_node catalog schema mismatch: missing required columns");
	}

	return state;
}

static void
merkle_catalog_flush_batch(MerkleCatalogFlushState *state)
{
	int i;

	if (state->nslots == 0)
		return;

	/* 1. Bulk insert slots into heap relation in one operation */
	table_multi_insert(state->catalog_rel,
					   state->slots,
					   state->nslots,
					   state->mycid,
					   0,
					   state->bistate);

	/* 2. Insert index tuples for each slot */
	for (i = 0; i < state->nslots; i++)
	{
		List *recheckIndexes = ExecInsertIndexTuples(state->slots[i],
													 state->estate,
													 false,
													 NULL,
													 NIL);
		if (recheckIndexes != NIL)
			list_free(recheckIndexes);

		ExecClearTuple(state->slots[i]);
	}

	state->nslots = 0;
	MemoryContextReset(state->batch_cxt);
	ResetPerTupleExprContext(state->estate);
}

static void
merkle_catalog_flush_add(MerkleCatalogFlushState *state,
						 Oid index_oid,
						 const MerkleNodeRecord *r)
{
	MemoryContext oldcxt;
	bytea	   *node_id_bytea;
	bytea	   *hash_bytea;
	Datum		values[7];
	bool		isnull[7] = {false, false, false, false, false, false, false};
	HeapTuple	htup;

	if (state->nslots >= state->batch_size)
		merkle_catalog_flush_batch(state);

	oldcxt = MemoryContextSwitchTo(state->batch_cxt);

	node_id_bytea = (bytea *) palloc(VARHDRSZ + 8);
	SET_VARSIZE(node_id_bytea, VARHDRSZ + 8);
	memcpy(VARDATA(node_id_bytea), r->node_id, 8);

	hash_bytea = (bytea *) palloc(VARHDRSZ + MERKLE_HASH_BYTES);
	SET_VARSIZE(hash_bytea, VARHDRSZ + MERKLE_HASH_BYTES);
	memcpy(VARDATA(hash_bytea), r->hash.data, MERKLE_HASH_BYTES);

	values[state->att_index_oid] = ObjectIdGetDatum(index_oid);
	values[state->att_partition_id] = Int32GetDatum(r->partition_id);
	values[state->att_node_id] = PointerGetDatum(node_id_bytea);
	values[state->att_prefix_len] = Int16GetDatum(r->prefix_len);
	values[state->att_is_leaf] = BoolGetDatum(r->is_leaf);
	values[state->att_tuple_count] = Int64GetDatum(r->tuple_count);
	values[state->att_hash] = PointerGetDatum(hash_bytea);

	htup = heap_form_tuple(state->tupdesc, values, isnull);
	ExecStoreHeapTuple(htup, state->slots[state->nslots], true);
	state->nslots++;

	MemoryContextSwitchTo(oldcxt);
}

static void
merkle_catalog_flush_finish(MerkleCatalogFlushState *state)
{
	int i;

	if (state->nslots > 0)
		merkle_catalog_flush_batch(state);

	table_finish_bulk_insert(state->catalog_rel, 0);
	ExecCloseIndices(state->result_rel_info);
	FreeBulkInsertState(state->bistate);

	for (i = 0; i < state->batch_size; i++)
	{
		if (state->slots[i])
			ExecDropSingleTupleTableSlot(state->slots[i]);
	}
	pfree(state->slots);

	FreeExecutorState(state->estate);
	MemoryContextDelete(state->batch_cxt);
	table_close(state->catalog_rel, RowExclusiveLock);
	pfree(state);
}

typedef enum MerkleAttrKind
{
	MERKLE_ATTR_GENERIC = 0,
	MERKLE_ATTR_INT8,
	MERKLE_ATTR_INT4,
	MERKLE_ATTR_INT2,
	MERKLE_ATTR_BOOL,
	MERKLE_ATTR_VARLENA
} MerkleAttrKind;

static inline MerkleAttrKind
merkle_resolve_attr_kind(Oid typid)
{
	switch (typid)
	{
		case INT8OID:
			return MERKLE_ATTR_INT8;
		case INT4OID:
			return MERKLE_ATTR_INT4;
		case INT2OID:
			return MERKLE_ATTR_INT2;
		case BOOLOID:
			return MERKLE_ATTR_BOOL;
		case TEXTOID:
		case VARCHAROID:
		case BPCHAROID:
		case BYTEAOID:
			return MERKLE_ATTR_VARLENA;
		default:
			return MERKLE_ATTR_GENERIC;
	}
}

/*
 * Per-tuple callback state for index build
 */
typedef struct
{
	Relation	indexRel;
	Relation	heapRel;
	IndexFetchTableData *heapFetch;
	TupleTableSlot *heapSlot;
	double		indtuples;
	int			nkeys;			/* Number of index key columns */
	int			fanout;

	/* Chunked tuple tracking */
	MerkleTupleHashEntry **chunks;
	int			num_chunks;
	int			max_chunks;
	size_t		num_entries;

	TupleDesc	tupdesc;
	FmgrInfo   *send_functions;
	FmgrInfo   *key_send_functions;
	MerkleAttrKind *attr_kinds;
	MerkleAttrKind *key_kinds;
	uint8     (*attr_headers)[12];
	uint8     (*key_headers)[12];
	blake3_hasher base_route_hasher;
	blake3_hasher base_tuple_hasher;

	int			bits_per_split;
	int			split_threshold;
	int			merge_threshold;
	int			num_partitions;
} MerkleBuildState;

static void merkle_emit_build_nodes_report(Relation indexRel,
										  MerkleBuildState *buildstate);

static void
merkle_emit_build_nodes_report(Relation indexRel, MerkleBuildState *buildstate)
{
	/* Logging is disabled during index creation (CREATE INDEX / REINDEX) */
	(void) indexRel;
	(void) buildstate;
}

static inline uint64
merkle_entry_get_key64(const MerkleTupleHashEntry *e)
{
	return pg_bswap64(*(const uint64 *) e->key_hash);
}

static void
merkle_entry_insertion_sort(MerkleTupleHashEntry *arr, size_t n)
{
	size_t i;
	for (i = 1; i < n; i++)
	{
		MerkleTupleHashEntry tmp = arr[i];
		uint64 tmp_k = merkle_entry_get_key64(&tmp);
		size_t j = i;
		while (j > 0 && merkle_entry_get_key64(&arr[j - 1]) > tmp_k)
		{
			arr[j] = arr[j - 1];
			j--;
		}
		arr[j] = tmp;
	}
}

static void
merkle_entry_quicksort_inlined(MerkleTupleHashEntry *arr, size_t n)
{
	while (n > 16)
	{
		size_t mid = n / 2;
		uint64 k_low = merkle_entry_get_key64(&arr[0]);
		uint64 k_mid = merkle_entry_get_key64(&arr[mid]);
		uint64 k_high = merkle_entry_get_key64(&arr[n - 1]);
		uint64 pivot_k;
		size_t i, j;
		MerkleTupleHashEntry tmp;

		/* Median-of-three pivot selection */
		if (k_low > k_mid)
		{
			tmp = arr[0]; arr[0] = arr[mid]; arr[mid] = tmp;
			k_mid = k_low;
			k_low = merkle_entry_get_key64(&arr[0]);
		}
		if (k_mid > k_high)
		{
			tmp = arr[mid]; arr[mid] = arr[n - 1]; arr[n - 1] = tmp;
			k_mid = merkle_entry_get_key64(&arr[mid]);
		}
		if (k_low > k_mid)
		{
			tmp = arr[0]; arr[0] = arr[mid]; arr[mid] = tmp;
			k_mid = merkle_entry_get_key64(&arr[mid]);
		}
		pivot_k = k_mid;

		/* Hoare partition */
		i = 0;
		j = n - 1;
		for (;;)
		{
			while (merkle_entry_get_key64(&arr[i]) < pivot_k)
				i++;
			while (merkle_entry_get_key64(&arr[j]) > pivot_k)
				j--;
			if (i >= j)
				break;
			tmp = arr[i];
			arr[i] = arr[j];
			arr[j] = tmp;
			i++;
			j--;
		}

		if (i == 0)
			i = 1;

		if (i < n - i)
		{
			if (i > 1)
				merkle_entry_quicksort_inlined(arr, i);
			arr += i;
			n -= i;
		}
		else
		{
			if (n - i > 1)
				merkle_entry_quicksort_inlined(arr + i, n - i);
			n = i;
		}
	}
	if (n > 1)
		merkle_entry_insertion_sort(arr, n);
}

typedef struct MerkleSortThreadArg
{
	MerkleTupleHashEntry *entries;
	size_t			   *partition_starts;
	size_t			   *partition_counts;
	int					p_start;
	int					p_end;
} MerkleSortThreadArg;

static void *
merkle_sort_worker(void *arg)
{
	MerkleSortThreadArg *targ = (MerkleSortThreadArg *) arg;
	int p;

	for (p = targ->p_start; p < targ->p_end; p++)
	{
		size_t start = targ->partition_starts[p];
		size_t count = targ->partition_counts[p];

		if (count > 1)
			merkle_entry_quicksort_inlined(&targ->entries[start], count);
	}

	return NULL;
}

static MerkleEntryArray
merkle_prepare_sorted_entries(MerkleBuildState *buildstate)
{
	MerkleEntryArray result;
	size_t total_entries = buildstate->num_entries;
	int num_partitions = buildstate->num_partitions;
	size_t *partition_counts;
	size_t *partition_starts;
	size_t *partition_cur;
	size_t running_offset;
	int p, c;
	int num_threads;
	pthread_t threads[16];
	MerkleSortThreadArg thread_args[16];

	result.num_entries = total_entries;
	result.chunk_size = MERKLE_ENTRY_CHUNK_SIZE;

	if (total_entries == 0)
	{
		result.num_chunks = 0;
		result.chunks = NULL;
		return result;
	}

	if (num_partitions <= 0)
		num_partitions = 1;

	partition_counts = (size_t *) palloc0(num_partitions * sizeof(size_t));
	partition_starts = (size_t *) palloc(num_partitions * sizeof(size_t));
	partition_cur = (size_t *) palloc(num_partitions * sizeof(size_t));

	/* Pass 1: Count entries per partition in linear O(N) */
	for (c = 0; c < buildstate->num_chunks; c++)
	{
		size_t chunk_len = (c == buildstate->num_chunks - 1) ?
			(total_entries - (size_t) c * MERKLE_ENTRY_CHUNK_SIZE) :
			(size_t) MERKLE_ENTRY_CHUNK_SIZE;
		MerkleTupleHashEntry *chunk = buildstate->chunks[c];
		size_t i;

		for (i = 0; i < chunk_len; i++)
		{
			uint32 part = chunk[i].partition_id;
			if (part < (uint32) num_partitions)
				partition_counts[part]++;
		}
	}

	/* Compute partition prefix offsets */
	running_offset = 0;
	for (p = 0; p < num_partitions; p++)
	{
		partition_starts[p] = running_offset;
		partition_cur[p] = running_offset;
		running_offset += partition_counts[p];
	}

	/* Allocate result chunks in TopTransactionContext */
	result.num_chunks = (int) ((total_entries + MERKLE_ENTRY_CHUNK_SIZE - 1) / MERKLE_ENTRY_CHUNK_SIZE);
	result.chunks = (MerkleTupleHashEntry **) MemoryContextAlloc(
		TopTransactionContext, (size_t) result.num_chunks * sizeof(MerkleTupleHashEntry *));

	for (c = 0; c < result.num_chunks; c++)
	{
		size_t cap = (c == result.num_chunks - 1) ?
			(total_entries - (size_t) c * MERKLE_ENTRY_CHUNK_SIZE) :
			(size_t) MERKLE_ENTRY_CHUNK_SIZE;
		result.chunks[c] = (MerkleTupleHashEntry *) MemoryContextAlloc(
			TopTransactionContext, cap * sizeof(MerkleTupleHashEntry));
	}

	/* Pass 2: Linear O(N) scatter directly into sorted partition slices */
	for (c = 0; c < buildstate->num_chunks; c++)
	{
		size_t chunk_len = (c == buildstate->num_chunks - 1) ?
			(total_entries - (size_t) c * MERKLE_ENTRY_CHUNK_SIZE) :
			(size_t) MERKLE_ENTRY_CHUNK_SIZE;
		MerkleTupleHashEntry *chunk = buildstate->chunks[c];
		size_t i;

		for (i = 0; i < chunk_len; i++)
		{
			uint32 part = chunk[i].partition_id;
			size_t dst_idx = partition_cur[part]++;
			size_t dst_c = dst_idx / MERKLE_ENTRY_CHUNK_SIZE;
			size_t dst_o = dst_idx % MERKLE_ENTRY_CHUNK_SIZE;
			result.chunks[dst_c][dst_o] = chunk[i];
		}
	}

	/* Free original unpartitioned input chunks */
	for (c = 0; c < buildstate->num_chunks; c++)
		pfree(buildstate->chunks[c]);
	pfree(buildstate->chunks);
	buildstate->chunks = NULL;
	buildstate->num_chunks = 0;

	/*
	 * Pass 3: Parallel partition sorting across CPU worker threads.
	 * If result is within a single chunk (up to 4M entries), all partition slices
	 * are contiguous in result.chunks[0].
	 */
	if (result.num_chunks == 1)
	{
		num_threads = 16;
		if (num_threads > num_partitions)
			num_threads = num_partitions;
		if (num_threads < 1)
			num_threads = 1;

		if (num_threads == 1)
		{
			thread_args[0].entries = result.chunks[0];
			thread_args[0].partition_starts = partition_starts;
			thread_args[0].partition_counts = partition_counts;
			thread_args[0].p_start = 0;
			thread_args[0].p_end = num_partitions;
			merkle_sort_worker(&thread_args[0]);
		}
		else
		{
			int t;
			for (t = 0; t < num_threads; t++)
			{
				thread_args[t].entries = result.chunks[0];
				thread_args[t].partition_starts = partition_starts;
				thread_args[t].partition_counts = partition_counts;
				thread_args[t].p_start = (t * num_partitions) / num_threads;
				thread_args[t].p_end = ((t + 1) * num_partitions) / num_threads;
				pthread_create(&threads[t], NULL, merkle_sort_worker, &thread_args[t]);
			}
			for (t = 0; t < num_threads; t++)
			{
				pthread_join(threads[t], NULL);
			}
		}
	}
	else
	{
		/* Fallback for multi-chunk datasets (> 4M entries) */
		for (p = 0; p < num_partitions; p++)
		{
			size_t start = partition_starts[p];
			size_t count = partition_counts[p];
			size_t start_c = start / MERKLE_ENTRY_CHUNK_SIZE;
			size_t end_c = (start + count - 1) / MERKLE_ENTRY_CHUNK_SIZE;

			if (start_c == end_c && count > 1)
			{
				size_t start_o = start % MERKLE_ENTRY_CHUNK_SIZE;
				merkle_entry_quicksort_inlined(&result.chunks[start_c][start_o], count);
			}
			else if (count > 1)
			{
				MerkleTupleHashEntry *temp = (MerkleTupleHashEntry *) palloc(count * sizeof(MerkleTupleHashEntry));
				size_t i;
				for (i = 0; i < count; i++)
					temp[i] = *merkle_entry_at(&result, start + i);
				merkle_entry_quicksort_inlined(temp, count);
				for (i = 0; i < count; i++)
					*merkle_entry_at(&result, start + i) = temp[i];
				pfree(temp);
			}
		}
	}

	pfree(partition_counts);
	pfree(partition_starts);
	pfree(partition_cur);

	return result;
}

static inline void
merkle_hash_uint32_local(blake3_hasher *hasher, uint32 value)
{
	uint8 bytes[4];
	bytes[0] = (uint8) (value >> 24);
	bytes[1] = (uint8) (value >> 16);
	bytes[2] = (uint8) (value >> 8);
	bytes[3] = (uint8) value;
	blake3_hasher_update(hasher, bytes, sizeof(bytes));
}

static double g_single_pass_hash_time_ms = 0.0;
static double g_prof_p1_vacuum_ms = 0.0;
static double g_prof_p1_deform_ms = 0.0;
static double g_prof_p1_route_hash_ms = 0.0;
static double g_prof_p1_row_hash_ms = 0.0;
static double g_prof_p1_chunk_append_ms = 0.0;

static inline void
merkle_hash_datum(blake3_hasher *hasher,
				  const uint8 header[12],
				  Datum val,
				  bool isnull,
				  MerkleAttrKind kind,
				  FmgrInfo *send_fn)
{
	uint8 header_buf[17];

	if (isnull)
	{
		memcpy(header_buf, header, 12);
		header_buf[12] = 1;
		blake3_hasher_update(hasher, header_buf, 13);
		return;
	}

	memcpy(header_buf, header, 12);
	header_buf[12] = 0;

	switch (kind)
	{
		case MERKLE_ATTR_INT8:
		{
			uint64 v = pg_bswap64((uint64) DatumGetInt64(val));
			header_buf[13] = 0;
			header_buf[14] = 0;
			header_buf[15] = 0;
			header_buf[16] = 8;
			blake3_hasher_update(hasher, header_buf, 17);
			blake3_hasher_update(hasher, &v, 8);
			break;
		}
		case MERKLE_ATTR_INT4:
		{
			uint32 v = pg_bswap32((uint32) DatumGetInt32(val));
			header_buf[13] = 0;
			header_buf[14] = 0;
			header_buf[15] = 0;
			header_buf[16] = 4;
			blake3_hasher_update(hasher, header_buf, 17);
			blake3_hasher_update(hasher, &v, 4);
			break;
		}
		case MERKLE_ATTR_INT2:
		{
			uint16 v = pg_bswap16((uint16) DatumGetInt16(val));
			header_buf[13] = 0;
			header_buf[14] = 0;
			header_buf[15] = 0;
			header_buf[16] = 2;
			blake3_hasher_update(hasher, header_buf, 17);
			blake3_hasher_update(hasher, &v, 2);
			break;
		}
		case MERKLE_ATTR_BOOL:
		{
			uint8 v = DatumGetBool(val) ? 1 : 0;
			header_buf[13] = 0;
			header_buf[14] = 0;
			header_buf[15] = 0;
			header_buf[16] = 1;
			blake3_hasher_update(hasher, header_buf, 17);
			blake3_hasher_update(hasher, &v, 1);
			break;
		}
		case MERKLE_ATTR_VARLENA:
		{
			struct varlena *vl = (struct varlena *) DatumGetPointer(val);
			uint32 length;
			char *data;

			if (VARATT_IS_EXTENDED(vl))
			{
				struct varlena *detoasted = pg_detoast_datum_packed(vl);
				length = (uint32) VARSIZE_ANY_EXHDR(detoasted);
				data = VARDATA_ANY(detoasted);

				header_buf[13] = (uint8) (length >> 24);
				header_buf[14] = (uint8) (length >> 16);
				header_buf[15] = (uint8) (length >> 8);
				header_buf[16] = (uint8) length;
				blake3_hasher_update(hasher, header_buf, 17);
				if (length > 0)
					blake3_hasher_update(hasher, data, length);

				if (detoasted != vl)
					pfree(detoasted);
			}
			else
			{
				length = (uint32) VARSIZE_ANY_EXHDR(vl);
				data = VARDATA_ANY(vl);

				header_buf[13] = (uint8) (length >> 24);
				header_buf[14] = (uint8) (length >> 16);
				header_buf[15] = (uint8) (length >> 8);
				header_buf[16] = (uint8) length;
				blake3_hasher_update(hasher, header_buf, 17);
				if (length > 0)
					blake3_hasher_update(hasher, data, length);
			}
			break;
		}
		default:
		{
			bytea *encoded = DatumGetByteaP(FunctionCall1(send_fn, val));
			uint32 length = (uint32) VARSIZE_ANY_EXHDR(encoded);

			header_buf[13] = (uint8) (length >> 24);
			header_buf[14] = (uint8) (length >> 16);
			header_buf[15] = (uint8) (length >> 8);
			header_buf[16] = (uint8) length;
			blake3_hasher_update(hasher, header_buf, 17);
			if (length > 0)
				blake3_hasher_update(hasher, VARDATA_ANY(encoded), length);

			pfree(encoded);
			break;
		}
	}
}

static inline void
merkle_compute_route_and_hash_direct(Relation indexRel,
									  Datum *key_values, bool *key_isnull,
									  Datum *row_values, bool *row_isnull,
									  MerkleBuildState *buildstate,
									  MerkleRoute *route,
									  MerkleHash *tuple_hash)
{
	blake3_hasher route_hasher = buildstate->base_route_hasher;
	blake3_hasher tuple_hasher = buildstate->base_tuple_hasher;
	TupleDesc tupdesc = buildstate->tupdesc;
	int i;

	/* 1. Compute Route Digest over index keys using cached headers and zero-alloc serializer */
	for (i = 0; i < buildstate->nkeys; i++)
	{
		merkle_hash_datum(&route_hasher,
						  buildstate->key_headers[i],
						  key_values[i],
						  key_isnull[i],
						  buildstate->key_kinds[i],
						  &buildstate->key_send_functions[i]);
	}

	/* 2. Compute Tuple Hash over all heap attributes using cached headers and zero-alloc serializer */
	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

		if (attr->attisdropped)
			continue;

		merkle_hash_datum(&tuple_hasher,
						  buildstate->attr_headers[i],
						  row_values[i],
						  row_isnull[i],
						  buildstate->attr_kinds[i],
						  &buildstate->send_functions[i]);
	}

	blake3_hasher_finalize(&route_hasher, route->route_digest, MERKLE_HASH_BYTES);
	blake3_hasher_finalize(&tuple_hasher, tuple_hash->data, MERKLE_HASH_BYTES);

	route->static_route_value = pg_bswap64(*((uint64 *) route->route_digest));
	route->partition_id = (int) (route->static_route_value % (uint64) buildstate->num_partitions);
}

/*
 * merkle_process_tuple_direct() - Process and append one live tuple during index build
 */
static inline void
merkle_process_tuple_direct(MerkleBuildState *buildstate,
							Datum *key_values,
							bool *key_isnull,
							Datum *row_values,
							bool *row_isnull)
{
	MerkleHash		hash;
	MerkleRoute		route;
	size_t			chunk_idx;
	size_t			chunk_off;
	MerkleTupleHashEntry *entry;

	merkle_compute_route_and_hash_direct(buildstate->indexRel,
										 key_values, key_isnull,
										 row_values, row_isnull,
										 buildstate, &route, &hash);

	/* Dynamic indexing tuple tracking via chunked allocations */
	chunk_idx = buildstate->num_entries / MERKLE_ENTRY_CHUNK_SIZE;
	chunk_off = buildstate->num_entries % MERKLE_ENTRY_CHUNK_SIZE;

	if (chunk_idx >= (size_t) buildstate->num_chunks)
	{
		if (buildstate->num_chunks >= buildstate->max_chunks)
		{
			int new_max = buildstate->max_chunks ? buildstate->max_chunks * 2 : 16;

			if (buildstate->chunks == NULL)
				buildstate->chunks = (MerkleTupleHashEntry **)
					palloc((size_t) new_max * sizeof(MerkleTupleHashEntry *));
			else
				buildstate->chunks = (MerkleTupleHashEntry **)
					repalloc(buildstate->chunks,
							 (size_t) new_max * sizeof(MerkleTupleHashEntry *));
			buildstate->max_chunks = new_max;
		}
		buildstate->chunks[buildstate->num_chunks] = (MerkleTupleHashEntry *)
			MemoryContextAlloc(TopTransactionContext,
							   (size_t) MERKLE_ENTRY_CHUNK_SIZE * sizeof(MerkleTupleHashEntry));
		buildstate->num_chunks++;
	}

	entry = &buildstate->chunks[chunk_idx][chunk_off];
	memcpy(entry->key_hash, route.route_digest, 8);
	entry->partition_id = (uint32) route.partition_id;
	memcpy(&entry->tuple_hash, &hash, sizeof(MerkleHash));
	buildstate->num_entries++;

	buildstate->indtuples += 1;
}

/*
 * merkle_heapam_index_build_scan() - High-performance single-pass Heap AM scan
 *
 * Scans the heap table directly without redundant re-fetch calls, extracting
 * all tuple attributes in-place from pinned buffer slots while maintaining full
 * MVCC visibility, HOT chain root resolution, and vacuum compliance.
 */
static double
merkle_heapam_index_build_scan(Relation heapRelation,
							   Relation indexRelation,
							   IndexInfo *indexInfo,
							   MerkleBuildState *buildstate)
{
	Snapshot		snapshot;
	TransactionId	OldestXmin = InvalidTransactionId;
	BlockNumber		nblocks;
	BlockNumber		blkno;
	BufferAccessStrategy strategy;
	TupleDesc		tupdesc = RelationGetDescr(heapRelation);
	int				natts = tupdesc->natts;
	ExprState	   *predicate = NULL;
	EState		   *estate = NULL;
	ExprContext	   *econtext = NULL;
	TupleTableSlot *slot = NULL;
	double			reltuples = 0;
	bool			need_unregister_snapshot = false;
	int				max_tuples_per_page = MaxHeapTuplesPerPage;
	HeapTupleData  *page_tuples;
	Datum		  **page_row_values;
	bool		  **page_row_isnull;
	MerkleRoute	   *page_routes;
	MerkleHash	   *page_hashes;
	int				t;

	Assert(OidIsValid(indexRelation->rd_rel->relam));
	Assert(heapRelation->rd_rel->relam == HEAP_TABLE_AM_OID);

	page_tuples = (HeapTupleData *) palloc(max_tuples_per_page * sizeof(HeapTupleData));
	page_routes = (MerkleRoute *) palloc(max_tuples_per_page * sizeof(MerkleRoute));
	page_hashes = (MerkleHash *) palloc(max_tuples_per_page * sizeof(MerkleHash));
	page_row_values = (Datum **) palloc(max_tuples_per_page * sizeof(Datum *));
	page_row_isnull = (bool **) palloc(max_tuples_per_page * sizeof(bool *));

	for (t = 0; t < max_tuples_per_page; t++)
	{
		page_row_values[t] = (Datum *) palloc(natts * sizeof(Datum));
		page_row_isnull[t] = (bool *) palloc(natts * sizeof(bool));
	}

	if (indexInfo->ii_Predicate != NIL || indexInfo->ii_Expressions != NIL)
	{
		estate = CreateExecutorState();
		econtext = GetPerTupleExprContext(estate);
		slot = table_slot_create(heapRelation, NULL);
		econtext->ecxt_scantuple = slot;
		predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);
	}

	if (!IsBootstrapProcessingMode() && !indexInfo->ii_Concurrent)
		OldestXmin = GetOldestXmin(heapRelation, PROCARRAY_FLAGS_VACUUM);

	if (!TransactionIdIsValid(OldestXmin))
	{
		snapshot = RegisterSnapshot(GetTransactionSnapshot());
		need_unregister_snapshot = true;
	}
	else
		snapshot = SnapshotAny;

	strategy = GetAccessStrategy(BAS_BULKREAD);
	nblocks = RelationGetNumberOfBlocks(heapRelation);

	{
		instr_time hash_accum, vac_accum, def_accum, rhash_accum, thash_accum, app_accum;
		INSTR_TIME_SET_ZERO(hash_accum);
		INSTR_TIME_SET_ZERO(vac_accum);
		INSTR_TIME_SET_ZERO(def_accum);
		INSTR_TIME_SET_ZERO(rhash_accum);
		INSTR_TIME_SET_ZERO(thash_accum);
		INSTR_TIME_SET_ZERO(app_accum);

		for (blkno = 0; blkno < nblocks; blkno++)
		{
			Buffer			buf;
			Page			page;
			OffsetNumber	maxoff;
			OffsetNumber	offnum;
			OffsetNumber	root_offsets[MaxHeapTuplesPerPage];
			instr_time		t_proc_start, t_proc_end;
			instr_time		t_sub_start, t_sub_end;
			int				ntup = 0;
			int				j;

			CHECK_FOR_INTERRUPTS();

			buf = ReadBufferExtended(heapRelation, MAIN_FORKNUM, blkno,
									 RBM_NORMAL, strategy);
			LockBuffer(buf, BUFFER_LOCK_SHARE);
			page = BufferGetPage(buf);

			if (PageIsNew(page) || PageIsEmpty(page))
			{
				LockBuffer(buf, BUFFER_LOCK_UNLOCK);
				ReleaseBuffer(buf);
				continue;
			}

			maxoff = PageGetMaxOffsetNumber(page);
			heap_get_root_tuples(page, root_offsets);

			INSTR_TIME_SET_CURRENT(t_proc_start);

			/* 1. Vacuum Visibility & Tuple Collection */
			INSTR_TIME_SET_CURRENT(t_sub_start);
			for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum = OffsetNumberNext(offnum))
			{
				ItemId			itemId = PageGetItemId(page, offnum);
				bool			indexIt = false;

				if (!ItemIdIsNormal(itemId))
					continue;

				page_tuples[ntup].t_data = (HeapTupleHeader) PageGetItem(page, itemId);
				page_tuples[ntup].t_len = ItemIdGetLength(itemId);
				ItemPointerSet(&(page_tuples[ntup].t_self), blkno, offnum);
				page_tuples[ntup].t_tableOid = RelationGetRelid(heapRelation);

				if (snapshot == SnapshotAny)
				{
					switch (HeapTupleSatisfiesVacuum(&page_tuples[ntup], OldestXmin, buf))
					{
						case HEAPTUPLE_DEAD:
							indexIt = false;
							break;
						case HEAPTUPLE_LIVE:
							indexIt = true;
							reltuples += 1;
							break;
						case HEAPTUPLE_RECENTLY_DEAD:
							if (HeapTupleIsHotUpdated(&page_tuples[ntup]))
							{
								indexIt = false;
								indexInfo->ii_BrokenHotChain = true;
							}
							else
							{
								indexIt = true;
								reltuples += 1;
							}
							break;
						case HEAPTUPLE_INSERT_IN_PROGRESS:
							indexIt = true;
							reltuples += 1;
							break;
						case HEAPTUPLE_DELETE_IN_PROGRESS:
							indexIt = true;
							reltuples += 1;
							break;
						default:
							elog(ERROR, "unexpected HeapTupleSatisfiesVacuum result");
							indexIt = false;
							break;
					}
				}
				else
				{
					indexIt = true;
					reltuples += 1;
				}

				if (indexIt)
					ntup++;
			}
			INSTR_TIME_SET_CURRENT(t_sub_end);
			INSTR_TIME_ACCUM_DIFF(vac_accum, t_sub_end, t_sub_start);

			/* 2. In-Memory Tuple Deforming */
			INSTR_TIME_SET_CURRENT(t_sub_start);
			for (j = 0; j < ntup; j++)
			{
				heap_deform_tuple(&page_tuples[j], tupdesc, page_row_values[j], page_row_isnull[j]);
			}
			INSTR_TIME_SET_CURRENT(t_sub_end);
			INSTR_TIME_ACCUM_DIFF(def_accum, t_sub_end, t_sub_start);

			/* 3. Route Key Hashing */
			INSTR_TIME_SET_CURRENT(t_sub_start);
			for (j = 0; j < ntup; j++)
			{
				blake3_hasher route_hasher = buildstate->base_route_hasher;
				int k;

				for (k = 0; k < buildstate->nkeys; k++)
				{
					AttrNumber attno = indexInfo->ii_IndexAttrNumbers[k];
					if (attno > 0)
					{
						merkle_hash_datum(&route_hasher,
										  buildstate->key_headers[k],
										  page_row_values[j][attno - 1],
										  page_row_isnull[j][attno - 1],
										  buildstate->key_kinds[k],
										  &buildstate->key_send_functions[k]);
					}
					else if (indexInfo->ii_Expressions != NIL || predicate != NULL)
					{
						Datum key_val;
						bool key_null;
						MemoryContextReset(econtext->ecxt_per_tuple_memory);
						ExecStoreBufferHeapTuple(&page_tuples[j], slot, buf);
						FormIndexDatum(indexInfo, slot, estate, &key_val, &key_null);
						merkle_hash_datum(&route_hasher,
										  buildstate->key_headers[k],
										  key_val,
										  key_null,
										  buildstate->key_kinds[k],
										  &buildstate->key_send_functions[k]);
					}
				}

				blake3_hasher_finalize(&route_hasher, page_routes[j].route_digest, MERKLE_HASH_BYTES);
				page_routes[j].static_route_value = pg_bswap64(*((uint64 *) page_routes[j].route_digest));
				page_routes[j].partition_id = (int) (page_routes[j].static_route_value % (uint64) buildstate->num_partitions);
			}
			INSTR_TIME_SET_CURRENT(t_sub_end);
			INSTR_TIME_ACCUM_DIFF(rhash_accum, t_sub_end, t_sub_start);

			/* 4. Row Attribute Hashing */
			INSTR_TIME_SET_CURRENT(t_sub_start);
			for (j = 0; j < ntup; j++)
			{
				blake3_hasher tuple_hasher = buildstate->base_tuple_hasher;
				int i;

				for (i = 0; i < tupdesc->natts; i++)
				{
					Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

					if (attr->attisdropped)
						continue;

					merkle_hash_datum(&tuple_hasher,
									  buildstate->attr_headers[i],
									  page_row_values[j][i],
									  page_row_isnull[j][i],
									  buildstate->attr_kinds[i],
									  &buildstate->send_functions[i]);
				}

				blake3_hasher_finalize(&tuple_hasher, page_hashes[j].data, MERKLE_HASH_BYTES);
			}
			INSTR_TIME_SET_CURRENT(t_sub_end);
			INSTR_TIME_ACCUM_DIFF(thash_accum, t_sub_end, t_sub_start);

			/* 5. In-Memory Chunk Array Append */
			INSTR_TIME_SET_CURRENT(t_sub_start);
			for (j = 0; j < ntup; j++)
			{
				size_t chunk_idx = buildstate->num_entries / MERKLE_ENTRY_CHUNK_SIZE;
				size_t chunk_off = buildstate->num_entries % MERKLE_ENTRY_CHUNK_SIZE;
				MerkleTupleHashEntry *entry;

				if (chunk_idx >= (size_t) buildstate->num_chunks)
				{
					if (buildstate->num_chunks >= buildstate->max_chunks)
					{
						int new_max = buildstate->max_chunks ? buildstate->max_chunks * 2 : 16;

						if (buildstate->chunks == NULL)
							buildstate->chunks = (MerkleTupleHashEntry **)
								palloc((size_t) new_max * sizeof(MerkleTupleHashEntry *));
						else
							buildstate->chunks = (MerkleTupleHashEntry **)
								repalloc(buildstate->chunks,
										 (size_t) new_max * sizeof(MerkleTupleHashEntry *));
						buildstate->max_chunks = new_max;
					}
					buildstate->chunks[buildstate->num_chunks] = (MerkleTupleHashEntry *)
						MemoryContextAlloc(TopTransactionContext,
										   (size_t) MERKLE_ENTRY_CHUNK_SIZE * sizeof(MerkleTupleHashEntry));
					buildstate->num_chunks++;
				}

				entry = &buildstate->chunks[chunk_idx][chunk_off];
				memcpy(entry->key_hash, page_routes[j].route_digest, 8);
				entry->partition_id = (uint32) page_routes[j].partition_id;
				memcpy(&entry->tuple_hash, &page_hashes[j], sizeof(MerkleHash));
				buildstate->num_entries++;
				buildstate->indtuples += 1;
			}
			INSTR_TIME_SET_CURRENT(t_sub_end);
			INSTR_TIME_ACCUM_DIFF(app_accum, t_sub_end, t_sub_start);

			INSTR_TIME_SET_CURRENT(t_proc_end);
			INSTR_TIME_ACCUM_DIFF(hash_accum, t_proc_end, t_proc_start);

			LockBuffer(buf, BUFFER_LOCK_UNLOCK);
			ReleaseBuffer(buf);
		}

		g_single_pass_hash_time_ms = INSTR_TIME_GET_MILLISEC(hash_accum);
		g_prof_p1_vacuum_ms = ((double) INSTR_TIME_GET_MICROSEC(vac_accum)) / 1000.0;
		g_prof_p1_deform_ms = ((double) INSTR_TIME_GET_MICROSEC(def_accum)) / 1000.0;
		g_prof_p1_route_hash_ms = ((double) INSTR_TIME_GET_MICROSEC(rhash_accum)) / 1000.0;
		g_prof_p1_row_hash_ms = ((double) INSTR_TIME_GET_MICROSEC(thash_accum)) / 1000.0;
		g_prof_p1_chunk_append_ms = ((double) INSTR_TIME_GET_MICROSEC(app_accum)) / 1000.0;
	}

	FreeAccessStrategy(strategy);
	for (t = 0; t < max_tuples_per_page; t++)
	{
		pfree(page_row_values[t]);
		pfree(page_row_isnull[t]);
	}
	pfree(page_row_values);
	pfree(page_row_isnull);
	pfree(page_tuples);
	pfree(page_routes);
	pfree(page_hashes);

	if (need_unregister_snapshot)
		UnregisterSnapshot(snapshot);

	if (estate != NULL)
	{
		ExecDropSingleTupleTableSlot(slot);
		FreeExecutorState(estate);
		indexInfo->ii_ExpressionsState = NIL;
		indexInfo->ii_PredicateState = NULL;
	}

	return reltuples;
}

/*
 * merkle_build_callback() - Process one tuple during fallback generic index build
 */
static void
merkle_build_callback(Relation index,
					  ItemPointer tid,
					  Datum *values,
					  bool *isnull,
					  bool tupleIsAlive,
					  void *state)
{
	MerkleBuildState *buildstate = (MerkleBuildState *) state;
	TupleTableSlot *slot = buildstate->heapSlot;
	Datum *row_values;
	bool *row_isnull;

	if (!tupleIsAlive)
		return;

	if (!table_index_fetch_tuple(buildstate->heapFetch, tid,
								 SnapshotAny, slot,
								 NULL, NULL))
		return;

	slot_getallattrs(slot);
	row_values = slot->tts_values;
	row_isnull = slot->tts_isnull;

	merkle_process_tuple_direct(buildstate, values, isnull, row_values, row_isnull);
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
	IndexBuildResult   *result = NULL;
	MerkleBuildState	buildstate;
	double				reltuples;
	MerkleOptions	   *opts;
	MerkleRecoveryStatusData recovery_status;
	MerkleEntryArray	ea;

	instr_time	start_total, end_total;
	instr_time	start_phase1, end_phase1;
	instr_time	start_phase2, end_phase2;
	instr_time	start_phase3, end_phase3;
	double		p1_ms = 0, p2_ms = 0, p3_ms = 0, total_ms = 0;

	INSTR_TIME_SET_CURRENT(start_total);
	INSTR_TIME_SET_CURRENT(start_phase1);
	g_single_pass_hash_time_ms = 0.0;
	g_phase3_slice_scan_ms = 0.0;
	g_phase3_tree_build_ms = 0.0;
	g_phase3_catalog_flush_ms = 0.0;

	buildstate.chunks = NULL;
	buildstate.num_chunks = 0;
	buildstate.max_chunks = 0;
	buildstate.num_entries = 0;
	MemSet(&ea, 0, sizeof(ea));
	MemSet(&recovery_status, 0, sizeof(recovery_status));

    PG_TRY();
    {
	if (heapRel->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT ||
		indexRel->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("crash-safe Merkle indexes require a permanent logged table"),
				 errhint("Use a logged table; TEMP and UNLOGGED Merkle indexes are not supported.")));

	if (merkle_has_staged_delta())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot build or reindex a Merkle index after table changes in the same transaction"),
				 errhint("Commit the table changes and synchronize Merkle recovery before rebuilding the index.")));
	merkle_get_recovery_status(&recovery_status);
	if (recovery_status.state != MERKLE_STATE_READY)
	{
		if ((recovery_status.state == MERKLE_STATE_INVALID ||
			 recovery_status.state == MERKLE_STATE_REBUILD_REQUIRED) &&
			recovery_status.applied_seq == recovery_status.target_seq)
		{
			merkle_mark_recovery_state(MERKLE_STATE_READY, NULL);
			recovery_status.state = MERKLE_STATE_READY;
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("cannot build or reindex a Merkle index while committed deltas are pending"),
					 errdetail("state=%d applied_seq=%llu target_seq=%llu",
							   (int) recovery_status.state,
							   (unsigned long long) recovery_status.applied_seq,
							   (unsigned long long) recovery_status.target_seq),
					 errhint("Run REINDEX on the existing Merkle index after the committed prefix is caught up.")));
	}
    opts = merkle_get_options(indexRel);
    
    {
        List       *indexList;
        ListCell   *lc;
        Oid         currentIndexOid = RelationGetRelid(indexRel);

        indexList = RelationGetIndexList(heapRel);
        foreach(lc, indexList)
        {
            Oid         indexOid = lfirst_oid(lc);
            Relation    otherIndexRel;

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
    
    buildstate.fanout = opts->fanout;
	buildstate.split_threshold = opts->split_threshold;
	buildstate.merge_threshold = opts->merge_threshold;
	buildstate.num_partitions = opts->num_partitions;
	buildstate.bits_per_split = merkle_bits_per_split_for_fanout(buildstate.fanout);

	merkle_init_tree(indexRel, RelationGetRelid(heapRel), opts,
					 recovery_status.managed ? recovery_status.applied_seq : 0);

    pfree(opts);
    
    buildstate.indexRel = indexRel;
    buildstate.heapRel = heapRel;
    buildstate.indtuples = 0;
    buildstate.nkeys = indexInfo->ii_NumIndexKeyAttrs;
	buildstate.heapFetch = NULL;
	buildstate.heapSlot = NULL;
	buildstate.tupdesc = RelationGetDescr(heapRel);
	buildstate.send_functions = (FmgrInfo *) palloc0(buildstate.tupdesc->natts * sizeof(FmgrInfo));
	buildstate.attr_kinds = (MerkleAttrKind *) palloc0(buildstate.tupdesc->natts * sizeof(MerkleAttrKind));
	buildstate.attr_headers = (uint8 (*)[12]) palloc0(buildstate.tupdesc->natts * 12);
	for (int att = 0; att < buildstate.tupdesc->natts; att++)
	{
		Form_pg_attribute attr = TupleDescAttr(buildstate.tupdesc, att);
		if (!attr->attisdropped)
		{
			Oid typsend;
			bool typisvarlena;
			uint32 v1, v2, v3;

			buildstate.attr_kinds[att] = merkle_resolve_attr_kind(attr->atttypid);
			getTypeBinaryOutputInfo(attr->atttypid, &typsend, &typisvarlena);
			fmgr_info(typsend, &buildstate.send_functions[att]);

			v1 = (uint32) attr->attnum;
			v2 = (uint32) attr->atttypid;
			v3 = (uint32) attr->atttypmod;

			buildstate.attr_headers[att][0] = (uint8) (v1 >> 24);
			buildstate.attr_headers[att][1] = (uint8) (v1 >> 16);
			buildstate.attr_headers[att][2] = (uint8) (v1 >> 8);
			buildstate.attr_headers[att][3] = (uint8) v1;

			buildstate.attr_headers[att][4] = (uint8) (v2 >> 24);
			buildstate.attr_headers[att][5] = (uint8) (v2 >> 16);
			buildstate.attr_headers[att][6] = (uint8) (v2 >> 8);
			buildstate.attr_headers[att][7] = (uint8) v2;

			buildstate.attr_headers[att][8] = (uint8) (v3 >> 24);
			buildstate.attr_headers[att][9] = (uint8) (v3 >> 16);
			buildstate.attr_headers[att][10] = (uint8) (v3 >> 8);
			buildstate.attr_headers[att][11] = (uint8) v3;
		}
	}

	{
		TupleDesc index_tupdesc = RelationGetDescr(indexRel);
		buildstate.key_headers = (uint8 (*)[12]) palloc0(buildstate.nkeys * 12);
		buildstate.key_kinds = (MerkleAttrKind *) palloc0(buildstate.nkeys * sizeof(MerkleAttrKind));
		buildstate.key_send_functions = (FmgrInfo *) palloc0(buildstate.nkeys * sizeof(FmgrInfo));
		for (int key = 0; key < buildstate.nkeys; key++)
		{
			Form_pg_attribute attr = TupleDescAttr(index_tupdesc, key);
			if (!attr->attisdropped)
			{
				Oid typsend;
				bool typisvarlena;
				uint32 v1 = (uint32) (key + 1);
				uint32 v2 = (uint32) attr->atttypid;
				uint32 v3 = (uint32) attr->atttypmod;

				buildstate.key_kinds[key] = merkle_resolve_attr_kind(attr->atttypid);
				getTypeBinaryOutputInfo(attr->atttypid, &typsend, &typisvarlena);
				fmgr_info(typsend, &buildstate.key_send_functions[key]);

				buildstate.key_headers[key][0] = (uint8) (v1 >> 24);
				buildstate.key_headers[key][1] = (uint8) (v1 >> 16);
				buildstate.key_headers[key][2] = (uint8) (v1 >> 8);
				buildstate.key_headers[key][3] = (uint8) v1;

				buildstate.key_headers[key][4] = (uint8) (v2 >> 24);
				buildstate.key_headers[key][5] = (uint8) (v2 >> 16);
				buildstate.key_headers[key][6] = (uint8) (v2 >> 8);
				buildstate.key_headers[key][7] = (uint8) v2;

				buildstate.key_headers[key][8] = (uint8) (v3 >> 24);
				buildstate.key_headers[key][9] = (uint8) (v3 >> 16);
				buildstate.key_headers[key][10] = (uint8) (v3 >> 8);
				buildstate.key_headers[key][11] = (uint8) v3;
			}
		}
	}

	{
		static const uint8 route_magic[] = {'A', 'R', 'I', 'A', 'R', 'O', 'U', 'T'};
		static const uint8 tuple_magic[] = {'A', 'R', 'I', 'A', 'M', 'R', 'K', 'L'};
		uint32 live_attributes = 0;

		for (int i = 0; i < buildstate.tupdesc->natts; i++)
			if (!TupleDescAttr(buildstate.tupdesc, i)->attisdropped)
				live_attributes++;

		blake3_hasher_init(&buildstate.base_route_hasher);
		blake3_hasher_update(&buildstate.base_route_hasher, route_magic, sizeof(route_magic));
		merkle_hash_uint32_local(&buildstate.base_route_hasher, MERKLE_ROUTE_FORMAT_VERSION);
		merkle_hash_uint32_local(&buildstate.base_route_hasher, (uint32) buildstate.nkeys);

		blake3_hasher_init(&buildstate.base_tuple_hasher);
		blake3_hasher_update(&buildstate.base_tuple_hasher, tuple_magic, sizeof(tuple_magic));
		merkle_hash_uint32_local(&buildstate.base_tuple_hasher, MERKLE_ROW_HASH_FORMAT_VERSION);
		merkle_hash_uint32_local(&buildstate.base_tuple_hasher, live_attributes);
	}

	if (heapRel->rd_rel->relam == HEAP_TABLE_AM_OID)
	{
		reltuples = merkle_heapam_index_build_scan(heapRel, indexRel, indexInfo, &buildstate);
	}
	else
	{
		buildstate.heapFetch = table_index_fetch_begin(heapRel);
		buildstate.heapSlot = table_slot_create(heapRel, NULL);

		reltuples = table_index_build_scan(heapRel, indexRel, indexInfo,
										   true,   /* allow_sync */
										   false,  /* progress */
										   merkle_build_callback,
										   (void *) &buildstate,
										   NULL);  /* use heap scan */

		table_index_fetch_end(buildstate.heapFetch);
		buildstate.heapFetch = NULL;
		ExecDropSingleTupleTableSlot(buildstate.heapSlot);
		buildstate.heapSlot = NULL;
	}

	if (buildstate.attr_headers)
	{
		pfree(buildstate.attr_headers);
		buildstate.attr_headers = NULL;
	}
	if (buildstate.key_headers)
	{
		pfree(buildstate.key_headers);
		buildstate.key_headers = NULL;
	}
	if (buildstate.send_functions)
	{
		pfree(buildstate.send_functions);
		buildstate.send_functions = NULL;
	}
	if (buildstate.key_send_functions)
	{
		pfree(buildstate.key_send_functions);
		buildstate.key_send_functions = NULL;
	}

	INSTR_TIME_SET_CURRENT(end_phase1);
	INSTR_TIME_SUBTRACT(end_phase1, start_phase1);
	p1_ms = INSTR_TIME_GET_MILLISEC(end_phase1);

	merkle_emit_build_nodes_report(indexRel, &buildstate);

	INSTR_TIME_SET_CURRENT(start_phase2);
	ea = merkle_prepare_sorted_entries(&buildstate);

	/* REINDEX must replace, rather than overlay, the previous dynamic
	 * geometry. This also removes pre-partition-format rows left by an
	 * upgraded cluster before the first partitioned rebuild. */
	if (SPI_connect() == SPI_OK_CONNECT)
	{
		Oid clear_types[1] = {OIDOID};
		Datum clear_values[1] = {ObjectIdGetDatum(RelationGetRelid(indexRel))};

		SPI_execute_with_args(
			"DELETE FROM ariabc_internal.merkle_node WHERE index_oid = $1",
			1, clear_types, clear_values, NULL, false, 0);

		if (SPI_tuptable != NULL)
			SPI_freetuptable(SPI_tuptable);
		SPI_finish();
	}

	INSTR_TIME_SET_CURRENT(end_phase2);
	INSTR_TIME_SUBTRACT(end_phase2, start_phase2);
	p2_ms = INSTR_TIME_GET_MILLISEC(end_phase2);

	INSTR_TIME_SET_CURRENT(start_phase3);
	g_phase3_tree_build_ms = 0.0;
	g_phase3_slice_scan_ms = 0.0;
	g_phase3_catalog_flush_ms = 0.0;

	{
		Oid index_oid = RelationGetRelid(indexRel);
		MerkleCatalogFlushState *flush_state;
		instr_time t_fl_init_start, t_fl_init_end;
		size_t cur_entry = 0;

		INSTR_TIME_SET_CURRENT(t_fl_init_start);
		flush_state = merkle_catalog_flush_init(index_oid, BULK_INSERT_BATCH);
		INSTR_TIME_SET_CURRENT(t_fl_init_end);
		INSTR_TIME_SUBTRACT(t_fl_init_end, t_fl_init_start);
		g_phase3_catalog_flush_ms += ((double) INSTR_TIME_GET_MICROSEC(t_fl_init_end) / 1000.0);

		/* Each partition owns an independent dynamic tree rooted at
		 * (partition_id, node_id=0, prefix_len=0). Entries are sorted by
		 * partition_id, so each partition is a contiguous slice. */
		for (int partition_id = 0;
			 partition_id < buildstate.num_partitions;
			 partition_id++)
		{
			size_t start;
			size_t count;
			uint8 zero_node_id[8] = {0};
			instr_time t_slice_start, t_slice_end;

			INSTR_TIME_SET_CURRENT(t_slice_start);
			while (cur_entry < ea.num_entries &&
				   merkle_entry_at(&ea, cur_entry)->partition_id < (uint32) partition_id)
				cur_entry++;

			start = cur_entry;
			while (cur_entry < ea.num_entries &&
				   merkle_entry_at(&ea, cur_entry)->partition_id == (uint32) partition_id)
				cur_entry++;
			count = cur_entry - start;

			INSTR_TIME_SET_CURRENT(t_slice_end);
			INSTR_TIME_SUBTRACT(t_slice_end, t_slice_start);
			g_phase3_slice_scan_ms += ((double) INSTR_TIME_GET_MICROSEC(t_slice_end) / 1000.0);

			if (count <= (size_t) buildstate.split_threshold)
			{
				MerkleHash partition_hash;
				MerkleNodeRecord r;
				instr_time t_fl_start, t_fl_end;

				merkle_hash_zero(&partition_hash);
				for (size_t k = 0; k < count; k++)
					merkle_hash_xor(&partition_hash,
									&merkle_entry_at(&ea, start + k)->tuple_hash);

				r.partition_id = partition_id;
				memcpy(r.node_id, zero_node_id, 8);
				r.prefix_len = 0;
				r.is_leaf = true;
				r.tuple_count = (int64) count;
				memcpy(&r.hash, &partition_hash, sizeof(MerkleHash));

				INSTR_TIME_SET_CURRENT(t_fl_start);
				merkle_catalog_flush_add(flush_state, index_oid, &r);
				INSTR_TIME_SET_CURRENT(t_fl_end);
				INSTR_TIME_SUBTRACT(t_fl_end, t_fl_start);
				g_phase3_catalog_flush_ms += ((double) INSTR_TIME_GET_MICROSEC(t_fl_end) / 1000.0);
			}
			else
			{
				MerkleBulkNodeSet bulk;
				MerkleHash computed_root_hash;
				int est_nodes = Max(1024, (int) ((count / buildstate.split_threshold) * 3));

				bulk_node_set_init(&bulk, est_nodes);
				PG_TRY();
				{
					instr_time	t_tb_start, t_tb_end;
					instr_time	t_fl_start, t_fl_end;

					INSTR_TIME_SET_CURRENT(t_tb_start);
					computed_root_hash = merkle_build_tree_pass1(
						&bulk, partition_id, zero_node_id, 0,
						&ea, start, count,
						buildstate.fanout, buildstate.bits_per_split,
						buildstate.split_threshold, MAX_PREFIX_LEN);
					INSTR_TIME_SET_CURRENT(t_tb_end);
					INSTR_TIME_SUBTRACT(t_tb_end, t_tb_start);
					g_phase3_tree_build_ms += ((double) INSTR_TIME_GET_MICROSEC(t_tb_end) / 1000.0);

					bulk_node_set_add(&bulk, partition_id, zero_node_id, 0, false,
									  (int64) count, &computed_root_hash);

					INSTR_TIME_SET_CURRENT(t_fl_start);
					for (int k = 0; k < bulk.num_records; k++)
						merkle_catalog_flush_add(flush_state, index_oid, &bulk.records[k]);
					INSTR_TIME_SET_CURRENT(t_fl_end);
					INSTR_TIME_SUBTRACT(t_fl_end, t_fl_start);
					g_phase3_catalog_flush_ms += ((double) INSTR_TIME_GET_MICROSEC(t_fl_end) / 1000.0);
				}
				PG_CATCH();
				{
					if (bulk.records)
					{
						pfree(bulk.records);
						bulk.records = NULL;
					}
					PG_RE_THROW();
				}
				PG_END_TRY();
				if (bulk.records)
					pfree(bulk.records);
			}
		}

		{
			instr_time t_fl_fin_start, t_fl_fin_end;

			INSTR_TIME_SET_CURRENT(t_fl_fin_start);
			merkle_catalog_flush_finish(flush_state);
			INSTR_TIME_SET_CURRENT(t_fl_fin_end);
			INSTR_TIME_SUBTRACT(t_fl_fin_end, t_fl_fin_start);
			g_phase3_catalog_flush_ms += ((double) INSTR_TIME_GET_MICROSEC(t_fl_fin_end) / 1000.0);
		}
	}

	INSTR_TIME_SET_CURRENT(end_phase3);
	INSTR_TIME_SUBTRACT(end_phase3, start_phase3);
	p3_ms = INSTR_TIME_GET_MILLISEC(end_phase3);

	INSTR_TIME_SET_CURRENT(end_total);
	INSTR_TIME_SUBTRACT(end_total, start_total);
	total_ms = INSTR_TIME_GET_MILLISEC(end_total);

	{
		double scan_ms = p1_ms - g_single_pass_hash_time_ms;

		elog(NOTICE, "[MERKLE_PROFILER] 1M Tuple Merkle Build Complete: Total=%.2f ms | Phase1_HeapScan=%.2f ms (%.1f%%) [SinglePass_HashAndRoute=%.2f ms, PureHeapScan=%.2f ms] [P1_Breakdown: VacCheck=%.2f ms, Deform=%.2f ms, RouteHash=%.2f ms, RowHash=%.2f ms, ChunkAppend=%.2f ms] | Phase2_InMemorySort=%.2f ms (%.1f%%) | Phase3_CatalogFlush=%.2f ms (%.1f%%) [SliceScan=%.2f ms, TreePass1=%.2f ms, CatalogFlush=%.2f ms]",
			 total_ms, p1_ms, (p1_ms / total_ms) * 100.0,
			 g_single_pass_hash_time_ms, scan_ms < 0 ? 0.0 : scan_ms,
			 g_prof_p1_vacuum_ms, g_prof_p1_deform_ms, g_prof_p1_route_hash_ms, g_prof_p1_row_hash_ms, g_prof_p1_chunk_append_ms,
			 p2_ms, (p2_ms / total_ms) * 100.0,
			 p3_ms, (p3_ms / total_ms) * 100.0,
			 g_phase3_slice_scan_ms, g_phase3_tree_build_ms, g_phase3_catalog_flush_ms);
	}

	if (ea.chunks)
	{
		for (int c = 0; c < ea.num_chunks; c++)
			pfree(ea.chunks[c]);
		pfree(ea.chunks);
		ea.chunks = NULL;
	}

    result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
    result->heap_tuples = reltuples;
    result->index_tuples = buildstate.indtuples;
    }
    PG_CATCH();
    {
		if (ea.chunks)
		{
			for (int c = 0; c < ea.num_chunks; c++)
				pfree(ea.chunks[c]);
			pfree(ea.chunks);
			ea.chunks = NULL;
		}
		if (buildstate.chunks)
		{
			for (int c = 0; c < buildstate.num_chunks; c++)
				pfree(buildstate.chunks[c]);
			pfree(buildstate.chunks);
			buildstate.chunks = NULL;
		}
		if (buildstate.heapFetch != NULL)
			table_index_fetch_end(buildstate.heapFetch);
		if (buildstate.heapSlot != NULL)
			ExecDropSingleTupleTableSlot(buildstate.heapSlot);
		if (buildstate.attr_headers)
			pfree(buildstate.attr_headers);
		if (buildstate.key_headers)
			pfree(buildstate.key_headers);
		if (buildstate.attr_kinds)
			pfree(buildstate.attr_kinds);
		if (buildstate.key_kinds)
			pfree(buildstate.key_kinds);
		if (buildstate.send_functions)
			pfree(buildstate.send_functions);
		if (buildstate.key_send_functions)
			pfree(buildstate.key_send_functions);
        PG_RE_THROW();
    }
    PG_END_TRY();

    return result;
}

void
merkleBuildempty(Relation indexRel)
{
    Page        metapage;
    MerkleMetaPageData *meta;
    MerkleOptions *opts;
    int         fanout;
	MerkleRecoveryStatusData recovery_status;

	if (indexRel->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("crash-safe Merkle indexes cannot be created for TEMP or UNLOGGED relations"),
				 errhint("Use a permanent logged table.")));

	MemSet(&recovery_status, 0, sizeof(recovery_status));
	merkle_get_recovery_status(&recovery_status);
	if (recovery_status.managed &&
		recovery_status.state != MERKLE_STATE_READY)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot initialize a Merkle index while recovery is not READY"),
				 errdetail("state=%d applied_seq=%llu target_seq=%llu",
						   (int) recovery_status.state,
						   (unsigned long long) recovery_status.applied_seq,
						   (unsigned long long) recovery_status.target_seq)));

    metapage = (Page) palloc(BLCKSZ);
    PageInit(metapage, BLCKSZ, 0);
    
    opts = merkle_get_options(indexRel);
    fanout = opts->fanout;

    if (fanout < 2 || fanout > 1024)
        fanout = MERKLE_DEFAULT_FANOUT;

    meta = MerklePageGetMeta(metapage);
    meta->version = MERKLE_VERSION;
    meta->heapRelid = InvalidOid;
    meta->fanout = fanout;
    meta->split_threshold = opts->split_threshold;
    meta->merge_threshold = opts->merge_threshold;
	meta->num_partitions = opts->num_partitions;
	meta->routeFormatVersion = MERKLE_ROUTE_FORMAT_VERSION;
	meta->rowHashFormatVersion = MERKLE_ROW_HASH_FORMAT_VERSION;
	meta->baselineApplySeq = recovery_status.managed ?
		recovery_status.applied_seq : 0;

    pfree(opts);
    
    RelationOpenSmgr(indexRel);

    PageSetChecksumInplace(metapage, MERKLE_METAPAGE_BLKNO);
    smgrwrite(indexRel->rd_smgr, INIT_FORKNUM, MERKLE_METAPAGE_BLKNO,
              (char *) metapage, true);
    log_newpage(&indexRel->rd_smgr->smgr_rnode.node, INIT_FORKNUM,
                MERKLE_METAPAGE_BLKNO, metapage, true);
    
    smgrimmedsync(indexRel->rd_smgr, INIT_FORKNUM);
    
    pfree(metapage);
}
