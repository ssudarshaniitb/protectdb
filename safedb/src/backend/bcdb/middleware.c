//
// Created by Chris Liu on 6/5/2020.
//
// ---------------------------------------------------------------------------
//  middleware.c -- BCDB SQL-facing middleware layer
// ---------------------------------------------------------------------------
//
//  This file is the entry point for every BCDB SQL function that submits
//  transactions or blocks into the deterministic (DT) execution engine.
//  PostgreSQL backends call into the symbols defined here when client code
//  invokes the BCDB SQL helpers (bcdb_tx_submit, bcdb_block_submit, etc.)
//  exposed via the C-language function bindings.
//
//  Pipeline overview (deterministic block path, active production flow):
//    1. Frontend submits a JSON block of transactions via SQL.
//    2. parse_block_with_txs() reserves a contiguous tx-id range from the
//       sentinel block (block id 1) and materialises each tx in shared
//       memory using create_tx().
//    3. Each tx is pushed onto a worker queue via tx_queue_insert().
//    4. Backend workers (worker.c) drain those queues, execute the SQL
//       deterministically, and publish results into the sentinel block's
//       result ring (one slot per tx, indexed by tx_id % ring_slots).
//    5. The submitting backend waits until every per-tx slot is published
//       (either by polling the contiguous committed watermark or by
//       scanning the block-local slots), then formats and returns a
//       newline-delimited completion payload back to the SQL caller.
//
//  Key correctness invariants enforced in this file:
//    - tx_id assignment is atomic across concurrent backends so block
//      members have stable, unique, ordered identifiers.
//    - block-submit txs become worker-visible in monotonically increasing
//      block id order, even when PostgreSQL backends parse later blocks first.
//    - A result slot is only read when result_committed_txid[slot] equals
//      the exact tx_id being fetched; otherwise the slot has been recycled
//      by a later tx and the data would be wrong.
//    - The sentinel block (id 1) owns runtime metadata (worker count,
//      result ring) and is never garbage-collected by block_cleaning_dt().
//
//  Legacy code paths (single-tx submit, burst submit, hash-addressed wait,
//  cleaning) remain for backward compatibility with older tests and tools
//  but are not part of the active distributed YCSB/TPCC pipelines.
// ---------------------------------------------------------------------------

#include "bcdb/shm_transaction.h"
#include "access/merkle.h"
#include "access/xact.h"
#include "bcdb/worker.h"
#include "bcdb/middleware.h"
#include "bcdb/shm_block.h"
#include "bcdb/worker_controller.h"
#include "libpq/libpq.h"
#include "libpq-fe.h"
#include "storage/condition_variable.h"
#include "pgstat.h"
#include "utils/memutils.h"
#include "storage/lwlock.h"
#include "storage/predicate.h"
#include "bcdb/globals.h"
#include "lib/stringinfo.h"
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>		/* for errno = 0 / ERANGE in strict strtoull parsing */

/*
 * Silence ad-hoc stdout debug prints in deterministic middleware.
 *
 * Many helpers in this file historically used raw printf() for ad-hoc
 * debugging.  In a live PostgreSQL backend those writes go to the same fd
 * as the frontend protocol stream and can corrupt client sessions.  We
 * redefine printf as a no-op so any forgotten debug call is compiled away
 * without having to touch every site.  Use ereport(LOG, ...) for anything
 * that needs to be visible at runtime.
 */
#undef printf
#define printf(...) ((void) 0)

/* ----------------------------------------------------------------------
 *  File-scope state
 * ----------------------------------------------------------------------
 *  bcdb_middleware_context : long-lived memory context reused across
 *                            bcdb_init() calls so restore scripts and
 *                            benchmark loops do not leak one context per
 *                            invocation.
 *  tx_num                  : monotonic tx-id counter for the legacy
 *                            single-tx submit path (bcdb_tx_submit).
 *  blocksize               : effective worker/queue count for this backend.
 *                            Despite the name, this is NOT a tx batch size;
 *                            the SQL parameter retains the historical name.
 *  numTxBurst / burstTime  : legacy controls used by submit_block2() to
 *                            throttle the enqueue rate during experiments.
 *  start_time              : wall-clock timestamp captured at init, used
 *                            only for the total-throughput LOG line.
 *  tx_id_counter           : unused holdover from the original sketch
 *                            (predates the atomic counter in bcdb).
 * ---------------------------------------------------------------------- */
MemoryContext bcdb_middleware_context;
int32         tx_num = 0;
int32         blocksize = 0;
int32         numTxBurst = 0;
int32         burstTime = 0;
uint64        start_time;
static int  tx_id_counter = 0; /* legacy: not used by deterministic path */

static uint8
hex_val(char c)
{
	if (c >= '0' && c <= '9') return c - '0';
	if (c >= 'a' && c <= 'f') return 10 + c - 'a';
	if (c >= 'A' && c <= 'F') return 10 + c - 'A';
	return 0;
}

static void
decode_hex(const char *hex, uint8 *out, int out_len)
{
	int i;
	if (!hex)
	{
		memset(out, 0, out_len);
		return;
	}
	for (i = 0; i < out_len; ++i)
	{
		if (hex[2 * i] == '\0' || hex[2 * i + 1] == '\0')
		{
			memset(out + i, 0, out_len - i);
			break;
		}
		out[i] = (hex_val(hex[2 * i]) << 4) | hex_val(hex[2 * i + 1]);
	}
}

static bool
is_valid_hex_64(const char *str)
{
	int i;
	if (str == NULL || strlen(str) != 64)
		return false;
	for (i = 0; i < 64; i++)
	{
		char c = str[i];
		/* Require strictly lowercase hex: uppercase is rejected so that
		 * the server epoch check (which requires lowercase) is consistent. */
		if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')))
			return false;
	}
	return true;
}

/*
 * Backend-local copy of the immutable per-tx fields needed after enqueue.
 *
 * DT workers own the lifecycle of BCDBShmXact entries once tx_queue_insert()
 * hands them off, and block_cleaning_dt() can reclaim old block headers as the
 * pipeline advances.  Submitters therefore must not keep dereferencing
 * block->txs[] while waiting for or formatting results.
 */
typedef struct BCDBBlockResultRef
{
	BCTxID tx_id;
	char   hash[TX_HASH_SIZE];
	bool   raft_ledger_enabled;
	uint64 raft_log_index;
	uint32 raft_item_ordinal;
	uint8  raft_epoch_id[BCDB_RAFT_DIGEST_BYTES];
} BCDBBlockResultRef;

typedef struct MerkleRaftTargetRef
{
	uint8  epoch_id[BCDB_RAFT_DIGEST_BYTES];
	uint64 raft_log_index;
	uint32 max_item_ordinal;
} MerkleRaftTargetRef;

/* ----------------------------------------------------------------------
 *  Forward declarations for file-local helpers.
 *
 *  Grouped by role:
 *    Parsing/attach   : parse_tx, parse_block_with_txs,
 *                       bcdb_middleware_attach_tx_to_block,
 *                       append_hex_encoded
 *    Worker sizing    : bcdb_select_worker_count
 *    Result indexing  : bcdb_result_slot_for_txid
 *    Enqueue ordering : bcdb_wait_for_block_enqueue_turn,
 *                       bcdb_advance_block_enqueue_turn
 *    Wait primitives  : bcdb_wait_until_committed,
 *                       bcdb_wait_until_slot_ready,
 *                       bcdb_wait_until_block_slots_ready
 *    Tunable env vars : bcdb_block_profile_enabled,
 *                       bcdb_block_return_actual_results_enabled,
 *                       bcdb_block_wait_watermark_enabled,
 *                       bcdb_decouple_workers_enabled,
 *                       bcdb_block_enqueue_yield_every
 *    Misc utilities   : bcdb_uint64_cmp
 * ---------------------------------------------------------------------- */
static BCDBShmXact *parse_tx(const char* json);
static void bcdb_middleware_attach_tx_to_block(BCDBShmXact *tx, BCBlock *block);
static BCBlock *parse_block_with_txs(const char *json);
static void append_hex_encoded(StringInfo out, const char *input);
static int32 bcdb_select_worker_count(int32 requested);
static inline int bcdb_result_slot_for_txid(BCTxID tx_id);
static inline void bcdb_wait_for_block_enqueue_turn(BCBlockID block_id);
static inline void bcdb_advance_block_enqueue_turn(BCBlockID block_id);
static inline uint64 bcdb_wait_until_committed(BCTxID target_tx_id);
static inline uint64 bcdb_wait_until_slot_ready(BCTxID target_tx_id);
static inline uint64 bcdb_wait_until_block_slots_ready(const BCDBBlockResultRef *refs,
													  int num_tx,
													  BCBlockID block_id);
static bool bcdb_block_profile_enabled(void);
static bool bcdb_block_return_actual_results_enabled(void);
static bool bcdb_block_wait_watermark_enabled(void);
static bool bcdb_decouple_workers_enabled(void);
static int bcdb_block_enqueue_yield_every(void);
static int bcdb_uint64_cmp(const void *a, const void *b);

/*
 * bcdb_block_profile_enabled
 * --------------------------
 * Reads BCDB_BLOCK_PROFILE from the environment and caches the answer in a
 * function-local static (so the cost is one strcmp pass per backend, not per
 * block).  When enabled, bcdb_middleware_submit_block_results() records and
 * logs phase timings (parse / enqueue / wait / format) plus per-slot wait
 * percentiles.
 *
 * Accepted "off" values: unset, empty, "0", "false"/"FALSE", "no"/"NO".
 * Everything else is treated as on.
 */
static bool
bcdb_block_profile_enabled(void)
{
	static int enabled = -1; /* -1 = not yet evaluated */

	if (enabled < 0)
	{
		const char *v = getenv("BCDB_BLOCK_PROFILE");

		enabled = (v != NULL && v[0] != '\0' &&
				   strcmp(v, "0") != 0 &&
				   strcmp(v, "false") != 0 &&
				   strcmp(v, "FALSE") != 0 &&
				   strcmp(v, "no") != 0 &&
				   strcmp(v, "NO") != 0);
	}
	return enabled != 0;
}

/*
 * bcdb_block_return_actual_results_enabled
 * ----------------------------------------
 * Reads BCDB_BLOCK_RETURN_ACTUAL_RESULTS and caches the result.
 *
 * The deterministic block-submit path defaults to returning a "completion
 * receipt" (tx hash + newline) per tx rather than the full row payload.  The
 * post-run Merkle gate is what verifies state correctness across replicas;
 * the per-tx row text returned to the frontend can otherwise reflect local
 * worker timing on reads.  Setting this env var to a truthy value reinstates
 * the full hex-encoded payload for diagnostics.
 */
static bool
bcdb_block_return_actual_results_enabled(void)
{
	static int enabled = -1;

	if (enabled < 0)
	{
		const char *v = getenv("BCDB_BLOCK_RETURN_ACTUAL_RESULTS");

		enabled = (v != NULL && v[0] != '\0' &&
				   strcmp(v, "0") != 0 &&
				   strcmp(v, "false") != 0 &&
				   strcmp(v, "FALSE") != 0 &&
				   strcmp(v, "no") != 0 &&
				   strcmp(v, "NO") != 0);
	}
	return enabled != 0;
}

/*
 * bcdb_block_wait_watermark_enabled
 * ---------------------------------
 * Reads BCDB_BLOCK_WAIT_WATERMARK and caches the result.
 *
 * Selects between two equivalent wait strategies inside
 * bcdb_middleware_submit_block_results():
 *
 *   - default (off): scan every block-local result slot in a single loop
 *                    (bcdb_wait_until_block_slots_ready).
 *   - on           : wait only on the contiguous committed watermark for
 *                    the highest tx_id in the block; workers always
 *                    publish result_committed_txid BEFORE advancing
 *                    last_committed_tx_id, so this is correctness-equivalent.
 *
 * Watermark mode was not faster on the 4-node YCSB run, so it remains opt-in
 * for A/B testing.
 */
static bool
bcdb_block_wait_watermark_enabled(void)
{
	static int enabled = -1;

	if (enabled < 0)
	{
		const char *v = getenv("BCDB_BLOCK_WAIT_WATERMARK");

		enabled = (v != NULL && v[0] != '\0' &&
				   strcmp(v, "0") != 0 &&
				   strcmp(v, "false") != 0 &&
				   strcmp(v, "FALSE") != 0 &&
				   strcmp(v, "no") != 0 &&
				   strcmp(v, "NO") != 0);
	}
	return enabled != 0;
}

/*
 * bcdb_block_enqueue_yield_every
 * ------------------------------
 * Reads BCDB_BLOCK_ENQUEUE_YIELD_EVERY and caches the parsed int.
 *
 * If non-zero, the block submit loop calls pg_usleep(1) after every N
 * tx_queue_insert() operations.  This gives workers a chance to drain the
 * queue while a backend is still pushing large blocks, smoothing the
 * producer/consumer pipeline on heavily loaded CPUs.  Clamped to [0, 256];
 * 0 disables the yield entirely (default).
 */
static int
bcdb_block_enqueue_yield_every(void)
{
	static int cached = -1;

	if (cached < 0)
	{
		const char *v = getenv("BCDB_BLOCK_ENQUEUE_YIELD_EVERY");
		int parsed = 0;

		if (v != NULL && *v != '\0')
		{
			parsed = atoi(v);
			if (parsed < 0)
				parsed = 0;
			if (parsed > 256)
				parsed = 256;
		}
		cached = parsed;
	}
	return cached;
}

/*
 * bcdb_decouple_workers_enabled
 * -----------------------------
 * Reads BCDB_DECOUPLE_WORKERS and caches the result.
 *
 * When enabled, bcdb_middleware_init() ignores the SQL-supplied "block_size"
 * argument and uses bcdb_worker_count / BCDB_DEFAULT_WORKER_COUNT instead.
 * That decouples the SQL frontend's notion of batch sizing from the worker
 * pool sizing, which is useful when running benchmarks that vary one
 * dimension without restarting the cluster.
 */
static bool
bcdb_decouple_workers_enabled(void)
{
	static int enabled = -1;

	if (enabled < 0)
	{
		const char *v = getenv("BCDB_DECOUPLE_WORKERS");

		enabled = (v != NULL && v[0] != '\0' &&
				   strcmp(v, "0") != 0 &&
				   strcmp(v, "false") != 0 &&
				   strcmp(v, "FALSE") != 0 &&
				   strcmp(v, "no") != 0 &&
				   strcmp(v, "NO") != 0);
	}
	return enabled != 0;
}

/*
 * bcdb_uint64_cmp
 * ---------------
 * Standard ascending comparator for qsort() over an array of uint64.
 * Used by the profiling path to compute slot-wait percentiles.
 */
static int
bcdb_uint64_cmp(const void *a, const void *b)
{
	uint64 av = *(const uint64 *) a;
	uint64 bv = *(const uint64 *) b;

	if (av < bv)
		return -1;
	if (av > bv)
		return 1;
	return 0;
}

/*
 * bcdb_select_worker_count
 * ------------------------
 * Resolve the effective worker/queue count for this backend, applying a
 * fall-through chain of preference:
 *
 *   1. explicit caller request (the SQL `block_size` argument);
 *   2. the GUC-level default bcdb_worker_count if the caller passed <= 0;
 *   3. the compile-time BCDB_DEFAULT_WORKER_COUNT if both are missing;
 *   4. a hard floor of 1 to keep the worker list non-empty.
 *
 * Returning at least 1 ensures idle_worker_list_init() and the result ring
 * sizing logic always operate on a valid worker count.
 */
static int32
bcdb_select_worker_count(int32 requested)
{
	int32 workers = requested;

	if (workers <= 0)
		workers = bcdb_worker_count;
	if (workers <= 0)
		workers = BCDB_DEFAULT_WORKER_COUNT;
	if (workers <= 0)
		workers = 1;
	return workers;
}

/*
 * bcdb_result_slot_for_txid
 * -------------------------
 * Map a tx_id onto its slot index in the sentinel block's result ring.
 *
 * The result ring is a fixed-size circular buffer; slot ownership is
 * established by the worker writing result_committed_txid[slot] = tx_id
 * (RELEASE) after publishing the row payload, and verified by the reader
 * loading the same field (ACQUIRE) and comparing it to its expected tx_id.
 *
 * This helper MUST be used everywhere in the DT path (writers and readers)
 * because the result ring size is configurable independently of the worker
 * count -- a hard-coded modulus by blksize would silently disagree with the
 * ring and corrupt slot lookups.
 *
 * Guards against degenerate inputs:
 *   - clamps slots to >= 1 so the modulus is well-defined;
 *   - normalises negative results (BCTxID is signed) so the index is
 *     always within [0, slots).
 */
static inline int
bcdb_result_slot_for_txid(BCTxID tx_id)
{
	int slots = bcdb_get_runtime_result_ring_slots();
	int idx;

	if (slots <= 0)
		slots = 1;
	idx = tx_id % slots;
	if (idx < 0)
		idx += slots;
	return idx;
}

/*
 * bcdb_wait_for_block_enqueue_turn / bcdb_advance_block_enqueue_turn
 * ------------------------------------------------------------------
 * Enforce worker-visible block order for parallel block-submit backends.
 *
 * The gateway may have several PostgreSQL connections in flight.  If backend
 * B parses/enqueues block 3 before backend A enqueues block 2, a worker queue
 * can contain tx 769 before tx 257.  The worker that pops 769 waits for 768,
 * but 257 may be trapped behind 769 in that same FIFO queue -- a deterministic
 * deadlock.  We therefore gate only the enqueue phase by block id:
 *
 *   parse block N        may happen concurrently
 *   enqueue block N      must happen in block-id order
 *   wait/format block N  may happen concurrently after enqueue
 */
static inline void
bcdb_wait_for_block_enqueue_turn(BCBlockID block_id)
{
	int spins = 0;
	int poll_us = 0;
	uint64 wait_start_us = bcdb_get_time();
	uint64 next_warn_us = wait_start_us + 5000000; /* first warning at +5 s */
	bool   active_wait_registered = false;
	bool   did_wait = false;
	const bool collect_gate_stats = unlikely(bcdb_gate_telemetry_enabled);

	if (collect_gate_stats)
	{
		SHARD_INC(block_enqueue_turn_calls);
	}

	if (block_id < BCDB_FIRST_SUBMIT_BLOCK_ID)
		return;

	if (collect_gate_stats)
	{
		BCBlockID expected = __atomic_load_n(&block_meta->next_enqueue_block_id,
											 __ATOMIC_ACQUIRE);
		if (expected < block_id)
		{
			gate_stats_begin_wait(BCDB_GATE_PHASE_ENQUEUE, -1, block_id);
			active_wait_registered = true;
		}
	}

	for (;;)
	{
		BCBlockID expected;

		expected = __atomic_load_n(&block_meta->next_enqueue_block_id,
								   __ATOMIC_ACQUIRE);
		if (expected == block_id)
			break;
		if (expected > block_id)
			break;
		if ((block_id - expected) > MAX_NUM_BLOCKS)
		{
			BCBlock *expected_block = get_block_by_id(expected, false);

			/*
			 * Recovery for stale enqueue-gate state.  The normal path advances
			 * one block at a time, but an independent reset of BCDB metadata can
			 * leave next_enqueue_block_id back at the first submit id while the
			 * Aria executor is still using its monotonic per-server block ids.
			 * If the expected block is already outside the entire block-pool
			 * retention window and no header exists for it, waiting is permanent.
			 */
			if (expected_block == NULL &&
				__sync_bool_compare_and_swap(&block_meta->next_enqueue_block_id,
											 expected,
											 block_id))
			{
				ereport(LOG,
						(errmsg("BCDB block enqueue turn recovered stale gap: block_id=%d expected_block_id=%d max_blocks=%d",
								(int) block_id, (int) expected, (int) MAX_NUM_BLOCKS)));
				break;
			}
		}

		CHECK_FOR_INTERRUPTS();

		{
			uint64 now_us = bcdb_get_time();
			if (now_us >= next_warn_us)
			{
				ereport(LOG,
						(errmsg("[BCDB_HANG] block_enqueue_order_wait_stuck pid=%d block_id=%d expected_block_id=%d waited_us=%lu poll_us=%d spins=%d",
								(int) getpid(), (int) block_id,
								(int) expected,
								(unsigned long) (now_us - wait_start_us),
								poll_us, spins)));
				next_warn_us = now_us + 5000000;
			}
		}

		did_wait = true;
		if (spins < 128)
		{
			spins++;
			pg_spin_delay();
		}
		else
		{
			if (poll_us == 0)
				poll_us = 1;
			else if (poll_us < 1000)
				poll_us *= 2;
			if (poll_us > 1000)
				poll_us = 1000;
			pg_usleep((long) poll_us);
		}
	}

	if (collect_gate_stats)
	{
		if (did_wait)
		{
			uint64 elapsed = bcdb_get_time() - wait_start_us;

			SHARD_ADD(block_enqueue_turn_wait_total_us, elapsed);
			SHARD_UPDATE_MAX(block_enqueue_turn_wait_max_us, elapsed);
		}
	}
	if (active_wait_registered)
	{
		gate_stats_finish_wait();
	}
}

static inline void
bcdb_advance_block_enqueue_turn(BCBlockID block_id)
{
	BCBlockID next_block_id;

	if (block_id < BCDB_FIRST_SUBMIT_BLOCK_ID)
		return;
	next_block_id = block_id + 1;

	for (;;)
	{
		BCBlockID expected;

		expected = __atomic_load_n(&block_meta->next_enqueue_block_id,
								   __ATOMIC_ACQUIRE);
		if (expected > block_id)
			return;
		if (expected < block_id)
		{
			ereport(LOG,
					(errmsg("BCDB block enqueue turn advanced out of order: block_id=%d expected_block_id=%d",
							(int) block_id, (int) expected)));
			return;
		}
		if (__sync_bool_compare_and_swap(&block_meta->next_enqueue_block_id,
										 block_id,
										 next_block_id))
			return;
	}
}

/*
 * bcdb_wait_until_committed
 * -------------------------
 * Block the caller until the contiguous "last committed tx id" watermark
 * has caught up to target_tx_id.
 *
 * Why poll instead of just sleeping on a condition variable?
 *   Earlier revisions used ConditionVariableBroadcast() from worker.c, but
 *   we hit cases where a wakeup landed between the loader's CHECK and its
 *   ConditionVariableSleep(), causing indefinite hangs.  This loop instead
 *   uses adaptive backoff (busy-spin -> exponential usleep), and only falls
 *   back to a condition variable in BCDB_SERIAL_GATE_MODE_CONDVAR mode for
 *   experiments that explicitly want that path.
 *
 * Backoff schedule:
 *   - first 128 iterations: pg_spin_delay() (a few cycles of PAUSE-equivalent);
 *   - thereafter           : usleep ramping 1us -> 2 -> 4 -> ... -> 64us;
 *   - alternative          : ConditionVariableSleep gated by the global
 *                            bcdb_serial_gate_mode tunable.
 *
 * HANG DEBUG: logs every 5 s if the watermark fails to advance.  This fires
 * unconditionally (no env-var gate) so any production hang is immediately
 * visible in server.log with the relevant pid / target / observed state.
 *
 * Returns: elapsed wait time in microseconds (consumed by profiling).
 */
static inline uint64
bcdb_wait_until_committed(BCTxID target_tx_id)
{
	BCBlock *blk = bcdb_get_block1(); /* sentinel block holds the CV */
	int spins = 0;
	int poll_us = 0;
	uint64 wait_start_us = bcdb_get_time();
	uint64 next_warn_us  = wait_start_us + 5000000; /* first warning at +5 s */
	const bool collect_gate_stats = unlikely(bcdb_gate_telemetry_enabled);
	bool active_wait_registered = false;

	if (collect_gate_stats)
	{
		SHARD_INC(block_watermark_wait_calls);
	}

	Assert(blk != NULL);

	if (collect_gate_stats && get_last_committed_txid(NULL) < target_tx_id)
	{
		gate_stats_begin_wait(BCDB_GATE_PHASE_WATERMARK, target_tx_id, -1);
		active_wait_registered = true;
	}

	for (;;)
	{
		/* Fast path: a worker may already have caught the watermark up. */
		BCTxID committed = get_last_committed_txid(NULL);
		if (committed >= target_tx_id)
		{
			uint64 elapsed = bcdb_get_time() - wait_start_us;

			if (collect_gate_stats)
			{
				SHARD_ADD(block_watermark_wait_total_us, elapsed);
				SHARD_UPDATE_MAX(block_watermark_wait_max_us, elapsed);
			}
			if (active_wait_registered)
				gate_stats_finish_wait();
			return elapsed;
		}

		/* Respect SIGINT/SIGTERM/SIGUSR while we wait. */
		CHECK_FOR_INTERRUPTS();

		/* Always-on hang watchdog: fire every 5 s so a stuck loop is visible. */
		{
			uint64 now_us = bcdb_get_time();
			if (now_us >= next_warn_us)
			{
				ereport(LOG,
						(errmsg("[BCDB_HANG] committed_wait_stuck pid=%d target_txid=%d last_committed=%d waited_us=%lu poll_us=%d spins=%d",
								(int) getpid(), (int) target_tx_id,
								(int) committed,
								(unsigned long) (now_us - wait_start_us),
								poll_us, spins)));
				bcdb_log_gate_snapshot("committed_wait_stuck", -1, target_tx_id, -1);
				next_warn_us = now_us + 5000000;
			}
		}

		if (spins < 128)
		{
			/* Phase 1: busy-spin briefly; this is the cheapest path when
			 * the watermark is about to advance within a few microseconds. */
			spins++;
			pg_spin_delay();
		}
		else
		{
			if (bcdb_serial_gate_mode == BCDB_SERIAL_GATE_MODE_CONDVAR)
			{
				/* Phase 2a (opt-in): block on the sentinel block's commit
				 * CV.  We MUST re-check the watermark between Prepare and
				 * Sleep to avoid missing a wakeup that fired in between. */
				ConditionVariablePrepareToSleep(&blk->condCommit);
				if (get_last_committed_txid(NULL) < target_tx_id)
					ConditionVariableSleep(&blk->condCommit, WAIT_EVENT_BLOCK_COMMIT);
				ConditionVariableCancelSleep();
			}
			else
			{
				/* Phase 2b (default): exponential backoff sleep, capped at 64us
				 * so we still respond quickly once the watermark moves. */
				if (poll_us == 0)
					poll_us = 1;
				else if (poll_us < 64)
					poll_us *= 2;
				pg_usleep((long) poll_us);
			}
		}
	}
}

/*
 * bcdb_wait_until_slot_ready
 * --------------------------
 * Per-slot completion wait: spin/sleep until the sentinel block's
 * result_committed_txid[slot] equals the caller's target_tx_id, indicating
 * that the worker has finished writing both the row payload and the slot
 * ownership marker.
 *
 * Memory ordering:
 *   The worker writes the row payload first and only then publishes the
 *   tx_id into result_committed_txid[slot] with __ATOMIC_RELEASE.  This
 *   reader uses __ATOMIC_ACQUIRE so the row store is visible iff the
 *   tx_id match succeeds.  Reading the slot any other way is unsafe.
 *
 * Important scope limitation (T3 experiment context):
 *   Observing that THIS slot is ready does NOT imply that earlier slots
 *   in the same block are also ready -- workers write slots at Step 10
 *   but only enter the serialisation gate at Step 11.  Use this only when
 *   waiting on a specific tx in isolation.  For whole-block readiness,
 *   prefer bcdb_wait_until_block_slots_ready (or the watermark variant).
 *
 * HANG DEBUG: fires every 5 s if the slot value never matches.  The log
 * line includes the slot index, the current vs expected tx id, and the
 * latest committed watermark -- enough to distinguish "worker crashed
 * before publishing" from "worker still running but slow".
 *
 * Returns: elapsed wait time in microseconds.
 */
static inline uint64
bcdb_wait_until_slot_ready(BCTxID target_tx_id)
{
	BCBlock *blk     = bcdb_get_block1(); /* sentinel block holds the result ring */
	int      slot    = bcdb_result_slot_for_txid(target_tx_id);
	int      spins   = 0;
	int      poll_us = 0;
	uint64   wait_start_us = bcdb_get_time();
	uint64   next_warn_us  = wait_start_us + 5000000; /* first warning at +5 s */
	const bool collect_gate_stats = unlikely(bcdb_gate_telemetry_enabled);
	bool   active_wait_registered = false;

	if (collect_gate_stats)
	{
		SHARD_INC(slot_fallback_wait_calls);
	}

	Assert(blk != NULL);

	if (collect_gate_stats && __atomic_load_n(&blk->result_committed_txid[slot], __ATOMIC_ACQUIRE) < target_tx_id)
	{
		gate_stats_begin_wait(BCDB_GATE_PHASE_SLOT_FALLBACK, target_tx_id, -1);
		active_wait_registered = true;
	}

	for (;;)
	{
		/* ACQUIRE so any payload write done before the release-store
		 * in the worker is observable when the tx ids match. */
		BCTxID published = __atomic_load_n(&blk->result_committed_txid[slot],
										   __ATOMIC_ACQUIRE);
		if (published == target_tx_id)
		{
			uint64 elapsed = bcdb_get_time() - wait_start_us;
			if (collect_gate_stats)
			{
				SHARD_ADD(slot_fallback_wait_total_us, elapsed);
				SHARD_UPDATE_MAX(slot_fallback_wait_max_us, elapsed);
			}
			if (active_wait_registered)
				gate_stats_finish_wait();
			return elapsed;
		}

		CHECK_FOR_INTERRUPTS();

		/* Always-on hang watchdog: fire every 5 s so a stuck loop is visible. */
		{
			uint64 now_us = bcdb_get_time();
			if (now_us >= next_warn_us)
			{
				BCTxID last_committed = get_last_committed_txid(NULL);
				ereport(LOG,
						(errmsg("[BCDB_HANG] slot_ready_stuck pid=%d target_txid=%d slot=%d slot_value=%d last_committed=%d waited_us=%lu poll_us=%d spins=%d",
								(int) getpid(), (int) target_tx_id, slot,
								(int) published, (int) last_committed,
								(unsigned long) (now_us - wait_start_us),
								poll_us, spins)));
				next_warn_us = now_us + 5000000;
			}
		}

		if (spins < 128)
		{
			/* Phase 1: busy-spin briefly. */
			spins++;
			pg_spin_delay();
		}
		else
		{
			/* Phase 2: exponential backoff sleep, capped at 64us. */
			if (poll_us == 0)
				poll_us = 1;
			else if (poll_us < 64)
				poll_us *= 2;
			pg_usleep((long) poll_us);
		}
	}
}

/*
 * bcdb_wait_until_block_slots_ready
 * ---------------------------------
 * Wait until every locally copied tx ref has its result slot published in
 * the sentinel block's result ring.
 *
 * Rationale (vs. per-slot waits in a loop):
 *   The earlier code path walked block->txs[] in order and called the
 *   per-slot wait for each entry.  When a block contains hundreds of txs
 *   that turns a single ~few-microsecond readiness delay into hundreds of
 *   small adaptive backoff loops -- a lot of wasted spinning.  This routine
 *   amortises the wait by scanning all slots once per backoff iteration
 *   and only continuing to spin/sleep when at least one is still missing.
 *
 *   Correctness is identical: each slot is verified with the same
 *   ACQUIRE-load tx_id match, so we never read a recycled slot.
 *
 *   The input is a backend-local copy, not block->txs[].  Workers can free
 *   BCDBShmXact entries and block_cleaning_dt() can reclaim block headers
 *   while the submitter is still waiting under deeper gateway pipelines.
 *
 * Reporting:
 *   On the 5-second hang watchdog, we report the FIRST missing slot
 *   (which is the one stalling the block).  That makes server.log point
 *   directly at the slowest tx without flooding logs for every slot.
 *
 * Returns: elapsed wait time in microseconds.
 */
static inline uint64
bcdb_wait_until_block_slots_ready(const BCDBBlockResultRef *refs,
								  int num_tx,
								  BCBlockID block_id)
{
	BCBlock *result_block = bcdb_get_block1(); /* sentinel block holds the ring */
	int    spins = 0;
	int    poll_us = 0;
	uint64 wait_start_us = bcdb_get_time();
	uint64 next_warn_us = wait_start_us + 5000000; /* first warning at +5 s */
	const bool collect_gate_stats = unlikely(bcdb_gate_telemetry_enabled);
	bool   active_wait_registered = false;

	if (collect_gate_stats)
	{
		SHARD_INC(block_slot_wait_calls);
	}

	Assert(refs != NULL || num_tx == 0);
	Assert(result_block != NULL);

	if (collect_gate_stats)
	{
		bool all_ready = true;
		for (int i = 0; i < num_tx; ++i)
		{
			const BCTxID tx_id = refs[i].tx_id;
			const int slot = bcdb_result_slot_for_txid(tx_id);
			if (__atomic_load_n(&result_block->result_committed_txid[slot], __ATOMIC_ACQUIRE) != tx_id)
			{
				all_ready = false;
				break;
			}
		}
		if (!all_ready)
		{
			BCTxID tx_id = num_tx > 0 ? refs[0].tx_id : -1;
			gate_stats_begin_wait(BCDB_GATE_PHASE_SLOT, tx_id, block_id);
			active_wait_registered = true;
		}
	}

	for (;;)
	{
		bool all_ready = true;
		BCTxID first_missing_txid = -1;
		BCTxID first_missing_value = -1;
		int first_missing_slot = -1;

		/* One pass over every tx; bail out as soon as a slot is missing. */
		for (int i = 0; i < num_tx; ++i)
		{
			const BCTxID tx_id = refs[i].tx_id;
			const int slot = bcdb_result_slot_for_txid(tx_id);
			BCTxID published;

			/* ACQUIRE-load: pairs with the worker's RELEASE-store after
			 * publishing the row payload.  Slot is owned by tx only when
			 * the stored tx_id matches exactly. */
			published = __atomic_load_n(&result_block->result_committed_txid[slot],
										__ATOMIC_ACQUIRE);
			if (published != tx_id)
			{
				all_ready = false;
				first_missing_txid = tx_id;
				first_missing_value = published;
				first_missing_slot = slot;
				break;
			}
		}
		if (all_ready)
		{
			uint64 elapsed = bcdb_get_time() - wait_start_us;

			if (collect_gate_stats)
			{
				SHARD_ADD(block_slot_wait_total_us, elapsed);
				SHARD_UPDATE_MAX(block_slot_wait_max_us, elapsed);
			}
			if (active_wait_registered)
				gate_stats_finish_wait();
			return elapsed;
		}

		CHECK_FOR_INTERRUPTS();

		/* Hang watchdog -- log the first stalled slot so it's actionable. */
		{
			uint64 now_us = bcdb_get_time();
			if (now_us >= next_warn_us)
			{
				BCTxID last_committed = get_last_committed_txid(NULL);
				ereport(LOG,
						(errmsg("[BCDB_HANG] block_slots_ready_stuck pid=%d block_id=%d first_missing_txid=%d slot=%d slot_value=%d last_committed=%d waited_us=%lu poll_us=%d spins=%d",
								(int) getpid(), (int) block_id,
								(int) first_missing_txid, first_missing_slot,
								(int) first_missing_value, (int) last_committed,
								(unsigned long) (now_us - wait_start_us),
								poll_us, spins)));
				next_warn_us = now_us + 5000000;
			}
		}

		if (spins < 128)
		{
			/* Phase 1: busy-spin briefly between full slot scans. */
			spins++;
			pg_spin_delay();
		}
		else
		{
			/* Phase 2: exponential backoff sleep, capped at 64us. */
			if (poll_us == 0)
				poll_us = 1;
			else if (poll_us < 64)
				poll_us *= 2;
			pg_usleep((long) poll_us);
		}
	}
}

/*
 * bcdb_middleware_init
 * --------------------
 * Initialize the middleware-facing BCDB runtime for the calling backend.
 * This is the C-callable side of the SQL function bcdb_init().
 *
 * Arguments:
 *   is_oep_mode -- legacy flag from the original codebase; Aria does NOT
 *                  use OEP mode, so this is ignored.  We always set
 *                  is_bcdb_master = true (every node behaves as a master).
 *   block_size  -- historical name; in this implementation it selects the
 *                  WORKER/QUEUE count, not a tx batch size.  See
 *                  BCDB_DECOUPLE_WORKERS for the case where the caller
 *                  wants to ignore this parameter entirely.
 *
 * Sentinel block (block id 1):
 *   This block does not contain user txs -- it stores runtime metadata
 *   (blksize, result ring, condition variables).  Its blksize value is
 *   the source of truth for the worker count.  If a later bcdb_init()
 *   call requests a different size we abort with ERROR: the worker pool
 *   cannot be safely resized once workers have been launched, so the
 *   correct fix is to restart PostgreSQL.
 *
 * Memory context:
 *   bcdb_middleware_context is created once under TopMemoryContext and
 *   then reused across every subsequent bcdb_init() call within the same
 *   backend.  Restore scripts and benchmark drivers call bcdb_init()
 *   repeatedly; without this reuse we would leak one long-lived context
 *   per invocation.
 *
 * Side effects (in order):
 *   - selects worker_queues per the env-var / param fallback chain;
 *   - publishes blocksize globally;
 *   - lazily creates the middleware memory context;
 *   - allocates/locates the sentinel block;
 *   - warns if the result ring is shallower than 2x the worker count;
 *   - validates against a prior blksize and errors on mismatch;
 *   - initialises the idle-worker list on first call.
 */
void
bcdb_middleware_init(bool is_oep_mode, int32 block_size)
{
	MemoryContext    old_context;
	BCBlock *block;
	int32 worker_queues;

	/* Aria does not have OEP (Original Execution Plan) mode -- every node
	 * behaves as a deterministic master regardless of the is_oep_mode arg. */
	is_bcdb_master = true;

	if (bcdb_decouple_workers_enabled())
	{
		/* Ignore the SQL block_size and pick the worker count purely from
		 * the GUC / compile-time default. */
		worker_queues = bcdb_select_worker_count(0);
	}
	else
	{
		/* Honour the SQL argument and also publish it as the GUC value so
		 * subsequent code (and other modules) see a consistent number. */
		worker_queues = bcdb_select_worker_count(block_size);
		bcdb_worker_count = worker_queues;
	}
	blocksize = worker_queues;

	/* Lazy, one-time context creation; reused across re-inits. */
	if (bcdb_middleware_context == NULL)
		bcdb_middleware_context =
			AllocSetContextCreate(TopMemoryContext,
								  "middleware memory context",
								  ALLOCSET_DEFAULT_SIZES);
	old_context = MemoryContextSwitchTo(bcdb_middleware_context);

	/* Materialise/locate the sentinel block, the holder of runtime metadata. */
	block = get_block_by_id(1, true);

	/* Result ring sizing warning.  The runtime clamps slots when it's
	 * smaller than 2x worker count, but that hides throughput problems --
	 * we want operators to notice and tune bcdb_result_ring_slots. */
	if (bcdb_get_result_ring_slots() < 2 * blocksize)
		ereport(WARNING,
			(errmsg("bcdb_result_ring_slots=%d is lower than 2 * bcdb_worker_count=%d; runtime will clamp slots",
				bcdb_get_result_ring_slots(), 2 * blocksize)));

	/* Worker count is immutable for the life of the cluster. */
	if (block->blksize > 0 && block->blksize != blocksize)
		ereport(ERROR,
				(errmsg("bcdb_worker_count mismatch: existing=%d requested=%d; restart required",
						block->blksize, blocksize)));

	/* Publish the agreed-upon blksize and bring up the idle-worker list
	 * on the very first init.  Subsequent inits validate consistency. */
	set_blksz(blocksize);
	if (idle_workers.num == 0)
		idle_worker_list_init(blocksize);
	else if (idle_workers.num != blocksize)
		ereport(ERROR,
				(errmsg("BCDB workers already initialized with %d workers; requested %d",
						idle_workers.num, blocksize)));

	MemoryContextSwitchTo(old_context);
#if SAFEDBG2
	printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
#endif

	/* Timestamp used only for the "total throughput" LOG line. */
	start_time = bcdb_get_time();
}

/*
 * bcdb_middleware_init2
 * ---------------------
 * Identical to bcdb_middleware_init() but additionally captures the legacy
 * burst-throttle parameters used by bcdb_middleware_submit_block2().
 *
 * numTx     -- number of txs between throttle pauses (numTxBurst).
 * timeSlot  -- microseconds of usleep() per pause (burstTime).
 *
 * These knobs do NOT influence determinism or correctness; they only
 * govern how aggressively submit_block2() pushes txs onto worker queues.
 * The active distributed YCSB/TPCC path uses
 * bcdb_middleware_submit_block_results() instead and ignores these values.
 */
void
bcdb_middleware_init2(bool is_oep_mode, int32 block_size, int32 numTx, int32 timeSlot)
{
	MemoryContext    old_context;
	BCBlock *block;
	int32 worker_queues;

	is_bcdb_master = true;
	if (bcdb_decouple_workers_enabled())
		worker_queues = bcdb_select_worker_count(0);
	else
	{
		worker_queues = bcdb_select_worker_count(block_size);
		bcdb_worker_count = worker_queues;
	}
	blocksize = worker_queues;
	/* Capture legacy burst knobs for submit_block2(). */
	numTxBurst = numTx;
	burstTime = timeSlot;

	if (bcdb_middleware_context == NULL)
		bcdb_middleware_context =
			AllocSetContextCreate(TopMemoryContext,
								  "middleware memory context",
								  ALLOCSET_DEFAULT_SIZES);
	old_context = MemoryContextSwitchTo(bcdb_middleware_context);
	block = get_block_by_id(1, true);
	if (bcdb_get_result_ring_slots() < 2 * blocksize)
		ereport(WARNING,
			(errmsg("bcdb_result_ring_slots=%d is lower than 2 * bcdb_worker_count=%d; runtime will clamp slots",
				bcdb_get_result_ring_slots(), 2 * blocksize)));
	if (block->blksize > 0 && block->blksize != blocksize)
		ereport(ERROR,
				(errmsg("bcdb_worker_count mismatch: existing=%d requested=%d; restart required",
						block->blksize, blocksize)));
	set_blksz(blocksize);
	if (idle_workers.num == 0)
		idle_worker_list_init(blocksize);
	else if (idle_workers.num != blocksize)
		ereport(ERROR,
				(errmsg("BCDB workers already initialized with %d workers; requested %d",
						idle_workers.num, blocksize)));
	MemoryContextSwitchTo(old_context);
#if SAFEDBG
	printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
#endif

	start_time = bcdb_get_time();
}

/*
 * parse_tx
 * --------
 * Parse a single transaction JSON object and allocate its shared-memory
 * tx entry (BCDBShmXact).
 *
 * Expected JSON shape:
 *   { "hash":      "<unique tx identifier string>",   // required
 *     "sql":       "<SQL command text>",              // required
 *     "create_ts": "<optional integer timestamp>" }   // optional, ms epoch
 *
 * Tx id is intentionally NOT assigned here:
 *   - Single-tx flow: bcdb_middleware_submit_tx() assigns the id atomically
 *     via an fetch-and-add on tx_num after parsing.
 *   - Block flow: parse_block_with_txs() reserves a contiguous tx-id range
 *     up front so every tx in the block has a stable id BEFORE any worker
 *     can observe it.  This guarantees deterministic order within the block.
 *
 * pred_lock=false:
 *   Aligns with the direct deterministic wire path ("s <seq> <sql>").
 *   PostgreSQL's predicate-lock hook still records BCDB read-set tags
 *   for our own conflict_checkDT(), but the heavyweight SSI predicate
 *   lock acquisition is skipped because BCDB's deterministic conflict
 *   resolution does not rely on SSI.
 *
 * On parse failure we ereport(ERROR); cJSON allocations live in the current
 * memory context and will be reaped automatically when the error unwinds.
 */
BCDBShmXact *
parse_tx(const char* json)
{
	cJSON   *parsed   = NULL;
	cJSON   *sql      = NULL;
	cJSON   *hash     = NULL;
	cJSON   *create_time = NULL;
	BCDBShmXact   *tx;
	int     isolation;
	bool    pred_lock = false;

	parsed = cJSON_Parse(json);
	if (!parsed)
		goto error;

	/* Required: "sql" -- the actual SQL string to execute. */
	sql = cJSON_GetObjectItemCaseSensitive(parsed, "sql");
	if (!cJSON_IsString(sql) || (sql->valuestring == NULL))
		goto error;

	/* Required: "hash" -- a caller-supplied unique identifier; used as
	 * the key for get_tx_by_hash() in the legacy attach/wait paths. */
	hash = cJSON_GetObjectItemCaseSensitive(parsed, "hash");
	if (!cJSON_IsString(hash))
		goto error;

	/* BCDB always runs SERIALIZABLE; deterministic execution is built on
	 * top of serial-equivalent ordering. */
	isolation = XACT_SERIALIZABLE;
	pred_lock = false; /* see header comment for rationale */

	/* Materialise the tx in shared memory.  Tx id and block id are left
	 * invalid here -- callers fill them in once they know the assignment. */
	tx = create_tx(hash->valuestring, sql->valuestring,
				   BCDBInvalidTid, BCDBInvalidBid, isolation, pred_lock);
	if (tx == NULL)
	{
		ereport(ERROR,
			(errmsg("[ZL] cannot create transaction in shared memory")));
		return NULL;
	}

#if SAFEDBG
	printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
#endif
	/* Optional: "create_ts" is the millisecond-epoch timestamp of when
	 * the frontend produced the tx; used for end-to-end latency telemetry. */
	create_time = cJSON_GetObjectItemCaseSensitive(parsed, "create_ts");
	if (cJSON_IsString(create_time))
	{
		char *endpt;
		tx->create_time = strtoll(create_time->valuestring, &endpt, 10);
	}

	cJSON_Delete(parsed);
	return tx;

error:
	ereport(ERROR,
		(errmsg("[ZL] Cannot parse transaction: %s", json)));
	/* Memory context cleanup handles partially-built cJSON nodes. */
	return NULL;
}

/*
 * parse_block_with_txs
 * --------------------
 * Parse a "block of transactions" JSON payload, materialise every tx in
 * shared memory, and stitch them into a BCBlock with stable, contiguous
 * tx ids.  This is the central setup step for the deterministic execution
 * pipeline.
 *
 * Expected JSON shape:
 *   { "bid": <int block id>,
 *     "txs": [ { "hash": "...", "sql": "...", "create_ts": "..." }, ... ] }
 *
 * Key invariant -- contiguous tx-id range:
 *   We reserve [tx_base, tx_base + num_tx) atomically from the sentinel
 *   block's num_tx_sub counter BEFORE entering the per-tx loop.  Then we
 *   assign tx->tx_id = tx_base + tx_local_idx in order.  Without this
 *   reservation, two backends submitting blocks concurrently could
 *   interleave their tx-id assignments and break the in-block ordering
 *   that workers rely on.
 *
 * Side effects:
 *   - block->num_tx is set from the JSON array size;
 *   - block->txs[i] is populated in order;
 *   - each tx has tx_id and block_id_committed set BEFORE workers see it
 *     (workers can only see a tx after tx_queue_insert(), which our caller
 *     performs after this function returns).
 *
 * On any failure we ereport(FATAL) -- a malformed block is unrecoverable
 * because shared-memory state may already be partially mutated.
 */
BCBlock *
parse_block_with_txs(const char *json)
{
	cJSON *parsed;
	cJSON *tx_list;
	cJSON *block_id;
	cJSON *tx_json;
	BCBlock *block;
	BCTxID *explicit_txids = NULL;
	bool use_explicit_txids = true;
	int j = 0;             /* cap for first-N SAFEDBG cJSON_Print calls */
	int tx_base = 0;       /* atomically reserved starting tx-id */
	int tx_local_idx = 0;  /* offset within this block, 0..num_tx-1 */
	BCBlock *sentinel = NULL;

	parsed = cJSON_Parse(json);
	if (!parsed)
		goto error;

	/* "bid" -- the block id this submission targets. */
	block_id = cJSON_GetObjectItemCaseSensitive(parsed, "bid");

	/* "txs" -- mandatory array of per-tx JSON objects. */
	tx_list = cJSON_GetObjectItemCaseSensitive(parsed, "txs");
	if (!cJSON_IsArray(tx_list))
		goto error;

	/* Locate/allocate the destination block header (create=true). */
	block = get_block_by_id(block_id->valueint, true);
#if SAFEDBG
	printf("ariaMyDbg %s : %s: %d blksz %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , get_blksz(), getpid());
#endif
	block->num_tx = cJSON_GetArraySize(tx_list);
	if (block->num_tx > MAX_TX_PER_BLOCK)
		goto error;

	sentinel = get_block_by_id(1, true);
	Assert(sentinel != NULL);
	if (block->num_tx > 0)
	{
		int explicit_idx = 0;
		BCTxID prev_txid = -1;

		explicit_txids = (BCTxID *) palloc0(sizeof(BCTxID) * block->num_tx);
		cJSON_ArrayForEach(tx_json, tx_list)
		{
			cJSON *txid_json = cJSON_GetObjectItemCaseSensitive(tx_json, "txid");
			BCTxID txid;

			if (!cJSON_IsNumber(txid_json) ||
				txid_json->valuedouble < 0 ||
				txid_json->valuedouble > PG_INT32_MAX)
			{
				use_explicit_txids = false;
				break;
			}
			txid = (BCTxID) txid_json->valuedouble;
			if (explicit_idx > 0 && txid != prev_txid + 1)
				goto error;
			explicit_txids[explicit_idx++] = txid;
			prev_txid = txid;
		}
	}
	else
	{
		use_explicit_txids = false;
	}

	if (!use_explicit_txids)
	{
		if (explicit_txids != NULL)
		{
			pfree(explicit_txids);
			explicit_txids = NULL;
		}
		/* Reserve a contiguous tx-id range from the sentinel.  __sync_fetch_and_add
		 * returns the pre-increment value; that becomes our tx_base so the first
		 * tx gets exactly the value other backends will skip past. */
		tx_base = __sync_fetch_and_add(&sentinel->num_tx_sub, block->num_tx);
	}

	cJSON_ArrayForEach(tx_json, tx_list)
	{
		cJSON   *sql      = NULL;
		cJSON   *hash     = NULL;
		cJSON   *create_time = NULL;
		BCDBShmXact   *tx;
		int     isolation;
		bool    pred_lock = false;

		/* Required: "sql". */
		sql = cJSON_GetObjectItemCaseSensitive(tx_json, "sql");
		if (!cJSON_IsString(sql) || (sql->valuestring == NULL))
			goto error;

		/* Required: "hash". */
		hash = cJSON_GetObjectItemCaseSensitive(tx_json, "hash");
		if (!cJSON_IsString(hash) || (hash->valuestring == NULL))
			goto error;

#if SAFEDBG
		/* Diagnostic dump of the first few txs only */
		if (j < 5) {
			char *p_sql = cJSON_Print(sql);
			char *p_hash = cJSON_Print(hash);
			if (p_sql) { printf("safeDbg sql: %s\n", p_sql); free(p_sql); }
			if (p_hash) { printf("safeDbg hash: %s\n", p_hash); free(p_hash); }
			j++;
		}
#endif

		isolation = XACT_SERIALIZABLE;
		/* See parse_tx() for full rationale: pred_lock=false matches the
		 * direct DT wire path -- read-set capture still happens via BCDB's
		 * own predicate-lock hook, just without the SSI heavyweight locks. */
		pred_lock = false;

		tx = create_tx(hash->valuestring, sql->valuestring,
					   BCDBInvalidTid, BCDBInvalidBid, isolation, pred_lock);
		if (tx == NULL)
			goto error;

		/* Optional: "create_ts" frontend-side millisecond timestamp. */
		create_time = cJSON_GetObjectItemCaseSensitive(tx_json, "create_ts");
		if (cJSON_IsString(create_time))
		{
			char *endpt;
			tx->create_time = strtoll(create_time->valuestring, &endpt, 10);
		}

		/* Parse Raft apply ledger metadata (Commit D2) */
		{
			/*
			 * raft_log_index is transmitted as a JSON *string* (decimal digits) to
			 * preserve exact uint64 precision above 2^53 — JSON "number" uses
			 * IEEE-754 double which loses the low bits of very large indices.
			 *
			 * P0-D strict validation rules:
			 *   raft_log_index   — JSON string, decimal digits only, no sign,
			 *                      no whitespace, no overflow, value > 0
			 *   raft_item_count  — exact integer 1..UINT32_MAX (not fractional,
			 *                      not negative, not zero)
			 *   raft_item_ordinal — exact integer 0 <= ordinal < item_count
			 *   epoch / entry_digest / item_digest — exactly 64 lowercase hex chars
			 *
			 * All safe metadata fields must be present together or absent together.
			 * Partial metadata is rejected.
			 */
			cJSON *raft_ledger_required = cJSON_GetObjectItemCaseSensitive(tx_json, "raft_ledger_required");
			cJSON *raft_log_index = cJSON_GetObjectItemCaseSensitive(tx_json, "raft_log_index");
			cJSON *raft_item_ordinal = cJSON_GetObjectItemCaseSensitive(tx_json, "raft_item_ordinal");
			cJSON *raft_item_count = cJSON_GetObjectItemCaseSensitive(tx_json, "raft_item_count");
			cJSON *raft_epoch_id = cJSON_GetObjectItemCaseSensitive(tx_json, "raft_epoch_id");
			cJSON *entry_digest = cJSON_GetObjectItemCaseSensitive(tx_json, "entry_digest");
			cJSON *item_digest = cJSON_GetObjectItemCaseSensitive(tx_json, "item_digest");
			bool required = false;
			bool has_any_raft_metadata = false;

			if (raft_ledger_required != NULL)
			{
				if (cJSON_IsTrue(raft_ledger_required))
					required = true;
				else if (!cJSON_IsFalse(raft_ledger_required))
					goto error;
			}

			has_any_raft_metadata =
				raft_log_index != NULL ||
				raft_item_ordinal != NULL ||
				raft_item_count != NULL ||
				raft_epoch_id != NULL ||
				entry_digest != NULL ||
				item_digest != NULL;

			if (required || has_any_raft_metadata)
			{
				/* All six fields must be present and of the correct type.  Any
				 * missing or malformed field is a hard error — reject the block. */
				if (!cJSON_IsString(raft_log_index) ||
					!raft_item_ordinal || !cJSON_IsNumber(raft_item_ordinal) ||
					!raft_item_count || !cJSON_IsNumber(raft_item_count) ||
					!raft_epoch_id || !cJSON_IsString(raft_epoch_id) || !is_valid_hex_64(raft_epoch_id->valuestring) ||
					!entry_digest || !cJSON_IsString(entry_digest) || !is_valid_hex_64(entry_digest->valuestring) ||
					!item_digest || !cJSON_IsString(item_digest) || !is_valid_hex_64(item_digest->valuestring))
				{
					goto error;
				}

				/* Parse log index as decimal string to avoid double precision loss.
				 * Strict rules: decimal digits only, no sign, no whitespace, > 0. */
				{
					const char *idx_str = raft_log_index->valuestring;
					char *endptr = NULL;
					unsigned long long parsed_idx;

					/* Reject leading sign, whitespace, or empty string */
					if (!idx_str || idx_str[0] == '\0' ||
						idx_str[0] == '+' || idx_str[0] == '-' ||
						idx_str[0] == ' ' || idx_str[0] == '\t')
						goto error;

					errno = 0;
					parsed_idx = strtoull(idx_str, &endptr, 10);
					if (errno != 0 || !endptr || *endptr != '\0' || parsed_idx == 0)
						goto error; /* overflow, trailing garbage, or zero */

					tx->raft_log_index = (uint64) parsed_idx;
				}

				/*
				 * Strict integer validation for raft_item_count:
				 *   - must be a whole number (no fractional part)
				 *   - must be in [1, UINT32_MAX]
				 */
				{
					double count_d = raft_item_count->valuedouble;
					double count_floor = (double)(unsigned long long) count_d;

					/* Reject fractional, negative, zero, or overflowed counts */
					if (count_d < 1.0 || count_d != count_floor ||
						count_d > (double) 0xFFFFFFFFULL)
						goto error;

					tx->raft_item_count = (uint32) (unsigned long long) count_d;
				}

				/*
				 * Strict integer validation for raft_item_ordinal:
				 *   - must be a whole number (no fractional part)
				 *   - must be in [0, item_count - 1]
				 */
				{
					double ord_d = raft_item_ordinal->valuedouble;
					double ord_floor = (double)(unsigned long long) ord_d;
					uint32 item_count = tx->raft_item_count;

					/* Reject fractional, negative, or out-of-range ordinals */
					if (ord_d < 0.0 || ord_d != ord_floor ||
						ord_d > (double) 0xFFFFFFFFULL)
						goto error;

					{
						uint32 ordinal = (uint32) (unsigned long long) ord_d;

						if (ordinal >= item_count)
							goto error; /* ordinal must be < count */

						tx->raft_item_ordinal = ordinal;
					}
				}

				tx->raft_ledger_enabled = true;
				tx->raft_terminal_state = 0;
				tx->raft_terminal_format_version = 0;

				decode_hex(raft_epoch_id->valuestring, tx->raft_epoch_id, BCDB_RAFT_DIGEST_BYTES);
				decode_hex(entry_digest->valuestring, tx->raft_entry_digest, BCDB_RAFT_DIGEST_BYTES);
				decode_hex(item_digest->valuestring, tx->raft_item_digest, BCDB_RAFT_DIGEST_BYTES);
			}
			else
			{
				tx->raft_ledger_enabled = false;
				tx->raft_log_index = 0;
				tx->raft_item_ordinal = 0;
				tx->raft_item_count = 0;
				tx->raft_terminal_state = 0;
				tx->raft_terminal_format_version = 0;
				memset(tx->raft_epoch_id, 0, BCDB_RAFT_DIGEST_BYTES);
				memset(tx->raft_entry_digest, 0, BCDB_RAFT_DIGEST_BYTES);
				memset(tx->raft_item_digest, 0, BCDB_RAFT_DIGEST_BYTES);
				memset(tx->raft_terminal_digest, 0, BCDB_RAFT_DIGEST_BYTES);
			}
		}

		/* Stamp the assigned ids and slot into the block's tx array.  Order
		 * here defines deterministic execution order within the block. */
		tx->tx_id = use_explicit_txids
			? explicit_txids[tx_local_idx]
			: tx_base + tx_local_idx;
		tx->block_id_committed = block->id;
		block->txs[tx_local_idx] = tx;
		tx_local_idx += 1;
#if SAFEDBG
		printf("ariaMyDbg %s : %s: %d txid %d bid %d hash %s \n", __FILE__, __FUNCTION__, __LINE__ , tx->tx_id, block->id, hash->valuestring);
#endif
	}
	if (use_explicit_txids && block->num_tx > 0)
	{
		int desired_next = explicit_txids[block->num_tx - 1] + 1;

		for (;;)
		{
			int current = sentinel->num_tx_sub;

			if (current >= desired_next)
				break;
			if (__sync_bool_compare_and_swap(&sentinel->num_tx_sub,
											 current,
											 desired_next))
				break;
		}
	}
	if (explicit_txids != NULL)
		pfree(explicit_txids);
	if (parsed != NULL)
		cJSON_Delete(parsed);
	return block;

error:
	if (explicit_txids != NULL)
		pfree(explicit_txids);
	if (parsed != NULL)
		cJSON_Delete(parsed);
	print_trace();
	ereport(FATAL,
		(errmsg("[ZL] cannot create block in shared memory")));
	return NULL;
}

/*
 * bcdb_middleware_submit_tx
 * -------------------------
 * C entry point for the legacy SQL function bcdb_tx_submit(tx_json).
 *
 * Pipeline:
 *   1. Parse the JSON via parse_tx() to materialise the tx in shared memory.
 *   2. Assign a unique tx_id by atomically incrementing the file-scope
 *      counter tx_num (__sync_fetch_and_add returns the pre-increment value).
 *   3. Push the tx onto its worker queue (worker selection is hashed on
 *      tx_id inside tx_queue_insert()).
 *   4. Return the tx_id back to the SQL caller.
 *
 * Why return the id?
 *   The SQL caller pairs this with a later wait/lookup; if we simply
 *   returned a success constant they would have no way to address the
 *   in-flight tx.
 *
 * Note: this single-tx path is preserved for backward compatibility.
 * The deterministic block-submit path uses parse_block_with_txs() to
 * assign a contiguous tx-id range from the sentinel block instead.
 */
int
bcdb_middleware_submit_tx(const char* tx_string)
{
	BCDBShmXact *tx;
	int32       tx_id;

	tx = parse_tx(tx_string);
	if (tx == NULL)
		ereport(ERROR,
				(errmsg("failed to parse BCDB transaction")));

	/* Atomic monotonic id assignment across concurrent submitting backends. */
	tx_id = __sync_fetch_and_add(&tx_num, 1);
	tx->tx_id = tx_id;
	tx_queue_insert(tx, tx_id);
#if SAFEDBG
	printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
#endif
	return tx_id;
}

/*
 * bcdb_middleware_submit_block
 * ----------------------------
 * Legacy block-submit C entry point behind the SQL function
 * bcdb_block_submit().  Submits an entire block and returns the result
 * text of ONLY the highest tx id in the block.
 *
 * Why "only the last result"?
 *   Older callers used this when they cared about a single read-back
 *   value (or just wanted to wait for completion).  The current SQL
 *   wrapper ignores the return value entirely.  When per-tx receipts
 *   are needed -- the distributed YCSB/TPCC path -- callers use
 *   bcdb_middleware_submit_block_results() instead.  This function is
 *   kept defensive because older tests/tools still call it.
 *
 * Slot-ownership safety:
 *   Result slots in the sentinel ring are recycled by future txs.  Before
 *   reading result[slot], we ACQUIRE-load result_committed_txid[slot] and
 *   verify it equals max_tx_id.  Any mismatch means the slot was taken by
 *   a later tx and we error out rather than return stale/wrong data.
 *
 * Empty block:
 *   max_tx_id stays at -1 -- we skip the wait and return an empty string.
 */
char *
bcdb_middleware_submit_block(const char* block_json)
{
	BCBlock     *submitted_block;
	BCBlock     *result_block;
	struct timeval tv1;
	int max_tx_id = -1;

	tv1.tv_sec = 0; tv1.tv_usec = 0;
	submitted_block = parse_block_with_txs(block_json);
	if (submitted_block == NULL)
		ereport(ERROR,
				(errmsg("failed to parse BCDB block JSON")));
	/* Advance the global block watermark so cleaning sees this block. */
	__sync_add_and_fetch(&block_meta->global_bmax, 1);
#if SAFEDBG
		printf("ariaMyDbg %s : %s: %d pid %d txnum %d blk-numtx %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid(), tx_num, submitted_block->num_tx);
#endif
	/* Push every tx onto its worker queue and remember the maximum id. */
	for (int i= 0; i < submitted_block->num_tx; i++)
	{
	  BCDBShmXact *tx = submitted_block->txs[i];
	  tx_queue_insert(tx, tx->tx_id);
	  if (tx->tx_id > max_tx_id)
		  max_tx_id = tx->tx_id;
	}

	result_block = bcdb_get_block1();
	if (result_block == NULL)
		ereport(ERROR,
				(errmsg("BCDB result block is not initialized")));
#if SAFEDBG
	gettimeofday(&tv1, NULL);
#endif
	/* Wait for the contiguous committed watermark to reach max_tx_id.
	 * Because the watermark only advances after each predecessor publishes
	 * its result slot, this implies our target slot is also published. */
	if (max_tx_id >= 0)
		bcdb_wait_until_committed((BCTxID) max_tx_id);
#if SAFEDBG
			gettimeofday(&tv1, NULL);
			printf("\n\n\t time= %ld.%ld  getpid %d\n", tv1.tv_sec, tv1.tv_usec, getpid());
			printf("blkmid read result at %d= %s\n", max_tx_id, result_block->result[bcdb_result_slot_for_txid(max_tx_id)]);
			printf("\n\t *** safeDB completed txid %d pid %d %s : %s: %d *** \n\n",
				   max_tx_id, getpid(), __FILE__, __FUNCTION__, __LINE__ );
			printf("\n\t *** safeDB txid %d pid %d result %s file %s : %s: %d *** \n\n",
				   max_tx_id, getpid(), &result_block->result[bcdb_result_slot_for_txid(max_tx_id)],__FILE__, __FUNCTION__, __LINE__ );
#endif

	if (max_tx_id < 0)
		return "";
	{
		const int slot = bcdb_result_slot_for_txid((BCTxID) max_tx_id);
		BCTxID published;

		/* ACQUIRE pairs with the worker's RELEASE-store of the tx id.  If
		 * the values don't match, the slot already belongs to a newer tx
		 * (ring recycling) and we MUST NOT return its row payload. */
		published = __atomic_load_n(&result_block->result_committed_txid[slot],
									 __ATOMIC_ACQUIRE);
		if (published != max_tx_id)
			ereport(ERROR,
					(errmsg("BCDB result slot mismatch for txid %d: slot %d contains txid %d",
							max_tx_id, slot, (int) published)));
		return result_block->result[slot];
	}
}

/*
 * bcdb_middleware_submit_block_results
 * ------------------------------------
 * Active deterministic block-submit API used by ariabc_pg's production
 * YCSB/TPCC pipeline.  The frontend submits ONE block of transactions as
 * JSON, this function:
 *
 *   1. parses and materialises every tx,
 *   2. enqueues each tx onto its worker queue,
 *   3. waits until every tx in the block has its result slot published,
 *   4. emits a newline-delimited completion payload keyed by tx hash,
 *   5. marks each slot consumed so workers can reuse the ring.
 *
 * Returned payload format (one line per tx, in submission order):
 *   "<tx_hash>\t<optional hex payload>\n"
 *
 * The optional payload is included only when BCDB_BLOCK_RETURN_ACTUAL_RESULTS
 * is set; the production default is completion-only.  Rationale: workers
 * apply writes in deterministic order so final database state is identical
 * across replicas, but read-row text can reflect local worker timing.  The
 * post-run Merkle gate validates state; completion-only payloads avoid
 * coupling correctness to per-tx text comparison.
 *
 * Hot correctness rule -- slot ownership:
 *   result_committed_txid[slot] MUST equal tx->tx_id at the moment of read.
 *   A mismatch indicates the slot has been reused by a later tx (the ring
 *   is finite and wraps), in which case reading result[slot] would return
 *   data from a different transaction.  We:
 *     - ACQUIRE-load to pair with the worker's RELEASE-store;
 *     - fall back to bcdb_wait_until_slot_ready() if the load misses
 *       (defensive: the block-level wait above should already cover this);
 *     - publish result_consumed_txid[slot] = tx_id with RELEASE so workers
 *       know the slot is free for the next write.
 *
 * Profiling (BCDB_BLOCK_PROFILE=1):
 *   Records parse/enqueue/wait/format phase timings plus per-slot wait
 *   p50/p95/max and emits one PROFILE_BCDB_BLOCK LOG line per block.
 *   Useful for diagnosing whether throughput is bottlenecked on enqueue,
 *   worker execution, or post-execution formatting.
 *
 * Returns: a palloc'd string in the caller's memory context.
 */
char *
bcdb_middleware_submit_block_results(const char* block_json)
{
	BCBlock     *block;
	BCBlock     *result_block;
	BCDBBlockResultRef *tx_refs = NULL;
	StringInfoData out;
	bool        profile = bcdb_block_profile_enabled();
	int         num_tx;
	BCBlockID  block_id;
	/* Profiling timestamps (microsecond resolution).  All "_us" suffixed. */
	uint64      t_start_us = 0;
	uint64      t_parse_us = 0;
	uint64      t_enqueue_us = 0;
	uint64      t_wait_us = 0;
	uint64      t_format_us = 0;
	uint64      block_wait_us = 0;
	uint64     *slot_wait_us = NULL;    /* per-tx fallback wait timings */
	uint64      slot_wait_sum_us = 0;
	uint64      slot_wait_p50_us = 0;
	uint64      slot_wait_p95_us = 0;
	uint64      slot_wait_max_us = 0;
	bool        force_actual_results = false;
	bool        return_actual_results = false;
	bool        apply_merkle_synchronously = false;
	MerkleRaftTargetRef *merkle_targets = NULL;
	int         merkle_target_count = 0;

	if (profile)
		t_start_us = bcdb_get_time();

	/* Phase 1: parse JSON -> shared-memory block + tx entries. */
	block = parse_block_with_txs(block_json);
	if (block == NULL)
		ereport(ERROR,
				(errmsg("failed to parse BCDB block JSON")));
	/* Bump the global block watermark so cleaning logic sees this block. */
	__sync_add_and_fetch(&block_meta->global_bmax, 1);

	/* Sentinel block holds the result ring; it must already exist. */
	result_block = bcdb_get_block1();
	if (result_block == NULL)
		ereport(ERROR,
				(errmsg("BCDB result block is not initialized")));

	/*
	 * Copy the immutable fields needed by the waiter/formatter before
	 * enqueueing.  From tx_queue_insert() onward, workers may finish and
	 * delete BCDBShmXact entries, and sufficiently deep pipelines may reclaim
	 * old block headers before this backend returns to the gateway.
	 */
	num_tx = block->num_tx;
	block_id = block->id;
	if (num_tx > 0)
	{
		tx_refs = (BCDBBlockResultRef *) palloc0(sizeof(BCDBBlockResultRef) * num_tx);
		for (int i = 0; i < num_tx; ++i)
		{
			BCDBShmXact *tx = block->txs[i];

			Assert(tx != NULL);
			tx_refs[i].tx_id = tx->tx_id;
			strlcpy(tx_refs[i].hash, tx->hash, sizeof(tx_refs[i].hash));
			tx_refs[i].raft_ledger_enabled = tx->raft_ledger_enabled;
			if (tx->raft_ledger_enabled)
			{
				apply_merkle_synchronously = true;
				tx_refs[i].raft_log_index = tx->raft_log_index;
				tx_refs[i].raft_item_ordinal = tx->raft_item_ordinal;
				memcpy(tx_refs[i].raft_epoch_id, tx->raft_epoch_id,
					   BCDB_RAFT_DIGEST_BYTES);
			}
		}
	}
	if (apply_merkle_synchronously && synchronous_commit == SYNCHRONOUS_COMMIT_OFF)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("safe-ledger Merkle results require synchronous_commit=on"),
				 errhint("Commit the block with synchronous_commit enabled.")));
	if (profile)
		t_parse_us = bcdb_get_time();

	/* Allocate per-tx wait array only when profiling -- avoids palloc cost
	 * on the hot path when profiling is disabled. */
	if (profile && num_tx > 0)
		slot_wait_us = (uint64 *) palloc0(sizeof(uint64) * num_tx);

	/* Phase 2: enqueue every tx.  Parsing can overlap across PostgreSQL
	 * backends, but tx_queue_insert() must expose blocks to worker FIFO queues
	 * in block-id order or a higher txid can block a lower one behind it. */
	bcdb_wait_for_block_enqueue_turn(block_id);
	for (int i = 0; i < num_tx; ++i)
	{
		BCDBShmXact *tx = block->txs[i];
		tx_queue_insert(tx, tx->tx_id);
		if (bcdb_block_enqueue_yield_every() > 0 &&
			((i + 1) % bcdb_block_enqueue_yield_every()) == 0)
			pg_usleep(1);
	}
	bcdb_advance_block_enqueue_turn(block_id);
	if (profile)
		t_enqueue_us = bcdb_get_time();

	/* Phase 3: wait for every result slot in this block to be published.
	 * Two equivalent strategies (see bcdb_block_wait_watermark_enabled docs);
	 * both ensure every block-local slot is readable before we read it. */
	if (num_tx > 0)
	{
		BCTxID last_tx_id = tx_refs[num_tx - 1].tx_id;

		if (bcdb_block_wait_watermark_enabled())
		{
			/* Watermark mode: workers publish slot before advancing the
			 * contiguous committed watermark, so a watermark >= last_tx_id
			 * implies all slots in [first..last] are also published. */
			block_wait_us = bcdb_wait_until_committed(last_tx_id);
		}
		else
		{
			/* Block-scan mode: one amortised wait that polls every slot
			 * per backoff iteration. */
			block_wait_us = bcdb_wait_until_block_slots_ready(tx_refs, num_tx, block_id);
		}
	}

	/*
	 * Worker result publication happens only after the heap change and its
	 * terminal delta blob commit together.  Apply the now-visible contiguous
	 * prefix once for the whole submitted block before this SQL function can
	 * return to the gateway.  The outer PostgreSQL commit therefore makes the
	 * Generic WAL pages and durable apply watermark synchronous with the
	 * externally visible block result while retaining page-level batching.
	 */
	if (apply_merkle_synchronously && enable_merkle_index)
	{
		uint64 required_seq = 0;
		uint64 applied_seq;

		merkle_targets = palloc0(sizeof(*merkle_targets) * num_tx);
		for (int i = 0; i < num_tx; i++)
		{
			int target_index;

			if (!tx_refs[i].raft_ledger_enabled)
				continue;
			for (target_index = 0; target_index < merkle_target_count;
				 target_index++)
				if (merkle_targets[target_index].raft_log_index ==
					tx_refs[i].raft_log_index &&
					memcmp(merkle_targets[target_index].epoch_id,
						   tx_refs[i].raft_epoch_id,
						   BCDB_RAFT_DIGEST_BYTES) == 0)
					break;
			if (target_index == merkle_target_count)
			{
				memcpy(merkle_targets[target_index].epoch_id,
					   tx_refs[i].raft_epoch_id, BCDB_RAFT_DIGEST_BYTES);
				merkle_targets[target_index].raft_log_index =
					tx_refs[i].raft_log_index;
				merkle_targets[target_index].max_item_ordinal =
					tx_refs[i].raft_item_ordinal;
				merkle_target_count++;
			}
			else if (tx_refs[i].raft_item_ordinal >
					 merkle_targets[target_index].max_item_ordinal)
				merkle_targets[target_index].max_item_ordinal =
					tx_refs[i].raft_item_ordinal;
		}

		for (int i = 0; i < merkle_target_count; i++)
		{
			uint64 target = merkle_raft_apply_target(
				merkle_targets[i].epoch_id,
				merkle_targets[i].raft_log_index,
				merkle_targets[i].max_item_ordinal);

			if (target > required_seq)
				required_seq = target;
		}
		applied_seq = merkle_apply_until_internal(required_seq);
		if (applied_seq < required_seq)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("Merkle synchronous apply stopped before this block's committed position"),
					 errdetail("applied_seq=%llu required_seq=%llu",
							   (unsigned long long) applied_seq,
							   (unsigned long long) required_seq),
					 errhint("An earlier Raft item is not terminal; retry the block after recovery catches up.")));
		pfree(merkle_targets);
		merkle_targets = NULL;
	}

	/* Phase 4: format the completion payload and mark slots consumed. */
	initStringInfo(&out);

	/* Determine whether any tx in this block requires safe-ledger actual
	 * results.  Use the pre-enqueue snapshot in tx_refs[] — block->txs[]
	 * must not be accessed after tx_queue_insert() because workers can
	 * free transactions and reclaim blocks at any point after enqueue. */
	for (int i = 0; i < num_tx; ++i)
	{
		if (tx_refs[i].raft_ledger_enabled)
		{
			force_actual_results = true;
			break;
		}
	}
	return_actual_results =
		force_actual_results || bcdb_block_return_actual_results_enabled();

	for (int i = 0; i < num_tx; ++i)
	{
		const BCTxID tx_id = tx_refs[i].tx_id;
		const int mem_txid = bcdb_result_slot_for_txid(tx_id);
		BCTxID published;
		uint64 wait_us = 0;

		/* ACQUIRE-load the slot owner.  This must exactly equal tx_id;
		 * any other value means the slot is owned by a different tx. */
		published = __atomic_load_n(&result_block->result_committed_txid[mem_txid],
									 __ATOMIC_ACQUIRE);
		if (published != tx_id)
		{
			/* Defensive: block-level wait above should have made this
			 * unreachable.  Fall back to a per-slot wait so we never
			 * read a recycled slot.  Mismatch here would indicate
			 * an ordering bug in the worker publish path. */
			wait_us = bcdb_wait_until_slot_ready(tx_id);
		}
		if (profile)
		{
			slot_wait_sum_us += wait_us;
			if (wait_us > slot_wait_max_us)
				slot_wait_max_us = wait_us;
			if (slot_wait_us != NULL)
				slot_wait_us[i] = wait_us;
		}

		/* Always emit the hash; emit the row payload when forced by a
		 * safe-ledger transaction or when explicitly requested via
		 * BCDB_BLOCK_RETURN_ACTUAL_RESULTS. */
		appendStringInfoString(&out, tx_refs[i].hash);
		appendStringInfoChar(&out, '\t');
		if (return_actual_results)
		{
			const char *result_text = result_block->result[mem_txid];

			if (tx_refs[i].raft_ledger_enabled && result_text[0] == '\0')
				elog(ERROR,
					 "safe-ledger result missing after committed slot "
					 "tx_id=%d slot=%d",
					 (int) tx_id,
					 mem_txid);

			append_hex_encoded(&out, result_text);
		}
		appendStringInfoChar(&out, '\n');

		/* Hand the slot back: publish result_consumed_txid = tx_id with
		 * RELEASE so a worker writing the next tx into this slot knows
		 * the previous occupant has been fully consumed. */
		__atomic_store_n(&result_block->result_consumed_txid[mem_txid],
						 (int32) tx_id, __ATOMIC_RELEASE);
	}

	/* Phase 5 (optional): emit profiling LOG line. */
	if (profile)
	{
		uint64 t_done_us;
		uint64 total_us;

		t_wait_us = block_wait_us + slot_wait_sum_us;
		if (slot_wait_us != NULL && num_tx > 0)
		{
			/* Compute p50/p95 over per-slot fallback wait timings. */
			int p50_idx = num_tx / 2;
			int p95_idx = (num_tx * 95) / 100;

			if (p95_idx >= num_tx)
				p95_idx = num_tx - 1;
			qsort(slot_wait_us, num_tx, sizeof(uint64), bcdb_uint64_cmp);
			slot_wait_p50_us = slot_wait_us[p50_idx];
			slot_wait_p95_us = slot_wait_us[p95_idx];
		}
		t_done_us = bcdb_get_time();
		/* Format time = total - (parse + enqueue + wait).  Guard against
		 * negative arithmetic in case of clock jitter. */
		t_format_us = (t_done_us > t_enqueue_us + t_wait_us)
			? (t_done_us - t_enqueue_us - t_wait_us)
			: 0;
		total_us = t_done_us - t_start_us;

		ereport(LOG,
				(errmsg("PROFILE_BCDB_BLOCK pid=%d block_txs=%d total_ms=%.3f parse_ms=%.3f enqueue_ms=%.3f wait_block_ms=%.3f wait_slot_ms=%.3f format_ms=%.3f slot_wait_avg_us=%.3f slot_wait_p50_us=%lu slot_wait_p95_us=%lu slot_wait_max_us=%lu active_BCDB_workers_current=%lu active_BCDB_workers_max=%lu overlapping_BCDB_optimistic_execution=%lu",
						(int) getpid(),
						num_tx,
						total_us / 1000.0,
						(t_parse_us - t_start_us) / 1000.0,
						(t_enqueue_us - t_parse_us) / 1000.0,
						block_wait_us / 1000.0,
						t_wait_us / 1000.0,
						t_format_us / 1000.0,
						num_tx > 0
							? ((double) slot_wait_sum_us / (double) num_tx)
							: 0.0,
						(unsigned long) slot_wait_p50_us,
						(unsigned long) slot_wait_p95_us,
						(unsigned long) slot_wait_max_us,
						(unsigned long) __atomic_load_n(&block_meta->active_bcdb_workers,
														 __ATOMIC_RELAXED),
						(unsigned long) __atomic_load_n(&block_meta->active_bcdb_workers_max,
														 __ATOMIC_RELAXED),
						(unsigned long) __atomic_load_n(&block_meta->overlapping_bcdb_optimistic_execution,
														 __ATOMIC_RELAXED))));
		if (slot_wait_us != NULL)
			pfree(slot_wait_us);
	}
	if (tx_refs != NULL)
	{
		if (bcdb_gate_snapshot_each_block)
			bcdb_log_gate_snapshot("completed_block", block_id, tx_refs[0].tx_id, tx_refs[num_tx - 1].tx_id);
		pfree(tx_refs);
	}
	else
	{
		if (bcdb_gate_snapshot_each_block)
			bcdb_log_gate_snapshot("completed_block", block_id, -1, -1);
	}

	elog(LOG,
		 "SAFE_BLOCK_RETURN block=%d safe=%d actual=%d bytes=%d",
		 (int) block_id,
		 force_actual_results ? 1 : 0,
		 return_actual_results ? 1 : 0,
		 out.len);
	return out.data;
}

/*
 * bcdb_middleware_submit_block2
 * -----------------------------
 * Legacy burst-submit variant of the block-submit API.  Unlike
 * submit_block_results(), this function:
 *
 *   - intentionally pauses for `burstTime` microseconds after every
 *     `numTxBurst` enqueues (the throttle controls captured in
 *     bcdb_middleware_init2()),
 *   - does not wait for any tx to complete,
 *   - does not return any result payload.
 *
 * Used by older experiments that wanted to feed transactions to workers
 * at a controlled producer rate (to study queue depths, batching effects,
 * etc.).  The normal distributed YCSB/TPCC pipeline does not use this.
 *
 * Like the other submit paths it follows the parse-before-counter-update
 * rule (parse_block_with_txs() reserves the tx-id range before we touch
 * any worker queue).
 */
void
bcdb_middleware_submit_block2(const char* block_json)
{
	BCBlock     *block;
	struct timeval tv1 ;
	tv1.tv_sec = 0; tv1.tv_usec = 0;

#if SAFEDBG
	printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
#endif
	block = parse_block_with_txs(block_json);
	if (block == NULL)
		ereport(ERROR,
				(errmsg("failed to parse BCDB block JSON")));
	__sync_add_and_fetch(&block_meta->global_bmax, 1);

	/* Push txs at a throttled rate: after every numTxBurst enqueues we sleep
	 * for burstTime microseconds.  The "(i > 0)" guard prevents an extra
	 * sleep at i == 0 when i % numTxBurst is trivially zero. */
	for (int i=0; i < block->num_tx; i++)
	{
		tx_queue_insert(block->txs[i], block->txs[i]->tx_id);
		if ((i % numTxBurst == 0) && (i > 0))
		{
#if SAFEDBG
			gettimeofday(&tv1, NULL);
			printf("\n\n\t time= %ld.%ld  getpid %d\n", tv1.tv_sec, tv1.tv_usec, getpid());
			printf("\t ariaMyDbg %s : %s: %d pid %d  sleeping %dms next burstSz %d from tx %d\n\n", __FILE__, __FUNCTION__, __LINE__ , getpid() ,burstTime, numTxBurst, i );
#endif
			usleep(burstTime);
		}
	}
#if SAFEDBG
	printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
#endif
}

/*
 * bcdb_wait_tx_finish
 * -------------------
 * Block the caller until the named tx reaches TX_COMMITED or TX_ABORTED.
 *
 * This is the legacy hash-addressed compatibility API (called from the
 * older bcdb_wait_tx_finish() SQL wrapper).  Modern callers should prefer
 * the block-submit results path, which waits on slot publication rather
 * than per-tx condition variables.
 *
 * Robustness details:
 *   - A missing hash is reported via ereport(ERROR) rather than a
 *     null-pointer dereference.  Returning silently would conflate
 *     "never submitted" with "submitted and aborted".
 *   - The wait is open-ended (no caller-supplied deadline).  To avoid
 *     truly invisible hangs, we use ConditionVariableTimedSleep with a
 *     1 ms cap and emit a LOG line every 5 s reporting the current tx
 *     id and status.  That makes worker crashes or lost CV wakeups
 *     visible without changing the SQL surface contract.
 *   - The Prepare/Sleep/Cancel envelope is REQUIRED by PostgreSQL's
 *     condition-variable contract -- it registers this backend on the
 *     CV waiter list so a concurrent ConditionVariableBroadcast()
 *     reliably wakes us.
 */
void
bcdb_wait_tx_finish(char *tx_hash)
{
	BCDBShmXact *tx;
	uint64 wait_start_us;
	uint64 next_warn_us;

	tx = get_tx_by_hash(tx_hash);
	if (tx == NULL)
		ereport(ERROR,
				(errmsg("BCDB transaction not found: %s", tx_hash)));
	wait_start_us = bcdb_get_time();
	next_warn_us = wait_start_us + 5000000; /* first warning at +5 s */
	ConditionVariablePrepareToSleep(&tx->cond);
	while(tx->status != TX_COMMITED && tx->status != TX_ABORTED)
	{
		uint64 now_us;

		/* 1 ms timed sleep keeps the watchdog responsive even if a
		 * broadcast wakeup is lost. */
		ConditionVariableTimedSleep(&tx->cond, 1000L, WAIT_EVENT_TX_FINISH);
		CHECK_FOR_INTERRUPTS();
		now_us = bcdb_get_time();
		if (now_us >= next_warn_us)
		{
			ereport(LOG,
					(errmsg("[BCDB_HANG] tx_finish_wait_stuck pid=%d tx_hash=%s tx_id=%d status=%d waited_us=%lu",
							(int) getpid(), tx_hash, (int) tx->tx_id,
							(int) tx->status,
							(unsigned long) (now_us - wait_start_us))));
			next_warn_us = now_us + 5000000;
		}
	}
	ConditionVariableCancelSleep();
}

/*
 * bcdb_middleware_wait_all_to_finish
 * ----------------------------------
 * Block the caller until every submitted block has been committed AND log
 * the aggregate throughput.
 *
 * Implementation: WaitGlobalBmin(target) sleeps until block_meta->global_bmin
 * (the lowest active block) advances past `target`.  We pass global_bmax+1,
 * meaning "wait until no submitted block remains in flight".
 *
 * Throughput formula: num_committed / elapsed seconds, where elapsed is
 * computed against start_time captured in bcdb_init().  Used by benchmark
 * harnesses for an end-of-run summary line.
 */
void
bcdb_middleware_wait_all_to_finish()
{
	WaitGlobalBmin(block_meta->global_bmax + 1);
	ereport(LOG, (errmsg("[ZL] total throughput: %.3f",
		(double)block_meta->num_committed * 1e6 / (bcdb_get_time() - start_time))));
}

/*
 * bcdb_middleware_set_txs_committed_block
 * ---------------------------------------
 * Attach a previously submitted (hash-addressed) tx to a block.
 *
 * Supports the older two-step submission workflow:
 *   step 1: bcdb_tx_submit(tx_json)                -- creates the tx
 *   step 2: bcdb_add_tx_with_block_id(hash, bid)   -- assigns the block
 *
 * Idempotency / reattachment policy:
 *   - Attaching a tx to the SAME block it already belongs to is a no-op
 *     and returns silently (callers may retry on transient errors).
 *   - Attaching a tx to a DIFFERENT block when it already has a real
 *     block id is rejected -- worker queues and conflict metadata may
 *     already reference the original block membership, and silently
 *     moving the tx would corrupt that state.
 *   - The "no block yet" sentinel values are BCDBInvalidBid and
 *     BCDBMaxBid; either is treated as unattached.
 */
void
bcdb_middleware_set_txs_committed_block(char * tx_hash, int32 block_id)
{
	BCDBShmXact *tx;
	BCBlock     *block;
	tx = get_tx_by_hash(tx_hash);
	if (tx == NULL)
		ereport(ERROR,
				(errmsg("BCDB transaction not found: %s", tx_hash)));
	if (tx->block_id_committed == block_id)
		return; /* idempotent */
	if (tx->block_id_committed != BCDBInvalidBid &&
		tx->block_id_committed != BCDBMaxBid)
		ereport(ERROR,
				(errmsg("BCDB transaction %s already belongs to block %d",
						tx_hash, tx->block_id_committed)));
	block = get_block_by_id(block_id, true);
	bcdb_middleware_attach_tx_to_block(tx, block);
}

/*
 * bcdb_middleware_attach_tx_to_block
 * ----------------------------------
 * Internal helper: add `tx` into `block`'s tx array and stamp the block id
 * onto the tx.
 *
 * Locking is handled inside block_add_tx(), which owns the lock protecting
 * block->txs[] and block->num_tx.  This helper assumes the caller has
 * already validated that the tx exists and is not already attached to a
 * different block (see bcdb_middleware_set_txs_committed_block).
 */
void
bcdb_middleware_attach_tx_to_block(BCDBShmXact *tx, BCBlock *block)
{
	block_add_tx(block, tx);
	tx->block_id_committed = block->id;
}

/*
 * block_cleaning
 * --------------
 * Reclaim old non-DT block headers and their txs, AND emit periodic
 * throughput/abort-rate telemetry.
 *
 * NOTE: This is NOT a read-only status function.  It actively deletes txs
 * and block headers once the block window has advanced past
 * CLEANING_DELAY_BLOCKS.  Use block_cleaning_dt() instead on the
 * deterministic execution path -- DT workers reclaim individual tx-pool
 * entries themselves, so deleting them again here would double-free.
 *
 * Two responsibilities, on independent triggers:
 *   1. GC: when current_block_id > CLEANING_DELAY_BLOCKS, the block at
 *      offset `current - delay` ages out.  Block id 1 (sentinel) is
 *      explicitly skipped to preserve runtime metadata.
 *   2. Reporting: every REPORT_INTERVAL seconds, log throughput
 *      (committed delta / elapsed) and the running abort rate.  The
 *      first call only seeds the timestamps -- the rate report skips
 *      on previous_report_ts == 0 because the delta would be bogus.
 *
 * LOG_STATUS (compile-time): if enabled, every cleaned tx's
 * "<hash> <status>\n" line is appended to a 10 MiB in-memory log buffer
 * for offline analysis; overflow is FATAL to make truncation impossible
 * to miss.
 */
void
block_cleaning(BCBlockID current_block_id)
{
	BCBlock *block_to_clean;
	uint64 cur_report_ts = bcdb_get_time();
	int32  cur_num_committed = block_meta->num_committed;
	int32  total_finished = block_meta->num_aborted + block_meta->num_committed;
	float abort_rate = (total_finished > 0)
		? (float) block_meta->num_aborted / total_finished
		: 0.0f;
#if SAFEDBG
	printf("\nariaMyDbg %s : %s: %d \n", __FILE__, __FUNCTION__, __LINE__ );
	printf("ariaMyDbg %s : %s: %d \n\n", __FILE__, __FUNCTION__, __LINE__ );
#endif

	/* GC: reclaim a block once it's CLEANING_DELAY_BLOCKS behind the head. */
	if (current_block_id > CLEANING_DELAY_BLOCKS)
	{
		BCBlockID clean_block_id = current_block_id - CLEANING_DELAY_BLOCKS;

		/* Skip the sentinel (id 1) -- it permanently holds runtime metadata
		 * and the result ring, and must never be freed. */
		block_to_clean = (clean_block_id == 1)
			? NULL
			: get_block_by_id(clean_block_id, false);
		if (block_to_clean != NULL)
		{
			for (int i=0; i < block_to_clean->num_tx; i++)
			{
#ifdef LOG_STATUS
				block_meta->log_counter += sprintf(block_meta->log + block_meta->log_counter, "%s %d\n", block_to_clean->txs[i]->hash, block_to_clean->txs[i]->status);
				if (block_meta->log_counter > 1024 * 1024 * 10) /* 10 MiB cap */
					ereport(FATAL, (errmsg("[ZL] log overflow")));
#endif
				delete_tx(block_to_clean->txs[i]);
			}
		}
		delete_block(block_to_clean);
	}

	/* Reporting: emit throughput/abort-rate every REPORT_INTERVAL seconds. */
	if (cur_report_ts - block_meta->previous_report_ts > 1e6 * REPORT_INTERVAL)
	{
		if (block_meta->previous_report_ts != 0)
		{
			/* Skip the first interval where we have no baseline timestamp. */
			ereport(LOG, (errmsg("[ZL] throughput: %.3f", (cur_num_committed - block_meta->previous_report_commit) * 1e6 / (cur_report_ts - block_meta->previous_report_ts))));
			ereport(LOG, (errmsg("[ZL] abort rate: %.3f", abort_rate)));
		}
		block_meta->previous_report_ts = cur_report_ts;
		block_meta->previous_report_commit = cur_num_committed;
	}
}

/*
 * block_cleaning_dt
 * -----------------
 * Deterministic-execution-safe equivalent of block_cleaning().
 *
 * In the DT pipeline, workers (worker.c) write final results into the
 * sentinel block's result ring and call delete_tx() themselves as each
 * BCDBShmXact is finalised.  By the time a per-block header ages out,
 * its block->txs[] entries may already point at freed tx-pool slots.
 *
 * Therefore this function ONLY reclaims the per-block header.  It must
 * NOT iterate block->txs[] and must NOT touch the sentinel (block id 1).
 *
 * Triggering: nothing to do until current_block_id has advanced past
 * CLEANING_DELAY_BLOCKS; the early return keeps the hot path cheap.
 */
void
block_cleaning_dt(BCBlockID current_block_id)
{
	BCBlock *block_to_clean;
	BCBlockID clean_block_id;

	/* Not enough history accumulated yet -- nothing to reclaim. */
	if (current_block_id <= CLEANING_DELAY_BLOCKS)
		return;

	clean_block_id = current_block_id - CLEANING_DELAY_BLOCKS;
	/* Sentinel is permanent. */
	if (clean_block_id == 1)
		return;

	block_to_clean = get_block_by_id(clean_block_id, false);
	delete_block(block_to_clean);
}

/*
 * allow_all_block_txs_to_commit
 * -----------------------------
 * Historical commit-release hook from the original BCDB design.
 *
 * The original protocol had a per-block "commit allowed" flag that workers
 * polled before persisting.  Current BCDB workers do not consult any such
 * flag -- the deterministic worker pipeline commits a tx as soon as it
 * passes conflict_checkDT() and finishes executing.
 *
 * We keep the function (and its callers) as an explicit no-op to preserve
 * the legacy SQL surface.  Adding a flag that no worker reads would be a
 * footgun: callers might think they were gating commits when they were not.
 */
void
allow_all_block_txs_to_commit(BCBlock *block)
{
	return;
}

/*
 * bcdb_middleware_conflict_check
 * ------------------------------
 * Historical conflict-check hook for the old block-submit API.
 *
 * Active deterministic execution performs conflict_checkDT() inside
 * worker.c as each queued tx is processed -- the conflict graph is built
 * and walked there, not here.  This hook therefore has no work to do.
 *
 * Kept as an explicit no-op (rather than removed) so legacy SQL wrappers
 * that still call it remain link-compatible.  Do NOT add real conflict
 * logic here -- it would race the worker's own conflict_checkDT().
 */
void
bcdb_middleware_conflict_check(BCBlock *block)
{
	/* No work: deterministic conflict resolution lives in worker.c. */
	return;
}


/*
 * bcdb_middleware_allow_txs_exec_write_set_and_commit
 * ---------------------------------------------------
 * Compatibility wrapper for the older "allow execute / allow write / allow
 * commit" three-phase SQL flow.
 *
 * In current BCDB only the commit-release step remains, and it is itself
 * a no-op (see allow_all_block_txs_to_commit above).  Active deterministic
 * execution does not use this code path at all.
 */
void bcdb_middleware_allow_txs_exec_write_set_and_commit(BCBlock *block) {
	allow_all_block_txs_to_commit(block);
}

/*
 * bcdb_middleware_allow_txs_exec_write_set_and_commit_by_id
 * ---------------------------------------------------------
 * Convenience wrapper that resolves a block id to a BCBlock* before
 * delegating to the function above.
 *
 * Uses ereport(ERROR) rather than Assert() for the missing-block case so
 * the diagnostic survives production builds (Asserts are compiled out
 * unless USE_ASSERT_CHECKING is defined).
 */
void bcdb_middleware_allow_txs_exec_write_set_and_commit_by_id(int32 id){
	BCBlock *block;

	block = get_block_by_id(id, false);
	if (block == NULL)
		ereport(ERROR,
				(errmsg("BCDB block %d not found", id)));
	bcdb_middleware_allow_txs_exec_write_set_and_commit(block);
}

/*
 * bcdb_is_tx_commited
 * -------------------
 * Legacy "is this hash-addressed tx committed?" SQL helper.
 *
 * Returns true iff status == TX_COMMITED.  Any other status (including
 * TX_ABORTED) returns false.
 *
 * Important: we ereport(ERROR) on an unknown hash rather than returning
 * false.  Conflating "never submitted" with "submitted and aborted" would
 * silently mask client/runner bugs that submit txs to the wrong replica.
 *
 * (Note: function name retains the original misspelling "commited"
 *  for ABI/SQL-binding compatibility.)
 */
bool bcdb_is_tx_commited(char * tx_hash){
	BCDBShmXact* target_tx = get_tx_by_hash(tx_hash);

	if (target_tx == NULL)
		ereport(ERROR,
				(errmsg("BCDB transaction not found: %s", tx_hash)));

	return (target_tx->status == TX_COMMITED);
}

/*
 * bcdb_clear_block_txs_store
 * --------------------------
 * Reset in-memory BCDB metadata to a fresh state.  Used by the restore /
 * benchmark setup phase between independent runs.
 *
 * What this clears:
 *   - shared-memory block pool (block_pool[]) and its bookkeeping;
 *   - shared-memory tx pool (clear_tx_pool());
 *   - counters: tx_num, num_committed, num_aborted, reporting checkpoints;
 *   - block window bounds: global_bmin = 1, global_bmax = 0;
 *   - debug_seq (incremented so post-reset events can be correlated);
 *   - the idle-worker list, finishing each WorkerController in turn so
 *     the worker processes shut down cleanly.
 *
 * What this does NOT clear:
 *   - SQL table data, indexes, Merkle state on disk;
 *   - distributed coordination state (Kafka offsets, Raft logs);
 *   - already-running worker processes that aren't on the idle list
 *     (they must be stopped externally first).
 *
 * Callers (the distributed runners and restore scripts) invoke this only
 * during controlled restart phases AFTER stopping any active workload, so
 * concurrent submission isn't a concern.  Calling this while another
 * backend is mid-submit will produce undefined behaviour.
 */
void
bcdb_clear_block_txs_store()
{
#if SAFEDBG
	printf("\nariaMyDbg %s : %s: %d \n", __FILE__, __FUNCTION__, __LINE__ );
	printf("ariaMyDbg %s : %s: %d \n\n", __FILE__, __FUNCTION__, __LINE__ );
#endif
	/* Wipe the block pool hash table and reset its internal allocation state. */
	shm_hash_clear(block_pool, MAX_NUM_BLOCKS);
	bcdb_reset_block_pool_state();

	/* Wipe the tx pool. */
	clear_tx_pool();

	/* Reset counters. */
	tx_num = 0;
	block_meta->global_bmin = 1;
	block_meta->global_bmax = 0;
	block_meta->debug_seq += 1; /* bumped so post-reset logs are distinguishable */
	block_meta->num_committed = 0;
	block_meta->num_aborted = 0;
	block_meta->previous_report_commit = 0;
	block_meta->previous_report_ts = 0;
	block_meta->next_enqueue_block_id = BCDB_FIRST_SUBMIT_BLOCK_ID;
	block_meta->active_bcdb_workers = 0;
	block_meta->active_bcdb_workers_max = 0;
	block_meta->overlapping_bcdb_optimistic_execution = 0;
	start_time = bcdb_get_time();
	set_num_tx_sub(0);
	set_num_txqd(0);

	/* Tear down idle workers: stop each controller process, remove from
	 * the list, and free its host-side struct.  Active workers (still
	 * processing txs) are not on this list, by definition. */
	while(!LIST_EMPTY(&idle_workers.list))
	{
		WorkerController *worker = LIST_FIRST(&idle_workers.list);
		worker_finish(worker);
		LIST_REMOVE(worker, link);
		pfree(worker);
	}
	idle_workers.num = 0;
}

/*
 * append_hex_encoded
 * ------------------
 * Lowercase hex-encode `input` and append it to `out`.
 *
 * Used by bcdb_middleware_submit_block_results() when
 * BCDB_BLOCK_RETURN_ACTUAL_RESULTS=1 to emit raw row payloads safely on
 * the same line as the tx hash -- arbitrary bytes (including \n and \t)
 * in a SQL row would otherwise break the newline/tab-delimited frontend
 * parser.  Hex encoding is uniform and trivially reversible.
 *
 * NULL inputs are a no-op (defensive against missing slot data).
 */
static void
append_hex_encoded(StringInfo out, const char *input)
{
	static const char kHex[] = "0123456789abcdef";
	const unsigned char *p = (const unsigned char *) input;

	if (input == NULL)
		return;

	while (*p)
	{
		/* Two hex nibbles per input byte. */
		appendStringInfoChar(out, kHex[(*p >> 4) & 0x0F]);
		appendStringInfoChar(out, kHex[*p & 0x0F]);
		++p;
	}
}

/*
 * ----------------------------------------------------------------------
 *  Vestigial declarations preserved for historical reference.
 *
 *  These four prototypes belonged to an earlier file-based "dummy block"
 *  workflow that read transactions from a JSON-per-line file and submitted
 *  them in batch.  The active SQL/JSON path replaced them long ago; the
 *  signatures are kept here only as breadcrumbs for anyone digging through
 *  git blame.  Do not re-enable without rewriting against the current
 *  parse_block_with_txs / submit_block_results contract.
 * ----------------------------------------------------------------------
 *
 *   void bcdb_middleware_new_block_handler(BCBlock* block);
 *
 *   // assume dummy file contains jsons per line
 *   Transaction* parsing_dummy_block_file(const char* file_path);
 *
 *   // dummy function called by frontend
 *   void bcdb_middleware_dummy_block(const char* file_path, uint32 block_id);
 *
 *   void bcdb_middleware_dummy_submit_tx(const char* file_path);
 *
 *   // Returned false if 1) no tx with that hash, or 2) tx had not finished
 *   // execution.  Replaced by bcdb_is_tx_commited() above, which now
 *   // ereport(ERROR)s on the missing-hash case to avoid masking client bugs.
 * ----------------------------------------------------------------------
 */
