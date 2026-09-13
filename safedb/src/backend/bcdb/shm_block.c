#include "bcdb/shm_block.h"
#include "postgres.h"
#include "miscadmin.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"
#include <sys/queue.h>

/*
 * Silence ad-hoc stdout debug prints in the deterministic shared-memory block
 * machinery to avoid corrupting frontend protocol responses.
 */
#undef printf
#define printf(...) ((void) 0)

/*
 * Shared-memory sharded gate statistics array.
 */
BCDBGatesStatsShard *bcdb_gate_stats_shards = NULL;

/* --------------------------------------------------------------------------
 * bcdb_log_gate_snapshot
 *
 * Emits a PROFILE_BCDB_GATE log line with a point-in-time snapshot of every
 * gate counter and the current watermark state.  All reads are atomic with
 * RELAXED ordering (acceptable for diagnostic purposes).
 *
 * Called:
 *   - after each completed block (middleware.c)
 *   - on every watchdog interval
 *   - before bcdb_reset_block_pool_state (debug only)
 * --------------------------------------------------------------------------
 */
void
bcdb_log_gate_snapshot(const char *reason,
						BCBlockID block_id,
						BCTxID first_txid,
						BCTxID last_txid)
{
	BCBlock *blk = get_block_by_id(1, false);
	BCTxID   published     = blk ? (BCTxID)__atomic_load_n(&blk->published_max_tx_id, __ATOMIC_RELAXED) : -1;
	BCTxID   last_committed = blk ? (BCTxID)__atomic_load_n(&blk->last_committed_tx_id, __ATOMIC_RELAXED) : -1;
	BCBlockID next_enqueue  = block_meta ?
		(BCBlockID)__atomic_load_n(&block_meta->next_enqueue_block_id, __ATOMIC_RELAXED) : -1;

	uint64 agg_serial_gate_calls = 0;
	uint64 agg_serial_gate_wait_total_us = 0;
	uint64 agg_serial_gate_wait_max_us = 0;
	uint64 agg_serial_gate_cv_sleep_count = 0;
	uint64 agg_serial_gate_spin_iterations = 0;

	uint64 agg_commit_advance_calls = 0;
	uint64 agg_commit_initial_cas_failures = 0;
	uint64 agg_commit_prefix_steps = 0;
	uint64 agg_commit_broadcast_count = 0;

	uint64 agg_published_ready_calls = 0;
	uint64 agg_published_ready_prefix_steps = 0;
	uint64 agg_published_ready_cas_failures = 0;

	uint64 agg_block_enqueue_turn_calls = 0;
	uint64 agg_block_enqueue_turn_wait_total_us = 0;
	uint64 agg_block_enqueue_turn_max_us = 0;

	uint64 agg_block_watermark_wait_calls = 0;
	uint64 agg_block_watermark_wait_total_us = 0;
	uint64 agg_block_watermark_wait_max_us = 0;

	uint64 agg_block_slot_wait_calls = 0;
	uint64 agg_block_slot_wait_total_us = 0;
	uint64 agg_block_slot_wait_max_us = 0;

	uint64 agg_result_slot_consumable_wait_calls = 0;
	uint64 agg_result_slot_consumable_wait_total_us = 0;
	uint64 agg_result_slot_consumable_wait_max_us = 0;

	uint64 agg_slot_fallback_wait_calls = 0;
	uint64 agg_slot_fallback_wait_total_us = 0;
	uint64 agg_slot_fallback_wait_max_us = 0;

	uint64 agg_prev_commit_wait_calls = 0;
	uint64 agg_prev_commit_wait_total_us = 0;
	uint64 agg_prev_commit_wait_max_us = 0;

	uint64 agg_target_commit_wait_calls = 0;
	uint64 agg_target_commit_wait_total_us = 0;
	uint64 agg_target_commit_wait_max_us = 0;

	int active_serial_waiters = 0;
	uint64 oldest_active_wait_us = 0;
	int64 oldest_active_txid = -1;
	int64 oldest_active_block_id = -1;
	int oldest_active_phase = 0;

	if (bcdb_gate_stats_shards != NULL)
	{
		uint64 now_us = bcdb_get_time();
		int num_shards = bcdb_worker_count + MaxBackends;
		for (int i = 0; i < num_shards; i++)
		{
			BCDBGatesStatsShard *shard = &bcdb_gate_stats_shards[i];
			int phase;

			agg_serial_gate_calls += __atomic_load_n(&shard->serial_gate_calls, __ATOMIC_RELAXED);
			agg_serial_gate_wait_total_us += __atomic_load_n(&shard->serial_gate_wait_total_us, __ATOMIC_RELAXED);
			{
				uint64 s_max = __atomic_load_n(&shard->serial_gate_wait_max_us, __ATOMIC_RELAXED);
				if (s_max > agg_serial_gate_wait_max_us) agg_serial_gate_wait_max_us = s_max;
			}
			agg_serial_gate_cv_sleep_count += __atomic_load_n(&shard->serial_gate_cv_sleep_count, __ATOMIC_RELAXED);
			agg_serial_gate_spin_iterations += __atomic_load_n(&shard->serial_gate_spin_iterations, __ATOMIC_RELAXED);

			agg_commit_advance_calls += __atomic_load_n(&shard->commit_advance_calls, __ATOMIC_RELAXED);
			agg_commit_initial_cas_failures += __atomic_load_n(&shard->commit_initial_cas_failures, __ATOMIC_RELAXED);
			agg_commit_prefix_steps += __atomic_load_n(&shard->commit_prefix_steps, __ATOMIC_RELAXED);
			agg_commit_broadcast_count += __atomic_load_n(&shard->commit_broadcast_count, __ATOMIC_RELAXED);

			agg_published_ready_calls += __atomic_load_n(&shard->published_ready_calls, __ATOMIC_RELAXED);
			agg_published_ready_prefix_steps += __atomic_load_n(&shard->published_ready_prefix_steps, __ATOMIC_RELAXED);
			agg_published_ready_cas_failures += __atomic_load_n(&shard->published_ready_cas_failures, __ATOMIC_RELAXED);

			agg_block_enqueue_turn_calls += __atomic_load_n(&shard->block_enqueue_turn_calls, __ATOMIC_RELAXED);
			agg_block_enqueue_turn_wait_total_us += __atomic_load_n(&shard->block_enqueue_turn_wait_total_us, __ATOMIC_RELAXED);
			{
				uint64 e_max = __atomic_load_n(&shard->block_enqueue_turn_wait_max_us, __ATOMIC_RELAXED);
				if (e_max > agg_block_enqueue_turn_max_us) agg_block_enqueue_turn_max_us = e_max;
			}

			agg_block_watermark_wait_calls += __atomic_load_n(&shard->block_watermark_wait_calls, __ATOMIC_RELAXED);
			agg_block_watermark_wait_total_us += __atomic_load_n(&shard->block_watermark_wait_total_us, __ATOMIC_RELAXED);
			{
				uint64 w_max = __atomic_load_n(&shard->block_watermark_wait_max_us, __ATOMIC_RELAXED);
				if (w_max > agg_block_watermark_wait_max_us) agg_block_watermark_wait_max_us = w_max;
			}

			agg_block_slot_wait_calls += __atomic_load_n(&shard->block_slot_wait_calls, __ATOMIC_RELAXED);
			agg_block_slot_wait_total_us += __atomic_load_n(&shard->block_slot_wait_total_us, __ATOMIC_RELAXED);
			{
				uint64 sl_max = __atomic_load_n(&shard->block_slot_wait_max_us, __ATOMIC_RELAXED);
				if (sl_max > agg_block_slot_wait_max_us) agg_block_slot_wait_max_us = sl_max;
			}

			agg_result_slot_consumable_wait_calls += __atomic_load_n(&shard->result_slot_consumable_wait_calls, __ATOMIC_RELAXED);
			agg_result_slot_consumable_wait_total_us += __atomic_load_n(&shard->result_slot_consumable_wait_total_us, __ATOMIC_RELAXED);
			{
				uint64 c_max = __atomic_load_n(&shard->result_slot_consumable_wait_max_us, __ATOMIC_RELAXED);
				if (c_max > agg_result_slot_consumable_wait_max_us) agg_result_slot_consumable_wait_max_us = c_max;
			}

			agg_slot_fallback_wait_calls += __atomic_load_n(&shard->slot_fallback_wait_calls, __ATOMIC_RELAXED);
			agg_slot_fallback_wait_total_us += __atomic_load_n(&shard->slot_fallback_wait_total_us, __ATOMIC_RELAXED);
			{
				uint64 max = __atomic_load_n(&shard->slot_fallback_wait_max_us, __ATOMIC_RELAXED);
				if (max > agg_slot_fallback_wait_max_us) agg_slot_fallback_wait_max_us = max;
			}

			agg_prev_commit_wait_calls += __atomic_load_n(&shard->prev_commit_wait_calls, __ATOMIC_RELAXED);
			agg_prev_commit_wait_total_us += __atomic_load_n(&shard->prev_commit_wait_total_us, __ATOMIC_RELAXED);
			{
				uint64 max = __atomic_load_n(&shard->prev_commit_wait_max_us, __ATOMIC_RELAXED);
				if (max > agg_prev_commit_wait_max_us) agg_prev_commit_wait_max_us = max;
			}

			agg_target_commit_wait_calls += __atomic_load_n(&shard->target_commit_wait_calls, __ATOMIC_RELAXED);
			agg_target_commit_wait_total_us += __atomic_load_n(&shard->target_commit_wait_total_us, __ATOMIC_RELAXED);
			{
				uint64 max = __atomic_load_n(&shard->target_commit_wait_max_us, __ATOMIC_RELAXED);
				if (max > agg_target_commit_wait_max_us) agg_target_commit_wait_max_us = max;
			}

			phase = __atomic_load_n(&shard->active_wait_phase, __ATOMIC_ACQUIRE);
			if (phase == BCDB_GATE_PHASE_SERIAL)
			{
				active_serial_waiters++;
			}
			if (phase != BCDB_GATE_PHASE_NONE)
			{
				uint64 start = __atomic_load_n(&shard->active_wait_start_us, __ATOMIC_ACQUIRE);
				if (start > 0 && now_us >= start)
				{
					uint64 wait_dur = now_us - start;
					if (wait_dur > oldest_active_wait_us)
					{
						oldest_active_wait_us = wait_dur;
						oldest_active_txid = __atomic_load_n(&shard->active_wait_txid, __ATOMIC_RELAXED);
						oldest_active_block_id = __atomic_load_n(&shard->active_wait_block_id, __ATOMIC_RELAXED);
						oldest_active_phase = phase;
					}
				}
			}
		}
	}

	ereport(LOG,
		(errmsg("PROFILE_BCDB_GATE reason=%s"
				" block_id=%d first_txid=%d last_txid=%d"
				" published_max=%d last_committed=%d next_enqueue_block=%d"
				" serial_gate_calls=%lu serial_gate_wait_total_us=%lu"
				" serial_gate_wait_max_us=%lu serial_gate_cv_sleep_count=%lu"
				" serial_gate_spin_iters=%lu"
				" commit_advance_calls=%lu commit_cas_failures=%lu"
				" commit_prefix_steps=%lu commit_broadcast_count=%lu"
				" published_ready_calls=%lu published_ready_prefix_steps=%lu"
				" published_ready_cas_failures=%lu"
				" enqueue_turn_calls=%lu enqueue_turn_wait_us=%lu enqueue_turn_max_us=%lu"
				" watermark_wait_calls=%lu watermark_wait_us=%lu watermark_wait_max_us=%lu"
				" slot_wait_calls=%lu slot_wait_us=%lu slot_wait_max_us=%lu"
				" consumable_wait_calls=%lu consumable_wait_us=%lu consumable_wait_max_us=%lu"
				" slot_fallback_wait_calls=%lu slot_fallback_wait_us=%lu slot_fallback_wait_max_us=%lu"
				" prev_commit_wait_calls=%lu prev_commit_wait_us=%lu prev_commit_wait_max_us=%lu"
				" target_commit_wait_calls=%lu target_commit_wait_us=%lu target_commit_wait_max_us=%lu"
				" active_BCDB_workers_current=%lu active_BCDB_workers_max=%lu"
				" overlapping_BCDB_optimistic_execution=%lu"
				" active_serial_waiters=%d oldest_active_wait_us=%lu oldest_active_txid=%ld oldest_active_block_id=%ld oldest_active_phase=%d",
				reason ? reason : "?",
				(int)block_id, (int)first_txid, (int)last_txid,
				(int)published, (int)last_committed, (int)next_enqueue,
				(unsigned long)agg_serial_gate_calls,
				(unsigned long)agg_serial_gate_wait_total_us,
				(unsigned long)agg_serial_gate_wait_max_us,
				(unsigned long)agg_serial_gate_cv_sleep_count,
				(unsigned long)agg_serial_gate_spin_iterations,
				(unsigned long)agg_commit_advance_calls,
				(unsigned long)agg_commit_initial_cas_failures,
				(unsigned long)agg_commit_prefix_steps,
				(unsigned long)agg_commit_broadcast_count,
				(unsigned long)agg_published_ready_calls,
				(unsigned long)agg_published_ready_prefix_steps,
				(unsigned long)agg_published_ready_cas_failures,
				(unsigned long)agg_block_enqueue_turn_calls,
				(unsigned long)agg_block_enqueue_turn_wait_total_us,
				(unsigned long)agg_block_enqueue_turn_max_us,
				(unsigned long)agg_block_watermark_wait_calls,
				(unsigned long)agg_block_watermark_wait_total_us,
				(unsigned long)agg_block_watermark_wait_max_us,
				(unsigned long)agg_block_slot_wait_calls,
				(unsigned long)agg_block_slot_wait_total_us,
				(unsigned long)agg_block_slot_wait_max_us,
				(unsigned long)agg_result_slot_consumable_wait_calls,
				(unsigned long)agg_result_slot_consumable_wait_total_us,
				(unsigned long)agg_result_slot_consumable_wait_max_us,
				(unsigned long)agg_slot_fallback_wait_calls,
				(unsigned long)agg_slot_fallback_wait_total_us,
				(unsigned long)agg_slot_fallback_wait_max_us,
				(unsigned long)agg_prev_commit_wait_calls,
				(unsigned long)agg_prev_commit_wait_total_us,
				(unsigned long)agg_prev_commit_wait_max_us,
				(unsigned long)agg_target_commit_wait_calls,
				(unsigned long)agg_target_commit_wait_total_us,
				(unsigned long)agg_target_commit_wait_max_us,
				(unsigned long) (block_meta
					? __atomic_load_n(&block_meta->active_bcdb_workers,
									   __ATOMIC_RELAXED)
					: 0),
				(unsigned long) (block_meta
					? __atomic_load_n(&block_meta->active_bcdb_workers_max,
									   __ATOMIC_RELAXED)
					: 0),
				(unsigned long) (block_meta
					? __atomic_load_n(&block_meta->overlapping_bcdb_optimistic_execution,
									   __ATOMIC_RELAXED)
					: 0),
				active_serial_waiters,
				(unsigned long)oldest_active_wait_us,
				(long)oldest_active_txid,
				(long)oldest_active_block_id,
				oldest_active_phase)));
}

/*
 * Shared-memory block pool and associated metadata.
 *
 * block_pool      - Hash table (keyed by BCBlockID) living in shared memory.
 *                   Each entry is a BCBlock that groups a set of transactions
 *                   that are committed together in the same blockchain block.
 *
 * block_pool_lock - Single spinlock that serialises all structural mutations
 *                   of the hash table (HASH_ENTER / HASH_REMOVE) as well as
 *                   the per-block num_tx counter and the global
 *                   last_committed_tx_id / num_committed counters.
 *
 * block_meta      - Small singleton struct in shared memory holding global
 *                   bookkeeping: the current [global_bmin, global_bmax]
 *                   window, commit/abort counts, condition variables for
 *                   bmin advancement, and an optional debug log.
 */
HTAB     	 *block_pool;
slock_t  	 *block_pool_lock;
BlockMeta	 *block_meta;
/*
 * Per-process cache of the sentinel BCBlock (id=1). Avoids a hash_search()
 * + spinlock acquisition on every hot-path accessor (set/get_blksz,
 * set/get_num_tx_sub, last/published watermarks). Populated lazily and
 * cleared explicitly by bcdb_reset_block_pool_state.
 */
static BCBlock *block1_cache = NULL;

/*
 * bcdb_reset_block_entry
 *
 * Zero-initialises a freshly-allocated BCBlock so that all counters, ring
 * buffers and condition variables are in a well-defined starting state.
 * Used both for newly-inserted hash entries (get_block_by_id) and to recycle
 * the sentinel entry (bcdb_reset_block_pool_state).
 *
 * Note: blksize defaults to bcdb_worker_count *only* for the sentinel block
 * (id=1), which is the canonical place worker code reads the per-block
 * transaction count from. All other blocks start with blksize=0 and have it
 * set explicitly by the ordering layer when the block is finalised.
 */
static void
bcdb_reset_block_entry(BCBlock *block, BCBlockID id)
{
    block->id = id;
    block->num_tx = 0;
    block->num_ready = 0;
    block->num_finished = 0;
    block->last_committed_tx_id = -1;
    block->published_max_tx_id = -1;
    ConditionVariableInit(&block->cond);
    ConditionVariableInit(&block->condRecovery);
    ConditionVariableInit(&block->condCommit);
    block->num_tx_sub = 0;
    block->num_tx_qd = 0;
    block->blksize = (id == 1) ? bcdb_worker_count : 0;
    block->snapTid = 0;

	/* Clear the per-transaction slot array and its per-slot done CVs. */
    for (int i = 0; i < MAX_TX_PER_BLOCK; i++)
    {
        block->txs[i] = NULL;
        ConditionVariableInit(&block->done_conds[i]);
    }

	/*
	 * Result ring buffer: result[] holds the produced tuples, the parallel
	 * *_txid arrays track which txid owns each slot. Sentinel value -1 means
	 * "slot empty"; InvalidTransactionId means "no PG xid bound yet".
	 */
    for (int i = 0; i < BCDB_RESULT_RING_CAPACITY; i++)
    {
        memset(&block->result[i], 0, sizeof(block->result[i]));
        block->result_committed_txid[i] = -1;
        block->result_commit_xid[i] = InvalidTransactionId;
        block->result_consumed_txid[i] = -1;
    }

	/* Lever D publish-phase ready-bitset; -1 = txid not yet published. */
    for (int i = 0; i < MAX_TX_PER_BLOCK; i++)
        block->published_ready_txid[i] = -1;
}

/*
 * get_block1_cached
 *
 * Hot-path accessor for the sentinel BCBlock (id=1). Returns the
 * per-process cached pointer if available; otherwise consults the
 * shared hash table once and memoises the result.
 *
 * The cache is process-local, so it is safe to populate without locking:
 * any racing writer is in another process and would re-resolve from the
 * shared hash table on its own first call.
 *
 * `create` is forwarded to get_block_by_id: pass true when this is the
 * first call in a process that must guarantee the entry exists; pass
 * false for read-only fast paths.
 */
static inline BCBlock *
get_block1_cached(bool create)
{
    BCBlock *blk = block1_cache;
    if (blk != NULL)
        return blk;
    blk = get_block_by_id(1, create);
    if (blk != NULL)
        block1_cache = blk;
    return blk;
}

/*
 * block_pool_size
 *
 * Returns the total amount of shared memory required for the block
 * subsystem.  Called during the shmem sizing pass (before the postmaster
 * forks any workers) so that ShmemInitStruct / ShmemInitHash can be
 * satisfied without needing to enlarge the segment afterwards.
 *
 * The three components are:
 *   - One BlockMeta singleton.
 *   - One spinlock (slock_t) guarding the hash table and counters.
 *   - The hash table itself, sized for MAX_NUM_BLOCKS entries.
 */
Size
block_pool_size()
{
    Size ret = sizeof(BlockMeta);
    ret = add_size(ret, sizeof(slock_t));
    ret = add_size(ret, hash_estimate_size(MAX_NUM_BLOCKS, sizeof(BCBlock)));
	/* Add sizing for sharded stats: (bcdb_worker_count + MaxBackends) shards */
	ret = add_size(ret, (bcdb_worker_count + MaxBackends) * sizeof(BCDBGatesStatsShard));
    return ret;
}

/*
 * create_block_pool
 *
 * Initialises all shared-memory structures for the block subsystem.
 * Must be called exactly once, from the postmaster during shared-memory
 * initialisation (PG_INIT / _PG_init path), before any worker or backend
 * touches these structures.
 *
 * Initialisation order matters:
 *   1. BlockMeta singleton   -- global bmin/bmax window & statistics.
 *   2. Condition variables   -- one per bmin bucket (NUM_BMIN_COND slots);
 *                               workers wait here for global_bmin to advance.
 *   3. block_pool_lock       -- guards hash-table mutations and counters.
 *   4. block_pool hash table -- fixed-size, keyed by BCBlockID (uint32).
 *
 * NOTE: set_blksz(1) is intentionally left commented out here.  Calling
 * it at this point caused an immediate SIGSEGV because the hash table
 * entry for block 1 does not yet exist (ShmemInitHash is not done).
 */
void
create_block_pool(void)
{
	/* HASHCTL must outlive the ShmemInitHash call -- declared on stack here. */
    HASHCTL info;
    bool    found;

	/* (1) Singleton BlockMeta: global watermarks, commit/abort counters. */
	block_meta = ShmemInitStruct("BCDB_BLOCK_META", sizeof(BlockMeta), &found);
    block_meta->global_bmin = 1;
    block_meta->global_bmax = 0;
    block_meta->debug_seq = 0;
    block_meta->num_committed = 0;
    block_meta->num_aborted = 0;
	block_meta->previous_report_commit = 0;
	block_meta->previous_report_ts = 0;
	block_meta->next_enqueue_block_id = BCDB_FIRST_SUBMIT_BLOCK_ID;
	block_meta->active_bcdb_workers = 0;
	block_meta->active_bcdb_workers_max = 0;
	block_meta->overlapping_bcdb_optimistic_execution = 0;
#ifdef LOG_STATUS
    block_meta->log[0] = '\0';
    block_meta->log_counter = 0;
#endif

	/*
	 * (2) Bucketed CV array used to wake backends waiting for global_bmin
	 * to advance. Sharding by bucket reduces wakeup storms when many
	 * backends wait on different bmin values concurrently.
	 */
    for (int i = 0; i < NUM_BMIN_COND; i++)
        ConditionVariableInit(&block_meta->conds[i]);

	/* (3) Spinlock guarding structural mutations of the block_pool hash. */
    block_pool_lock = ShmemInitStruct("block_pool_lock", sizeof(slock_t), &found);
    if (!found)
        SpinLockInit(block_pool_lock);

	/* (4) Fixed-size shared hash table, keyed by BCBlockID (uint32). */
    MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(BCBlockID);
	info.entrysize = sizeof(BCBlock);
	info.hash = uint32_hash;
    block_pool = ShmemInitHash("bcdb_block_pool",
                   MAX_NUM_BLOCKS,
                   MAX_NUM_BLOCKS,
                   &info, HASH_ELEM | HASH_FUNCTION | HASH_FIXED_SIZE);

	/* (5) Shared-memory gate stats shards */
	bcdb_gate_stats_shards = ShmemInitStruct("BCDB_GATE_STATS_SHARDS",
											 (bcdb_worker_count + MaxBackends) * sizeof(BCDBGatesStatsShard),
											 &found);
	if (!found)
	{
		memset(bcdb_gate_stats_shards, 0, (bcdb_worker_count + MaxBackends) * sizeof(BCDBGatesStatsShard));
	}
}

/*
 * bcdb_reset_block_pool_state
 *
 * Re-initialises the sentinel BCBlock (id=1), which holds the global
 * counters and watermarks consulted by every worker. Called between runs
 * (e.g. when restarting a deterministic batch) so stale state from a
 * previous run does not leak into the next.
 *
 * The per-process cache pointer is cleared first, then repopulated under
 * the spinlock after the entry has been re-initialised, so any concurrent
 * reader either still sees the old cached pointer (whose fields are about
 * to be reset under the lock anyway) or the freshly-reset block.
 */
void
bcdb_reset_gate_stats(void)
{
	if (bcdb_gate_stats_shards != NULL)
		memset(bcdb_gate_stats_shards, 0,
			   (bcdb_worker_count + MaxBackends) *
			   sizeof(BCDBGatesStatsShard));
}

void
bcdb_reset_block_pool_state(void)
{
    BCBlockID id = 1;
    BCBlock  *block;
    bool      found;

	if (bcdb_gate_telemetry_enabled)
		bcdb_log_gate_snapshot("before_reset", -1, -1, -1);

	bcdb_reset_gate_stats();
	block_meta->active_bcdb_workers = 0;
	block_meta->active_bcdb_workers_max = 0;
	block_meta->overlapping_bcdb_optimistic_execution = 0;

    block1_cache = NULL;
    SpinLockAcquire(block_pool_lock);
	/* HASH_ENTER acts as upsert: returns existing entry if present. */
    block = hash_search(block_pool, &id, HASH_ENTER, &found);
    bcdb_reset_block_entry(block, id);
    SpinLockRelease(block_pool_lock);
    block1_cache = block;

}

/*
 * set_last_committed_txid
 *
 * Publishes tx->tx_id as the most-recently-committed transaction id on
 * both the sentinel BCBlock (id=1) and the global block_meta counter.
 *
 * The deterministic workers consult this watermark on every commit-gate
 * check, so we avoid block_pool_lock contention entirely and rely on
 * release-store / acquire-load atomics for visibility. In CONDVAR gate
 * mode we additionally broadcast condCommit so any waiter that parked
 * on the gate is woken without a spin.
 *
 * NOTE: The older non-atomic variant set_last_committed_id() has been
 * removed; its only call site in worker.c was already commented out.
 */
void set_last_committed_txid( BCDBShmXact *tx)
{
    //BCBlock* blk = get_block_by_id( tx->block_id_committed, false);
    BCBlock* blk = get_block1_cached(true);

	/* Release-store: any reader that acquires this value sees all prior
	 * writes performed by the committing tx (its result tuple, etc.). */
    __atomic_store_n(&blk->last_committed_tx_id, tx->tx_id, __ATOMIC_RELEASE);
    __atomic_store_n(&block_meta->num_committed, tx->tx_id, __ATOMIC_RELEASE);

	/* CV gate mode parks waiters; spin/yield modes re-read the watermark. */
    if (bcdb_serial_gate_mode == BCDB_SERIAL_GATE_MODE_CONDVAR)
        ConditionVariableBroadcast(&blk->condCommit);
#if SAFEDBG2
    printf("safeDbg %s : %s: %d  blk %x txid= %d\n",
              __FILE__, __FUNCTION__, __LINE__, blk, block_meta->num_committed);
#endif
}

/*
 * bcdb_get_block1
 *
 * Public, read-only accessor for the sentinel BCBlock (id=1). Returns NULL
 * if it has not yet been created in this process's cache and the entry does
 * not exist in shared memory. Used by external callers that just need to
 * peek at watermarks; use get_block1_cached(true) internally when creation
 * is required.
 */
BCBlock *
bcdb_get_block1(void)
{
    return get_block1_cached(false);
}

/*
 * advance_last_committed_txid
 *
 * T3: non-blocking replacement for the old
 * bcdb_wait_for_prev_committed + set_last_committed_txid pair.
 *
 * Each tx tries a single CAS: advance last_committed_tx_id from
 * (my_tx_id - 1) to my_tx_id.  If the predecessor has not committed yet,
 * the CAS fails and the function returns immediately — the predecessor will
 * carry the scan through this tx's slot when it finishes.
 *
 * After a successful CAS, the function eagerly scans forward through any
 * consecutive successor slots that have already set result_committed_txid,
 * so the fastest-finishing thread amortises the watermark advance for all.
 *
 * The scan is bounded by the ring-buffer slot count to guarantee O(slots)
 * worst-case work per commit and to avoid livelock if successors keep
 * arriving while we are still advancing.
 */
void
advance_last_committed_txid(BCDBShmXact *tx)
{
	const bool collect_gate_stats = unlikely(bcdb_gate_telemetry_enabled);
	BCBlock *blk   = get_block1_cached(true);
	int      slots = bcdb_get_runtime_result_ring_slots();
	BCTxID   my_id = tx->tx_id;
	BCTxID   prev  = my_id - 1;
	BCTxID   cur;
	bool     watermark_advanced = false;

    if (slots < 1)
        slots = 1;

	if (collect_gate_stats)
		SHARD_INC(commit_advance_calls);

	/*
	 * Step 1: Try to claim "I am the next to commit". CAS succeeds iff our
	 * immediate predecessor has already published; otherwise bail and let
	 * the predecessor's eventual scan pick up our slot.
	 */
	if (!__sync_bool_compare_and_swap(&blk->last_committed_tx_id, prev, my_id))
	{
		/* Telemetry: CAS lost — predecessor not yet committed. */
		if (collect_gate_stats)
			SHARD_INC(commit_initial_cas_failures);
		return;
	}

	watermark_advanced = true;

	/* Step 2: Mirror the watermark on block_meta. Broadcast deferred to end. */
	__atomic_store_n(&block_meta->num_committed, my_id, __ATOMIC_RELEASE);

	/*
	 * Step 3: Opportunistically carry the watermark forward through any
	 * already-published successor slots. Each iteration:
	 *   - locate the ring slot owned by the next contiguous txid;
	 *   - load it with ACQUIRE so a successful match also synchronises
	 *     with the successor's RELEASE store of its result tuple;
	 *   - CAS our watermark forward (paranoia: another scanner may race).
	 *
	 * CORRECTNESS FIX: the broadcast is intentionally deferred until AFTER
	 * the full prefix scan (Step 4).  Broadcasting only after the initial CAS
	 * created a race: a waiter could wake, observe its target not yet reached,
	 * go back to sleep, and then miss the final watermark advance that happens
	 * within this very loop.  A single broadcast at the end of the scan covers
	 * all advances atomically from the waiter's perspective.
	 */
	cur = my_id;
	int advanced_steps = 0;
	for (int step = 0; step < slots; step++)
	{
		BCTxID next_id   = cur + 1;
		int    next_slot = (int)(next_id % (BCTxID)slots);
		BCTxID published;

		if (next_slot < 0)
			next_slot += slots;

		published = __atomic_load_n(&blk->result_committed_txid[next_slot],
									__ATOMIC_ACQUIRE);
		if (published != next_id)
			break;              	/* gap: successor not yet ready */
		if (!__sync_bool_compare_and_swap(&blk->last_committed_tx_id, cur, next_id))
			break;              	/* another scanner advanced past us */

		__atomic_store_n(&block_meta->num_committed, next_id, __ATOMIC_RELEASE);
		if (collect_gate_stats)
			advanced_steps++;
		cur = next_id;
	}

	if (collect_gate_stats && advanced_steps > 0)
	{
		for (int i = 0; i < advanced_steps; i++)
			SHARD_INC(commit_prefix_steps);
	}

	/*
	 * Step 4: Single broadcast AFTER the full contiguous prefix has been
	 * advanced.  Any waiter in bcdb_wait_until_committed() or
	 * bcdb_wait_for_prev_committed() that parked on condCommit will now
	 * re-check the watermark and observe the final value.
	 */
	if (watermark_advanced &&
		bcdb_serial_gate_mode == BCDB_SERIAL_GATE_MODE_CONDVAR)
	{
		if (collect_gate_stats)
			SHARD_INC(commit_broadcast_count);
		ConditionVariableBroadcast(&blk->condCommit);
	}
}

/*
 * set_blksz / get_blksz
 *
 * Accessors for the "block size" (number of transactions per blockchain
 * block) stored on the sentinel BCBlock (id=1).  Used by the worker to
 * decide when a block is full and ready for ordering/commit.
 *
 * NOTE: These access the sentinel block without holding block_pool_lock.
 * They are currently called only from single-threaded paths (worker init
 * and configuration reload) — if that changes a lock should be added.
 */
void set_blksz(int num)
{
    BCBlock* blk = get_block1_cached(true);
#if SAFEDBG2
    printf("ariaMyDbg %s : %s: %d bid 1, blk %x\n",
              __FILE__, __FUNCTION__, __LINE__, blk);
#endif
    blk->blksize = num;
}

BCTxID get_blksz()
{
    BCBlock* blk = get_block1_cached(false);
    if (blk == NULL)
        blk = get_block1_cached(true);
    if (blk == NULL)
        return 0;
    return blk->blksize;
}

/*
 * bcdb_get_result_ring_slots / bcdb_get_runtime_result_ring_slots
 *
 * Accessors for the number of slots in the block result ring buffer,
 * which is currently fixed at compile time (BCDB_RESULT_RING_CAPACITY)
 * but may be made configurable in the future.
 *
 * The runtime accessor enforces a minimum size of 2x the worker count
 * to ensure that all workers can publish without blocking on a predecessor
 * (see advance_last_committed_txid).  It is called by the worker during
 * init and must be safe to call before the sentinel block entry is created,
 * so it falls back to the compile-time default if block 1 is not yet available.
 */
int
bcdb_get_result_ring_slots(void)
{
    int slots = bcdb_result_ring_slots;

    if (slots < 2)
        slots = 2;
    if (slots > BCDB_RESULT_RING_CAPACITY)
        slots = BCDB_RESULT_RING_CAPACITY;
    return slots;
}

int
bcdb_get_runtime_result_ring_slots(void)
{
    int workers = get_blksz();
    int min_slots;
    int slots = bcdb_get_result_ring_slots();

    if (workers <= 0)
        workers = bcdb_worker_count;
    if (workers <= 0)
        workers = 1;

    min_slots = 2 * workers;
    if (slots < min_slots)
        slots = min_slots;
    if (slots < 2)
        slots = 2;
    if (slots > BCDB_RESULT_RING_CAPACITY)
        slots = BCDB_RESULT_RING_CAPACITY;
    return slots;
}

/*
 * set_num_tx_sub / get_num_tx_sub
 *
 * Accessors for num_tx_sub: the count of transactions that have been
 * "submitted" (handed off to the ordering layer) for the current block
 * on the sentinel BCBlock (id=1).
 */
void set_num_tx_sub(int num)
{
    BCBlock* blk = get_block1_cached(false);
    if (blk == NULL)
        blk = get_block1_cached(true);
    if (blk == NULL)
        return;
    blk->num_tx_sub = num;
}

BCTxID get_num_tx_sub()
{
    BCBlock* blk = get_block1_cached(false);
    if (blk == NULL)
        blk = get_block1_cached(true);
    if (blk == NULL)
        return 0;
    return blk->num_tx_sub;
}

/*
 * set_num_txqd / get_num_txqd
 *
 * Accessors for num_tx_qd: the count of transactions currently queued
 * (waiting to be submitted) for the current sentinel block.  Used by
 * the worker to track back-pressure in the pipeline.
 */
void set_num_txqd(int num)
{
    BCBlock* blk = get_block1_cached(false);
    if (blk == NULL)
        blk = get_block1_cached(true);
    if (blk == NULL)
        return;
    blk->num_tx_qd = num;
}

BCTxID get_num_txqd()
{
    BCBlock* blk = get_block1_cached(false);
    if (blk == NULL)
        blk = get_block1_cached(true);
    if (blk == NULL)
        return 0;
    return blk->num_tx_qd;
}

/*
 * get_last_committed_txid
 *
 * Returns the most-recently-committed transaction ID from the sentinel
 * BCBlock (id=1). Mirrors set_last_committed_txid: uses an acquire-load
 * atomic so the reader synchronises with the writer's release-store and
 * also observes any data the committing tx published (e.g. its result
 * tuple in the ring buffer) before bumping the watermark.
 *
 * The `tx` parameter is currently unused (cast to void) and retained for
 * call-site symmetry with the setter; remove it if/when callers are
 * updated.
 */
BCTxID get_last_committed_txid(BCDBShmXact *tx)
{
    BCBlock* blk = get_block1_cached(false);
    if (blk == NULL)
        blk = get_block1_cached(true);
    if (blk == NULL)
        return -1;
    (void) tx;
    return (BCTxID) __atomic_load_n(&blk->last_committed_tx_id, __ATOMIC_ACQUIRE);
}

/*
 * Lever D publish-phase gate accessors.
 *
 * published_max_tx_id is advanced atomically immediately after
 * publish_ws_tableDT in worker.c (Lever D v2). The serial gate waits on
 * published_max instead of last_committed so apply/finish can run in
 * parallel across backends.
 *
 * In CONDVAR mode, wake only the immediate successor's per-slot condition
 * variable.  The successor still rechecks published_max_tx_id before entering
 * conflict_checkDT(), so this changes wakeup latency, not deterministic order.
 */

/*
 * bcdb_published_ready_slot_for_txid
 *
 * Map a transaction id to its slot in the published_ready_txid[] bitset.
 * MAX_TX_PER_BLOCK is the modulus so each block's id space wraps cleanly.
 * The post-modulo +MAX_TX_PER_BLOCK is defensive against negative results
 * if BCTxID is ever made signed.
 */
static inline int
bcdb_published_ready_slot_for_txid(BCTxID tx_id)
{
    int idx = tx_id % MAX_TX_PER_BLOCK;

    if (idx < 0)
        idx += MAX_TX_PER_BLOCK;
    return idx;
}

/*
 * bcdb_signal_serial_successor
 *
 * After advancing published_max_tx_id to `published_txid`, wake exactly
 * one waiter: the backend that owns the per-slot CV for txid+1 (the
 * immediate successor in deterministic order). Cheaper than broadcasting
 * because only one tx is now eligible to make forward progress.
 *
 * No-op outside CONDVAR gate mode -- spin/yield modes re-read the
 * watermark themselves.
 */
static inline void
bcdb_signal_serial_successor(BCBlock *blk, BCTxID published_txid)
{
    if (bcdb_serial_gate_mode == BCDB_SERIAL_GATE_MODE_CONDVAR)
    {
        int wake_slot = (int) ((published_txid + 1) % MAX_TX_PER_BLOCK);

        if (wake_slot < 0)
            wake_slot += MAX_TX_PER_BLOCK;
        ConditionVariableSignal(&blk->done_conds[wake_slot]);
    }
}

/*
 * bcdb_advance_published_ready_prefix
 *
 * Carry published_max_tx_id forward over every contiguous successor that
 * has already marked itself ready in published_ready_txid[]. Analogous to
 * the watermark scan in advance_last_committed_txid: the fastest publisher
 * amortises the advance for any stalled predecessors that have already
 * deposited their slot.
 *
 * Bounded by MAX_TX_PER_BLOCK to guarantee termination if a peer keeps
 * publishing further successors concurrently.
 */
static void
bcdb_advance_published_ready_prefix(BCBlock *blk)
{
	const bool collect_gate_stats = unlikely(bcdb_gate_telemetry_enabled);
	BCTxID current;
	int steps = 0;
	int prefix_steps = 0;
	int cas_failures = 0;

	Assert(blk != NULL);

	while (steps++ < MAX_TX_PER_BLOCK)
	{
		BCTxID next_id;
		int next_slot;
		BCTxID ready;

		/* Re-read watermark each iter: another scanner may have moved it. */
		current = (BCTxID) __atomic_load_n(&blk->published_max_tx_id,
										   __ATOMIC_ACQUIRE);
		next_id = current + 1;
		next_slot = bcdb_published_ready_slot_for_txid(next_id);
		ready = (BCTxID) __atomic_load_n(&blk->published_ready_txid[next_slot],
										 __ATOMIC_ACQUIRE);
		if (ready != next_id)
			break;                      	/* gap: stop scanning */

		/*
		 * CAS to claim the advance. On success, wake the new successor
		 * (now next_id+1). On failure, another scanner won the race and
		 * is responsible for the wake — we just exit.
		 */
		if (__atomic_compare_exchange_n(&blk->published_max_tx_id,
										&current,
										next_id,
										false,
										__ATOMIC_RELEASE,
										__ATOMIC_ACQUIRE))
		{
			if (collect_gate_stats)
				prefix_steps++;
			bcdb_signal_serial_successor(blk, next_id);
		}
		else
		{
			if (collect_gate_stats)
				cas_failures++;
		}
	}

	if (collect_gate_stats)
	{
		for (int i = 0; i < prefix_steps; i++)
			SHARD_INC(published_ready_prefix_steps);
		for (int i = 0; i < cas_failures; i++)
			SHARD_INC(published_ready_cas_failures);
	}
}

/*
 * mark_published_ready_txid
 *
 * Worker entry-point invoked from publish_ws_tableDT in worker.c after a
 * tx has finished publishing its write-set. Marks the tx's slot as ready
 * with a release-store (so the readiness flag becomes visible only after
 * the write-set itself), then opportunistically advances the watermark.
 */
void
mark_published_ready_txid(BCDBShmXact *tx)
{
	const bool collect_gate_stats = unlikely(bcdb_gate_telemetry_enabled);
	BCBlock *blk = get_block1_cached(true);
	int slot;

	Assert(tx != NULL);
	Assert(blk != NULL);

	if (collect_gate_stats)
		SHARD_INC(published_ready_calls);

	slot = bcdb_published_ready_slot_for_txid(tx->tx_id);
	__atomic_store_n(&blk->published_ready_txid[slot],
					 tx->tx_id,
					 __ATOMIC_RELEASE);
	bcdb_advance_published_ready_prefix(blk);
}

/*
 * set_published_max_txid
 *
 * Back-compat shim: older worker call sites used this name before the
 * publish-readiness bitset was introduced. Forwards to
 * mark_published_ready_txid so legacy callers participate in the same
 * watermark-advance path as new ones.
 */
void
set_published_max_txid(BCDBShmXact *tx)
{
    mark_published_ready_txid(tx);
}

/*
 * get_published_max_txid
 *
 * Acquire-load the current Lever D publish watermark. Returns -1 if the
 * sentinel block cannot be created (out-of-memory / shutdown). The `tx`
 * parameter is reserved for future per-tx accounting; currently unused.
 */
BCTxID get_published_max_txid(BCDBShmXact *tx)
{
    BCBlock* blk = get_block1_cached(false);
    if (blk == NULL)
        blk = get_block1_cached(true);
    if (blk == NULL)
        return -1;
    (void) tx;
    return (BCTxID) __atomic_load_n(&blk->published_max_tx_id, __ATOMIC_ACQUIRE);
}

/*
 * get_block_by_id
 *
 * Looks up — and optionally creates — a BCBlock entry in the shared-memory
 * hash table.
 *
 * Parameters:
 *   id                   - The blockchain block ID to look up.
 *   create_if_not_found  - When true, a new entry is inserted if none exists
 *                         (HASH_ENTER); when false, returns NULL if not found
 *                         (HASH_FIND).
 *
 * Both paths hold block_pool_lock for the duration of the hash_search call
 * so that concurrent create/find operations across multiple backends are
 * serialised and the hash table is never observed in a partially-initialised
 * state.
 *
 * New entries are zero-initialised here: num_tx=0, all ConditionVariables
 * prepared, result buffers cleared.  last_committed_tx_id starts at -1 to
 * indicate "no transaction committed yet in this block".
 *
 * bcdb_worker_init() is called unconditionally to ensure the calling process
 * has attached to any per-process worker state required before touching
 * shared structures.
 */
BCBlock*
get_block_by_id(BCBlockID id, bool create_if_not_found)
{
    BCBlock *block;
    bool found;

    Assert(block_pool != NULL);
    bcdb_worker_init();
    if (create_if_not_found)
    {
        SpinLockAcquire(block_pool_lock);
        block = hash_search(block_pool, &id, HASH_ENTER, &found);
        if (!found)
        {
            printf("\n \t ** safeDbg pid= %d new blk %s : %s: %d bid %d blk %x\n",
                   getpid(), __FILE__, __FUNCTION__, __LINE__, id, block);
            bcdb_reset_block_entry(block, id);
        }
        SpinLockRelease(block_pool_lock);
    }
    else
    {
        SpinLockAcquire(block_pool_lock);
        block = hash_search(block_pool, &id, HASH_FIND, &found);
        SpinLockRelease(block_pool_lock);
    }
    return block;
}

/*
 * delete_block
 *
 * Removes the given BCBlock from the shared-memory hash table.
 * Safe to call with a NULL pointer (no-op).
 *
 * The caller must ensure that no other process holds a pointer to this
 * block and will dereference it after the removal — there is no reference
 * counting; the hash entry is freed immediately.
 */
void
delete_block(BCBlock *block)
{
    if (block == NULL)
        return;
    DEBUGNOCHECK("[ZL] deleting block %d", block->id);
    SpinLockAcquire(block_pool_lock);
    hash_search(block_pool, &block->id, HASH_REMOVE, NULL);
    SpinLockRelease(block_pool_lock);
}

/*
 * block_add_tx
 *
 * Appends a transaction pointer to the block's txs[] array and bumps
 * num_tx.  Holds block_pool_lock for the entire operation because:
 *
 *   1. num_tx++ is a non-atomic read-modify-write; two backends racing
 *      here would corrupt the counter and potentially write to the same
 *      slot, losing one transaction silently.
 *   2. The txs[] array write must be visible to any reader of num_tx
 *      before the lock is released (sequentially consistent ordering).
 *
 * Preconditions (asserted):
 *   - tx has not already been attached to a block
 *     (block_id_committed == BCDBInvalidBid or BCDBMaxBid).
 *   - The block is not already full (num_tx < MAX_TX_PER_BLOCK).
 */
void
block_add_tx(BCBlock* block, BCDBShmXact* tx)
{
    Assert(tx->block_id_committed == BCDBInvalidBid || tx->block_id_committed == BCDBMaxBid);
    SpinLockAcquire(block_pool_lock);
    Assert(block->num_tx < MAX_TX_PER_BLOCK);
    block->txs[block->num_tx++] = tx;
    SpinLockRelease(block_pool_lock);
}
