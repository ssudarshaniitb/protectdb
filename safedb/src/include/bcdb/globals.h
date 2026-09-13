#ifndef BCDB_GLOBAL_H
#define BCDB_GLOBAL_H

#include "postgres.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include <unistd.h>
#include <stdlib.h>
#include <sys/resource.h>

extern PGDLLIMPORT bool                is_bcdb_master; 
extern PGDLLIMPORT bool                is_bcdb_worker; 
extern PGDLLIMPORT int                 gdb_pause_sig; 
extern PGDLLIMPORT char                *bcdb_host;
extern PGDLLIMPORT char                *bcdb_port;
extern PGDLLIMPORT bool                OEP_mode;
extern PGDLLIMPORT int32               bcdb_worker_count;
extern PGDLLIMPORT bool                bcdb_dt_conflict_tracking;
extern PGDLLIMPORT bool                bcdb_dt_completion_only_skip_reads;
extern PGDLLIMPORT int32               bcdb_serial_gate_mode;
extern PGDLLIMPORT int32               bcdb_dt_hashtab_switch_threshold;
extern PGDLLIMPORT int32               bcdb_result_ring_slots;
extern PGDLLIMPORT bool                bcdb_advance_commit_watermark;
extern PGDLLIMPORT int32               bcdb_serial_gate_source;
extern PGDLLIMPORT char                *bcdb_client_public_key;
extern PGDLLIMPORT bool                bcdb_enforce_signatures;
extern PGDLLIMPORT bool                bcdb_gate_snapshot_each_block;

/*
 * bcdb_gate_telemetry_enabled
 *
 * When false (the default), all hot-path SHARD_INC / SHARD_ADD /
 * SHARD_UPDATE_MAX counter increments in the serial gate, slot-consumable
 * gate, watermark advance, and middleware wait functions are completely
 * skipped.  This eliminates the atomic-fetch-add overhead on every
 * transaction in the direct-path hot loop.
 *
 * Set to true (bcdb_gate_telemetry = on, or env BCDB_GATE_TELEMETRY=1)
 * only for diagnostic runs; never for production performance measurement.
 *
 * Default: false.
 */
extern PGDLLIMPORT bool                bcdb_gate_telemetry_enabled;


typedef enum BcdbIsolationLevel{
    BCDB_READ_COMMITED,
    BCDB_SERIALIZABLE
} BcdbIsolationLevel;

typedef int32 BCBlockID;
typedef int32 BCTxID;

extern BcdbIsolationLevel BcdbCurrentIsolationLevel;
extern MemoryContext bcdb_middleware_context;
extern MemoryContext bcdb_tx_context;
extern bool          skip_conflict_checking;
extern pid_t         pid;
extern int32         blocksize;
extern int32         worker_id;

//#define FIRST_WRITER_WINS
#define WAIT_GDB while(gdb_pause_sig == 0) {set_ps_display("waiting gdb", false); sleep(1);}
#define PGDBG 0
#ifndef SAFEDBG
#define SAFEDBG 0
#endif
#ifndef SAFEDBG1
#define SAFEDBG1 0
#endif
#ifndef SAFEDBG2
#define SAFEDBG2 0
#endif
#ifndef SAFEDBG3
#define SAFEDBG3 0
#endif
#define BCDBInvalidBid -1
#define BCDBMaxBid     0x7FFFFFFF
#define BCDBInvalidTid -1
#define CLEANING_DELAY_BLOCKS 5
#define QUEUEING_BLOCKS 256
#define MAX_NUM_BLOCKS (QUEUEING_BLOCKS + CLEANING_DELAY_BLOCKS + 100)
#define MAX_TX_PER_BLOCK 50
#define WORK_TOKENS 64
#define BCDB_DEFAULT_WORKER_COUNT 2
#define BCDB_DEFAULT_HASHTAB_SWITCH_THRESHOLD 1500
#define BCDB_DEFAULT_RESULT_RING_SLOTS 128
/*
 * BCDB_RESULT_RING_CAPACITY — compile-time capacity of the three per-slot
 * result arrays in BCBlock (result[], result_committed_txid[],
 * result_commit_xid[]).  Must be >= 2 * bcdb_worker_count max (1024) so
 * bcdb_get_runtime_result_ring_slots() never produces an out-of-bounds index.
 * 2048 = 2 × 1024 satisfies this for all valid GUC settings.
 */
#define BCDB_RESULT_RING_CAPACITY 2048
#define BCDB_SERIAL_GATE_MODE_POLL 0
#define BCDB_SERIAL_GATE_MODE_CONDVAR 1

/* bcdb_serial_gate_source values */
#define BCDB_GATE_SRC_PUBLISHED_MAX  0  /* Lever D v2: wait on published_max_tx_id (default) */
#define BCDB_GATE_SRC_LAST_COMMITTED 1  /* Paper-style: wait on last_committed_tx_id before conflict-check */
#define NUM_WORKERS BCDB_DEFAULT_WORKER_COUNT
#define HASHTAB_SWITCH_THRESHOLD BCDB_DEFAULT_HASHTAB_SWITCH_THRESHOLD
#define WORKER_INIT_NUM 64
#define NUM_BMIN_COND 1
#define MAX_SHM_TX ((CLEANING_DELAY_BLOCKS + QUEUEING_BLOCKS + 100) * MAX_TX_PER_BLOCK)
#define MAX_WRITE_CONFLICT (MAX_TX_PER_BLOCK * 128)
#define TX_HASH_SIZE 32
#define WRITE_CONFLICT_MAP_NUM_PARTITIONS 512
#define DT_CONFLICT_TRACKING 0
#define AVAILABLE_LIST_PARTITION_SIZE (MAX_WRITE_CONFLICT * MAX_TX_PER_BLOCK / WRITE_CONFLICT_MAP_NUM_PARTITIONS)
#define NUM_TX_QUEUE_PARTITION MAX_TX_PER_BLOCK
//#define DEBUGMSG(f_, ...) do { if (activeTx) ereport(DEBUG3, (errmsg((f_), ##__VA_ARGS__))); } while(0)
//#define DEBUGNOCHECK(f_, ...) ereport(DEBUG3, (errmsg((f_), ##__VA_ARGS__)))
#define DEBUGMSG(...) {}
#define DEBUGNOCHECK(...) {}
//#define LOG_STATUS
#define REPORT_INTERVAL 2

#define SetPriority(prio) \
do { \
    if (setpriority(PRIO_PROCESS, pid, prio) != 0) \
        ereport(FATAL, (errmsg("[ZL] cannot set priority"))); \
} while(0)

#define ReleaseToken() \
do {\
if (activeTx->holding_token) { \
	if (__sync_add_and_fetch(&block_meta->work_token, 1) > 0) \
        ConditionVariableSignal(&block_meta->token_cond); \
    activeTx->holding_token = false; \
}} while(0)

#define ForceGetToken() \
do { \
if (!activeTx->holding_token) { \
    __sync_sub_and_fetch(&block_meta->work_token, 1); \
    tx->holding_token = true; \
}} while(0)

#define WaitConditionPidDbg(v, pid, cond) \
do {\
if (!(cond)) { \
    ConditionVariablePrepareToSleep(v); \
    while(!(cond)) \
        { printf("safeDbg pid %d checking cond %d\n", pid, cond); \
        ConditionVariableSleep(v, WAIT_EVENT_BLOCK_COMMIT); } \
    ConditionVariableCancelSleep(); \
}} while(0)

#define WaitConditionPid(v, pid, cond) \
do {\
if (!(cond)) { \
    ConditionVariablePrepareToSleep(v); \
    while(!(cond)) \
        { ConditionVariableSleep(v, WAIT_EVENT_BLOCK_COMMIT); } \
    ConditionVariableCancelSleep(); \
}} while(0)

	//	printf("pid %d checking timed cond %d\n", pid, cond); 
#define WaitConditionPidTimeout(v, pid, timeout, cond) \
do {\
if (!(cond)) { \
    ConditionVariablePrepareToSleep(v); \
    while(!(cond)) \
        { \
        ConditionVariableTimedSleep(v, timeout, WAIT_EVENT_BLOCK_COMMIT); } \
    ConditionVariableCancelSleep(); \
}} while(0)

#define WaitConditionTimeoutPid(v, cond, timeout, pid) \
do {\
if (!(cond)) { \
    ConditionVariablePrepareToSleep(v); \
    while(!(cond)) \
        { printf("pid %d checking timed cond %d\n", pid, cond); \
        ConditionVariableTimedSleep(v, timeout, WAIT_EVENT_BLOCK_COMMIT); } \
    ConditionVariableCancelSleep(); \
}} while(0)

#define WaitCondition(v, cond) \
do {\
if (!(cond)) { \
    ConditionVariablePrepareToSleep(v); \
    while(!(cond)) \
        { printf("checking cond \n"); \
        ConditionVariableSleep(v, WAIT_EVENT_BLOCK_COMMIT); } \
    ConditionVariableCancelSleep(); \
}} while(0)

#define WaitConditionWithBackOff(v, cond) \
do {\
if (!(cond)) { \
    volatile uint64 backoff = 1000 + (rand() % 100) * 10; \
    for (; backoff > 0; backoff--); \
    if (!(cond)) { \
        ConditionVariablePrepareToSleep(v); \
        while(!(cond)) \
            ConditionVariableSleep(v, WAIT_EVENT_BLOCK_COMMIT); \
        ConditionVariableCancelSleep(); \
    } \
}} while(0)

#define WaitConditionAndReleaseToken(v, cond) \
do {\
if (!(cond)) { \
    ReleaseToken(); \
    ConditionVariablePrepareToSleep(v); \
    while(!(cond)) \
        ConditionVariableSleep(v, WAIT_EVENT_BLOCK_COMMIT); \
    ConditionVariableCancelSleep(); \
}} while(0)

#define WaitConditionAndReleaseTokenWithBackoff(v, cond) \
do {\
if (!(cond)) { \
    if (activeTx->holding_token) { \
        volatile uint64 backoff = 10000 + (rand() % 1000) * 10; \
        for (; backoff > 0; backoff--); \
    } \
    if (!(cond)) { \
        ReleaseToken(); \
        ConditionVariablePrepareToSleep(v); \
        while(!(cond)) \
            ConditionVariableSleep(v, WAIT_EVENT_BLOCK_COMMIT); \
        ConditionVariableCancelSleep(); \
    } \
}} while(0)

#define WaitGlobalBmin(bmin) \
do \
{ \
    WaitCondition(&block_meta->conds[(bmin) % NUM_BMIN_COND], block_meta->global_bmin == (bmin)); \
} while(0)

#define WaitGlobalBminGreaterOrEqual(bmin) \
do \
{ \
    WaitCondition(&block_meta->conds[(bmin) % NUM_BMIN_COND], block_meta->global_bmin >= (bmin)); \
} while(0)

#define WaitGlobalBminAndReleaseToken(bmin) \
do \
{ \
    WaitConditionAndReleaseToken(&block_meta->conds[(bmin) % NUM_BMIN_COND], block_meta->global_bmin == (bmin)); \
} while(0)


/*
 * BCDBGatesStatsShard — shared-memory sharded gate instrumentation.
 *
 * Each backend/worker process has its own shard to prevent cache-line contention.
 * The shard array is allocated in shared memory.
 */
typedef struct pg_attribute_aligned(PG_CACHE_LINE_SIZE) BCDBGatesStatsShard
{
	/* Serial deterministic gate (bcdb_wait_for_serial_slot) */
	uint64	serial_gate_calls;
	uint64	serial_gate_wait_total_us;
	uint64	serial_gate_wait_max_us;
	uint64	serial_gate_cv_sleep_count;
	uint64	serial_gate_spin_iterations;

	/* Published-ready watermark (mark_published_ready_txid / bcdb_advance_published_ready_prefix) */
	uint64	published_ready_calls;
	uint64	published_ready_prefix_steps;
	uint64	published_ready_cas_failures;

	/* Commit watermark advancement (advance_last_committed_txid) */
	uint64	commit_advance_calls;
	uint64	commit_initial_cas_failures;
	uint64	commit_prefix_steps;
	uint64	commit_broadcast_count;

	/* Block enqueue ordering gate (bcdb_wait_for_block_enqueue_turn) */
	uint64	block_enqueue_turn_calls;
	uint64	block_enqueue_turn_wait_total_us;
	uint64	block_enqueue_turn_wait_max_us;

	/* Fastpath watermark wait (bcdb_wait_until_committed) */
	uint64	block_watermark_wait_calls;
	uint64	block_watermark_wait_total_us;
	uint64	block_watermark_wait_max_us;

	/* Block-slot readiness wait (bcdb_wait_until_block_slots_ready) */
	uint64	block_slot_wait_calls;
	uint64	block_slot_wait_total_us;
	uint64	block_slot_wait_max_us;

	/* Result ring slot consumable wait (bcdb_wait_for_slot_consumable) */
	uint64	result_slot_consumable_wait_calls;
	uint64	result_slot_consumable_wait_total_us;
	uint64	result_slot_consumable_wait_max_us;

	/* Fallback slot wait (bcdb_wait_until_slot_ready) */
	uint64	slot_fallback_wait_calls;
	uint64	slot_fallback_wait_total_us;
	uint64	slot_fallback_wait_max_us;

	/* Predecessor commit wait (bcdb_wait_for_prev_committed) */
	uint64	prev_commit_wait_calls;
	uint64	prev_commit_wait_total_us;
	uint64	prev_commit_wait_max_us;

	/* Target commit wait (bcdb_wait_for_target_committed) */
	uint64	target_commit_wait_calls;
	uint64	target_commit_wait_total_us;
	uint64	target_commit_wait_max_us;

	/* Active wait state for hang detection */
	uint64	active_wait_start_us;
	int64	active_wait_txid;
	int32	active_wait_phase;
	int32	active_wait_block_id;
	int32	pid;

} BCDBGatesStatsShard;

/* Gate wait phases */
#define BCDB_GATE_PHASE_NONE         0
#define BCDB_GATE_PHASE_SERIAL       1
#define BCDB_GATE_PHASE_ENQUEUE      2
#define BCDB_GATE_PHASE_WATERMARK    3
#define BCDB_GATE_PHASE_SLOT         4
#define BCDB_GATE_PHASE_CONSUMABLE   5
#define BCDB_GATE_PHASE_SLOT_FALLBACK 6
#define BCDB_GATE_PHASE_PREV_COMMIT   7
#define BCDB_GATE_PHASE_TARGET_COMMIT 8

extern PGDLLIMPORT int MyProcPid;
extern PGDLLIMPORT int MyBackendId;
extern PGDLLIMPORT int MaxBackends;
extern BCDBGatesStatsShard *bcdb_gate_stats_shards;

extern uint64 bcdb_get_time(void);

static inline int
get_my_gate_stats_shard_index(void)
{
	int idx;
	if (is_bcdb_worker && worker_id >= 0 && worker_id < bcdb_worker_count)
		idx = worker_id;
	else if (MaxBackends > 0 && MyBackendId != 0 && MyBackendId >= 1 && MyBackendId <= MaxBackends)
		idx = bcdb_worker_count + (MyBackendId - 1);
	else if (MaxBackends > 0)
		idx = bcdb_worker_count + (MyProcPid % MaxBackends);
	else
		idx = bcdb_worker_count + (MyProcPid % 32);

	if (bcdb_gate_stats_shards != NULL)
	{
		if (__atomic_load_n(&bcdb_gate_stats_shards[idx].pid, __ATOMIC_RELAXED) != MyProcPid)
			__atomic_store_n(&bcdb_gate_stats_shards[idx].pid, MyProcPid, __ATOMIC_RELAXED);
	}
	return idx;
}

static inline void
gate_stats_begin_wait(int phase, BCTxID txid, BCBlockID block_id)
{
	if (bcdb_gate_stats_shards != NULL)
	{
		int shard_idx = get_my_gate_stats_shard_index();
		BCDBGatesStatsShard *shard = &bcdb_gate_stats_shards[shard_idx];
		uint64 now_us = bcdb_get_time();

		__atomic_store_n(&shard->active_wait_start_us, now_us, __ATOMIC_RELAXED);
		__atomic_store_n(&shard->active_wait_txid, txid, __ATOMIC_RELAXED);
		__atomic_store_n(&shard->active_wait_block_id, block_id, __ATOMIC_RELAXED);

		/* Publish a complete active-wait record last. */
		__atomic_store_n(&shard->active_wait_phase, phase, __ATOMIC_RELEASE);
	}
}

static inline void
gate_stats_finish_wait(void)
{
	if (bcdb_gate_stats_shards != NULL)
	{
		int shard_idx = get_my_gate_stats_shard_index();
		BCDBGatesStatsShard *shard = &bcdb_gate_stats_shards[shard_idx];

		/* Invalidate record first. */
		__atomic_store_n(&shard->active_wait_phase, BCDB_GATE_PHASE_NONE, __ATOMIC_RELEASE);

		/* Then clear the other fields for tidiness. */
		__atomic_store_n(&shard->active_wait_start_us, 0, __ATOMIC_RELAXED);
		__atomic_store_n(&shard->active_wait_txid, -1, __ATOMIC_RELAXED);
		__atomic_store_n(&shard->active_wait_block_id, -1, __ATOMIC_RELAXED);
	}
}

static inline void
shard_update_max(uint64 *max_field_ptr, uint64 elapsed)
{
	uint64 cur_max;
	do {
		cur_max = __atomic_load_n(max_field_ptr, __ATOMIC_RELAXED);
	} while (elapsed > cur_max &&
			 !__atomic_compare_exchange_n(max_field_ptr, &cur_max, elapsed, false,
										  __ATOMIC_RELAXED, __ATOMIC_RELAXED));
}

/*
 * SHARD_INC / SHARD_ADD / SHARD_UPDATE_MAX
 *
 * Unconditional per-shard atomic counter helpers.  These are always-on and
 * are appropriate for very low-frequency events (e.g., once per block, once
 * per startup).  Do NOT use these inside per-transaction hot paths — use the
 * BCDB_GATE_STAT_* variants below instead.
 */
#define SHARD_INC(field) \
	do { \
		if (bcdb_gate_stats_shards != NULL) { \
			int _idx = get_my_gate_stats_shard_index(); \
			__atomic_fetch_add(&bcdb_gate_stats_shards[_idx].field, 1, __ATOMIC_RELAXED); \
		} \
	} while (0)

#define SHARD_ADD(field, val) \
	do { \
		if (bcdb_gate_stats_shards != NULL) { \
			int _idx = get_my_gate_stats_shard_index(); \
			__atomic_fetch_add(&bcdb_gate_stats_shards[_idx].field, (val), __ATOMIC_RELAXED); \
		} \
	} while (0)

#define SHARD_UPDATE_MAX(field, val) \
	do { \
		if (bcdb_gate_stats_shards != NULL) { \
			int _idx = get_my_gate_stats_shard_index(); \
			shard_update_max(&bcdb_gate_stats_shards[_idx].field, (val)); \
		} \
	} while (0)

/*
 * BCDB_GATE_STAT_INC / BCDB_GATE_STAT_ADD / BCDB_GATE_STAT_MAX
 *
 * Telemetry-guarded variants of the SHARD_* macros.  The outer
 * `unlikely(bcdb_gate_telemetry_enabled)` branch predicts false in steady
 * state so the CPU speculates through it for free.  When telemetry is off
 * (the default production setting) the entire counter update — including
 * the shard-index lookup and atomic operation — is never executed.
 *
 * Use these for every per-transaction hot-path counter: serial gate,
 * consumable-slot gate, watermark advance, middleware waits, etc.
 */
#define BCDB_GATE_STAT_INC(field) \
	do { \
		if (unlikely(bcdb_gate_telemetry_enabled)) \
			SHARD_INC(field); \
	} while (0)

#define BCDB_GATE_STAT_ADD(field, val) \
	do { \
		if (unlikely(bcdb_gate_telemetry_enabled)) \
			SHARD_ADD(field, (val)); \
	} while (0)

#define BCDB_GATE_STAT_MAX(field, val) \
	do { \
		if (unlikely(bcdb_gate_telemetry_enabled)) \
			SHARD_UPDATE_MAX(field, (val)); \
	} while (0)

/* Emit a PROFILE_BCDB_GATE log line (all atomic reads, no lock). */
extern void bcdb_log_gate_snapshot(const char *reason,
				BCBlockID block_id,
				BCTxID first_txid,
				BCTxID last_txid);

#define CLOCKS_PER_MICRO_SECOND (CLOCKS_PER_SEC / 1000000l)
uint64 bcdb_get_time(void);
int Base64Encode(const unsigned char* buffer, size_t length, char** b64text);
#endif
