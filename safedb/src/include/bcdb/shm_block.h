#ifndef BCDB_SHM_BLOCK_H
#define BCDB_SHM_BLOCK_H

#include "postgres.h"
#include "storage/lwlock.h"
#include "bcdb/shm_transaction.h"
#include "bcdb/bcdb_dsa.h"
#include "lib/dshash.h"
#include "sys/queue.h"
#include "bcdb/globals.h"
#include "storage/condition_variable.h"
#include "storage/spin.h"
#include "c.h"

#define BCDB_FIRST_SUBMIT_BLOCK_ID 2


/* unfortunately, the name Block conflicts with another component in postgres, so use ugly BCBlock for now */
typedef struct
{
    BCBlockID  id;
    int        num_tx;
    int volatile   last_committed_tx_id pg_attribute_aligned(PG_CACHE_LINE_SIZE);
    int volatile       num_ready;
    int volatile       num_finished;
    BCDBShmXact*       txs[MAX_TX_PER_BLOCK];
    ConditionVariable  cond;
    ConditionVariable  condRecovery;
    ConditionVariable  condCommit;
    int num_tx_sub;
    int num_tx_qd;
    int blksize;
    int snapTid;
    ConditionVariable  done_conds[MAX_TX_PER_BLOCK];
    char result[BCDB_RESULT_RING_CAPACITY][1024];
    /*
     * Lever D: publish-phase ordering counter.
     *
     * Gate now waits on (published_max_tx_id + 1 == my_tx_id) instead of
    * last_committed_tx_id. In Lever D v2 it is advanced immediately after
    * publish_ws_tableDT so apply/finish can overlap across worker backends.
     *
     * Placed AFTER result[] on purpose — result[] is the largest array in
     * the struct, so adding a field after it cannot shift the offset of
     * any earlier ConditionVariable's internal mutex byte. This is the
     * same CV-layout hazard that caused the shm_block.h wave 2.9 PANIC.
     */
    int volatile      published_max_tx_id;
    /*
     * T3 (2026-04-19): per-slot commit marker for decoupled middleware readback.
     *
	 * result_committed_txid[slot] is set to tx->tx_id by the worker after
	 * writing block->result[slot] and after finish_xact_command() has made
	 * the PostgreSQL transaction visible to later snapshots (release store).
     * Middleware and advance_last_committed_txid() check this slot-specific
     * value instead of the contiguous last_committed_tx_id watermark, so
     * each tx's result is readable as soon as that tx publishes — without
     * blocking on any predecessor.
     *
     * advance_last_committed_txid() uses a lock-free CAS prefix-scan to
     * advance last_committed_tx_id without bcdb_wait_for_prev_committed,
     * removing the O(N) serial bottleneck from the finish path.
     *
     * Initialized to -1.  Must remain LAST in BCBlock: inserting before
     * ConditionVariable fields causes CV-mutex byte displacement (wave-2.9
     * PANIC class of bug).
     */
    int32 volatile    result_committed_txid[BCDB_RESULT_RING_CAPACITY];
    /*
     * T3-v2 (2026-04-19): per-slot PostgreSQL commit XID for snapshot-based
     * conflict skip.
     *
     * Set to the committing backend's TransactionId (tx->xid) immediately
	 * AFTER finish_xact_command() and BEFORE result_committed_txid, so the
	 * release-acquire pair guarantees visibility: when a reader observes
	 * result_committed_txid[slot]==tx_id, result_commit_xid[slot] is already
	 * visible and the PostgreSQL commit has completed.
     *
     * conflict_checkDT (via table_checkDT) uses this to skip candidates that
     * committed before our GetTransactionSnapshot() xmin — those txs' writes
     * are already visible in our snapshot, so no write-set conflict is possible.
     *
     * Initialized to InvalidTransactionId (0). Must remain LAST in BCBlock.
     */
    TransactionId volatile result_commit_xid[BCDB_RESULT_RING_CAPACITY];
    /*
     * Per-slot consumed markers for result-ring overwrite protection.
     * result_consumed_txid[slot] is set to tx_id by middleware (and
     * self-marked by the worker in direct mode) after the result is read.
     * Workers spin on this before overwriting a slot whose previous occupant
     * has not yet been consumed.  Initialized to -1.
     * Must remain LAST in BCBlock (CV-layout hazard).
     */
    int32 volatile    result_consumed_txid[BCDB_RESULT_RING_CAPACITY];
    /*
     * Publish-gate readiness markers.
     *
     * A slot contains tx_id when that deterministic transaction no longer
     * needs to block later transactions at the write-set publish gate. Writers
     * set this after conflict_checkDT() + publish_ws_tableDT(); no-write
     * transactions may set it as soon as their write set is known empty.
     *
     * This array is keyed by tx_id % MAX_TX_PER_BLOCK rather than the result
     * ring size because the submit window can be larger than the result ring.
     */
    int32 volatile    published_ready_txid[MAX_TX_PER_BLOCK];
} BCBlock;

typedef struct
{
    /* bid < global_bmin: block committed */
    /* bid < global_bmin - CLEANING_DELAY_BLOCKS: block cleaned*/
    BCBlockID volatile global_bmin;
    BCBlockID volatile global_bmax;
    ConditionVariable  conds[NUM_BMIN_COND];
    ConditionVariable  token_cond;
    uint32 debug_seq;
    int32 volatile     num_committed;
    int32 volatile     num_aborted;
    uint64 volatile    previous_report_ts;
    int32 volatile    previous_report_commit; 
#ifdef LOG_STATUS
    char log[1024 * 1024 * 10];
    int  log_counter;
#endif
	/*
	 * Next block id allowed to enqueue worker-visible txs.
	 *
	 * Parallel PostgreSQL backends may parse block-submit SQL calls out of
	 * order.  Workers consume FIFO queues per partition, so allowing a higher
	 * block's txid to enter a worker queue before a lower block can make that
	 * worker wait forever on a predecessor queued behind it.  This gate keeps
	 * enqueue order monotonic while allowing later completion waits to overlap.
	 */
	BCBlockID volatile next_enqueue_block_id;
	uint64 volatile    active_bcdb_workers;
	uint64 volatile    active_bcdb_workers_max;
	uint64 volatile    overlapping_bcdb_optimistic_execution;
} BlockMeta;

/* todo: change it to HTAB */
extern HTAB          *block_pool;
extern slock_t       *block_pool_lock;
extern BlockMeta     *block_meta;

extern Size     block_pool_size(void);
extern void     create_block_pool(void);
extern void     bcdb_reset_block_pool_state(void);
extern void     bcdb_reset_gate_stats(void);
extern int      get_commited(int id);
extern void     set_commited(int id,  BCDBShmXact* tx);
extern BCBlock* get_block_by_id(BCBlockID id, bool create_if_not_found);
extern void     delete_block(BCBlock *block);
extern void     block_add_tx(BCBlock* block, BCDBShmXact* tx);

extern void     set_last_committed_txid(BCDBShmXact *tx);
extern BCTxID   get_last_committed_txid(BCDBShmXact *tx);
extern void     advance_last_committed_txid(BCDBShmXact *tx);
extern BCBlock *bcdb_get_block1(void);

extern void     set_published_max_txid(BCDBShmXact *tx);
extern void     mark_published_ready_txid(BCDBShmXact *tx);
extern BCTxID   get_published_max_txid(BCDBShmXact *tx);

extern void     set_blksz(int num);
extern BCTxID   get_blksz(void);

extern void     set_num_tx_sub(int num);
extern BCTxID   get_num_tx_sub(void);

extern void     set_num_txqd(int num);
extern BCTxID   get_num_txqd(void);
extern int      bcdb_get_result_ring_slots(void);
extern int      bcdb_get_runtime_result_ring_slots(void);
/* delete_block_by_id, print_block_status, and set_last_committed_id
 * have been removed — they had no callers in the codebase. */



#endif
