#include "bcdb/shm_block.h"
#include "postgres.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"
#include <sys/queue.h>

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
HTAB          *block_pool;
slock_t       *block_pool_lock;
BlockMeta     *block_meta;

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
    /* need to keep params in memory */
    HASHCTL info;
    bool    found;
	block_meta = ShmemInitStruct("BCDB_BLOCK_META", sizeof(BlockMeta), &found);
    block_meta->global_bmin = 1;
    block_meta->global_bmax = 0;
    block_meta->debug_seq = 0;
    block_meta->num_committed = 0;
    block_meta->num_aborted = 0;
    block_meta->previous_report_commit = 0;
    block_meta->previous_report_ts = 0;
#ifdef LOG_STATUS
    block_meta->log[0] = '\0';
    block_meta->log_counter = 0;
#endif
    for (int i = 0; i < NUM_BMIN_COND; i++)
        ConditionVariableInit(&block_meta->conds[i]);

    block_pool_lock = ShmemInitStruct("block_pool_lock", sizeof(slock_t), &found);
    if (!found)
        SpinLockInit(block_pool_lock);

    MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(BCBlockID);
	info.entrysize = sizeof(BCBlock);
	info.hash = uint32_hash;
    block_pool = ShmemInitHash("bcdb_block_pool",
                   MAX_NUM_BLOCKS,
                   MAX_NUM_BLOCKS,
                   &info, HASH_ELEM | HASH_FUNCTION | HASH_FIXED_SIZE);
}

/*
 * set_last_committed_txid
 *
 * Records the most-recently-committed transaction ID both on the sentinel
 * BCBlock (id=1) and in block_meta->num_committed.
 *
 * Uses block_pool_lock + pg_write_barrier() to ensure that concurrent
 * readers (get_last_committed_txid) in other processes never observe a
 * torn or stale value — important on weakly-ordered architectures.
 *
 * NOTE: The older non-atomic variant set_last_committed_id() has been
 * removed; its only call site in worker.c was already commented out.
 */
void set_last_committed_txid( BCDBShmXact *tx)
{
    //BCBlock* blk = get_block_by_id( tx->block_id_committed, false);
    BCBlock* blk = get_block_by_id(1, true);
    /* Atomic counter update with spinlock to prevent race conditions */
    SpinLockAcquire(block_pool_lock);
    blk->last_committed_tx_id = tx->tx_id;
    block_meta->num_committed = tx->tx_id;
    pg_write_barrier();  /* Ensure write ordering across processes */
    SpinLockRelease(block_pool_lock);
#if SAFEDBG2
    printf("safeDbg %s : %s: %d  blk %x txid= %d\n",
              __FILE__, __FUNCTION__, __LINE__, blk, block_meta->num_committed);
#endif
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
    BCBlock* blk = get_block_by_id(1, true);
#if SAFEDBG2
    printf("ariaMyDbg %s : %s: %d bid 1, blk %x\n",
              __FILE__, __FUNCTION__, __LINE__, blk);
#endif
    blk->blksize = num;
}

BCTxID get_blksz()
{
    BCBlock* blk = get_block_by_id(1, false);
    return blk->blksize;
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
    BCBlock* blk = get_block_by_id(1, false);
    blk->num_tx_sub = num;
}

BCTxID get_num_tx_sub()
{
    BCBlock* blk = get_block_by_id(1, false);
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
    BCBlock* blk = get_block_by_id(1, false);
    blk->num_tx_qd = num;
}

BCTxID get_num_txqd()
{
    BCBlock* blk = get_block_by_id(1, false);
    return blk->num_tx_qd;
}

/*
 * get_last_committed_txid
 *
 * Returns the most-recently-committed transaction ID from the sentinel
 * BCBlock (id=1).  Mirrors set_last_committed_txid: acquires
 * block_pool_lock and issues pg_read_barrier() to prevent the compiler
 * or CPU from reordering the load before the lock acquire, ensuring we
 * always see the latest value written by any process.
 */
BCTxID get_last_committed_txid(BCDBShmXact *tx)
{
    BCBlock* blk = get_block_by_id(1, false);
    BCTxID result;
    /* Atomic counter read with spinlock to prevent torn reads */
    SpinLockAcquire(block_pool_lock);
    pg_read_barrier();  /* Ensure read ordering across processes */
    result = blk->last_committed_tx_id;
    SpinLockRelease(block_pool_lock);
    return result;
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
            block->id = id;
            block->num_tx = 0;
            block->num_ready = 0;
            block->num_finished = 0;
            block->last_committed_tx_id = -1;
            ConditionVariableInit(&block->cond);
            ConditionVariableInit(&block->condRecovery);
            for (int i = 0; i < MAX_TX_PER_BLOCK; i++)
            {
                ConditionVariableInit(&block->done_conds[i]);
                memset(&block->result[i], 0, 1024);
            }
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
