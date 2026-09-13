#include "postgres.h"
#include "bcdb/shm_transaction.h"
#include "bcdb/utils/aligned_heap.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "libpq-fe.h"
#include "storage/shmem.h"
#include "string.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/relcache.h"
#include "nodes/pg_list.h"
#include "nodes/nodes.h"
#include "catalog/index.h"
#include "utils/rel.h"
#include <access/tableam.h>
#include <executor/executor.h>
#include <executor/nodeModifyTable.h>
#include <bcdb/worker.h>
#include "bcdb/shm_block.h"
#include "bcdb/bcdb_dsa.h"
#include "utils/hsearch.h"
#include <stddef.h>
#include <access/genam.h>
#include "access/xact.h"
#include "access/subtrans.h"
#include "access/heapam.h"
#include "access/merkle.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_type_d.h"
#include "access/htup_details.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "utils/hashutils.h"
#include "access/itup.h"
#include "access/nbtree.h"
#include "utils/fmgroids.h"
#include "utils/snapmgr.h"
#include "utils/hsearch.h"
#include "bcdb/worker_controller.h"
#include "storage/spin.h"
#include "storage/predicate_internals.h"
#include <time.h>
#include <stdio.h>

// BCDBShmXact  *mapTxToShm[TX_MAP_SZ];
// #define MAP_TX(id) ((id) % TX_MAP_SZ)
/*
 * shm_transaction.c — shared-memory transaction pool and conflict detection.
 *
 * OVERVIEW
 * --------
 * Every transaction that enters the BCDB pipeline is represented by a
 * BCDBShmXact entry in the shared-memory hash table tx_pool (keyed by hash
 * string).  A parallel xid_map (keyed by PostgreSQL TransactionId) allows
 * looking up the same entry during SSI predicate-lock callbacks.
 *
 * CONFLICT DETECTION — DETERMINISTIC EXECUTION (DT) PATH
 * -----------------------------------------------------
 * The current active path is the "DT" (Deterministic Execution) design:
 *
 *  1. During the OPTIMISTIC phase each backend calls ws_table_reserveDT() for
 *     every tuple it writes and rs_table_reserveDT() for every tuple it reads.
 *     These functions only append a WSTableEntryRecord to the process-local
 *     linked lists (ws_table_record / rs_table_record) — they do NOT touch
 *     the shared hash tables yet.
 *
 *  2. After the worker decides the tx ordering slot, it calls conflict_checkDT()
 *     which walks the local ws/rs record lists and queries BOTH shards of the
 *     dual write-set table (ws_table->map and ws_table->mapB) using
 *     ws_table_checkDT().  A conflict is detected if any earlier-ordered
 *     transaction registered that tuple.
 *
 *  3. Once the tx passes conflict check, publish_ws_tableDT() atomically
 *     commits the local write-set records into whichever hash-table shard is
 *     currently "active" (the DT ping-pong ensures older entries are cleared
 *     without blocking concurrent readers).
 *
 * NON-DT PATH (legacy, accessed when OEP_mode=false or dual_tab=false)
 * -----------------------------------------------------------------------
 * The non-DT path calls conflict_check() which queries ws_table->map via
 * ws_table_check() / rs_table_check().  However ws_table_reserve() and
 * rs_table_reserve() (which wrote to ws_table->map) have been removed because
 * all their call sites were replaced by the DT variants.  Consequently
 * ws_table_get() is never called to populate the map during the serial phase,
 * and conflict_check() is a no-op for conflict detection in this path.
 *
 * TX QUEUE
 * --------
 * Each worker has an associated TxQueue partition.  Backends call
 * tx_queue_insert() to enqueue work; workers call tx_queue_next() to dequeue.
 * Both sides use a spinlock + condition variable to implement blocking.
 */

// slock_t      *nexec_lock;

/*
 * activeTx — per-process pointer to the BCDBShmXact currently being executed
 *             by this worker.  Set at the start of each transaction's serial
 *             phase and cleared on completion.  Never accessed concurrently
 *             by multiple processes (it is purely process-local state that
 *             points into shared memory).
 *
 * tx_pool / tx_pool_lock — fixed-size shared-memory hash table holding all
 *             in-flight BCDBShmXact entries, keyed by TX_HASH_SIZE hash string.
 *             Guarded by tx_pool_lock (spinlock).
 *
 * xid_map / xid_map_lock — secondary index from PostgreSQL TransactionId to
 *             BCDBShmXact*, populated by add_tx_xid_map() when a worker
 *             assigns an XID.  Needed for SSI predicate-lock callbacks that
 *             only know the XID.
 *
 * ws_table / rs_table — shared write-set and read-set conflict maps.  Each
 *             is a WSTable holding two partitioned hash tables (map + mapB)
 *             for the Deterministic Execution (DT) ping-pong scheme, plus
 *             per-partition spinlocks.
 *
 * ws_table_record / rs_table_record — process-local singly-linked lists
 *             (LIST_HEAD) of WSTableEntryRecord nodes allocated in
 *             bcdb_tx_context.  Built during the optimistic phase by the
 *             reserveDT functions; read by conflict_checkDT() and
 *             publish_ws_tableDT(); freed automatically when bcdb_tx_context
 *             is reset between transactions.
 */
BCDBShmXact *activeTx;
slock_t *restart_counter_lock;
pg_atomic_uint32 *bcdb_safe_failpoint_fired;
int *numExecPt;
HTAB *tx_pool;
slock_t *tx_pool_lock;
HTAB *xid_map;
slock_t *xid_map_lock;
TxQueue *tx_queues;
WSTable *ws_table;
WSTable *rs_table;
WSTableRecord ws_table_record;
WSTableRecord rs_table_record;
static bool bcdb_apply_unique_violation = false;
extern HTAB *PredicateLockTargetHash;
extern HTAB *PredicateLockHash;

static TupleTableSlot *clone_slot(TupleTableSlot *slot);

static bool
bcdb_xidmap_debug_enabled(void)
{
    static int cached = -1;

    if (cached < 0)
    {
        const char *v = getenv("BCDB_XIDMAP_DEBUG");
        cached = (v != NULL && v[0] != '\0' && strcmp(v, "0") != 0) ? 1 : 0;
    }

    return cached == 1;
}

void bcdb_reset_apply_error_flags(void)
{
    bcdb_apply_unique_violation = false;
}

bool bcdb_apply_had_unique_violation(void)
{
    return bcdb_apply_unique_violation;
}

void bcdb_set_apply_unique_violation(bool val)
{
	bcdb_apply_unique_violation = val;
}

/*
 * Broad execution-flow diagnostics. Off by default; enable with
 * BCDB_FLOW_DEBUG=1 for targeted stall investigations.
 */
static bool
bcdb_flow_debug_enabled(void)
{
    static int cached = -1;

    if (cached < 0)
    {
        const char *v = getenv("BCDB_FLOW_DEBUG");
        cached = (v && *v && *v != '0' && *v != 'n' && *v != 'N' && *v != 'f' && *v != 'F') ? 1 : 0;
    }
    return cached == 1;
}

static bool
bcdb_ws_rotation_profile_enabled(void)
{
	static int enabled = -1;

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

static uint64 bcdb_ws_rotation_count = 0;
static uint64 bcdb_ws_rotation_lock_acquire_us = 0;
static uint64 bcdb_ws_rotation_clear_us = 0;
static uint64 bcdb_ws_rotation_total_us = 0;
static uint64 bcdb_ws_rotation_max_us = 0;
static uint64 bcdb_ws_rotation_stalled_publish_count = 0;

static void
bcdb_ws_rotation_note(uint64 lock_us, uint64 clear_us, uint64 total_us)
{
	bcdb_ws_rotation_count++;
	bcdb_ws_rotation_lock_acquire_us += lock_us;
	bcdb_ws_rotation_clear_us += clear_us;
	bcdb_ws_rotation_total_us += total_us;
	if (total_us > bcdb_ws_rotation_max_us)
		bcdb_ws_rotation_max_us = total_us;
	if (total_us > 1000)
		bcdb_ws_rotation_stalled_publish_count++;
}

static BCTxID bcdb_last_conflict_txid = -1;

void bcdb_reset_last_conflict_txid(void)
{
    bcdb_last_conflict_txid = -1;
}

BCTxID
bcdb_get_last_conflict_txid(void)
{
    return bcdb_last_conflict_txid;
}

#define BCDB_FLOW_LOG(...)                       \
    do                                           \
    {                                            \
        if (bcdb_flow_debug_enabled())           \
            ereport(LOG, (errmsg(__VA_ARGS__))); \
    } while (0)

#define BCDB_XIDMAP_LOG(...)                     \
    do                                           \
    {                                            \
        if (bcdb_xidmap_debug_enabled())         \
            ereport(LOG, (errmsg(__VA_ARGS__))); \
    } while (0)

static bool
bcdb_step1_tm_probe_enabled(void)
{
    static int cached = -1;

    if (cached < 0)
    {
        const char *v = getenv("BCDB_STEP1_TM_PROBE");

        if (v == NULL || v[0] == '\0')
            cached = 1;
        else
            cached = (v[0] != '0' && v[0] != 'n' && v[0] != 'N' &&
                      v[0] != 'f' && v[0] != 'F')
                         ? 1
                         : 0;
    }

    return cached == 1;
}

static bool
bcdb_apply_wait_debug_enabled(void)
{
    static int cached = -1;

    if (cached < 0)
    {
        const char *v = getenv("BCDB_APPLY_WAIT_DEBUG");

        cached = (v != NULL && v[0] != '\0' &&
                  v[0] != '0' && v[0] != 'n' && v[0] != 'N' &&
                  v[0] != 'f' && v[0] != 'F')
                     ? 1
                     : 0;
    }

    return cached == 1;
}

static inline void
bcdb_step1_note_tm_being_modified(bool is_update)
{
    if (!bcdb_ptrace_enabled())
        return;

    if (is_update)
    {
        bcdb_ptrace_inc_counter(BCDB_PTRACE_COUNTER_APPLY_UPDATE_TM_BEING_MODIFIED_COUNT, 1);
        bcdb_ptrace_inc_counter(BCDB_PTRACE_COUNTER_APPLY_UPDATE_WAIT_INCIDENT_COUNT, 1);
    }
    else
    {
        bcdb_ptrace_inc_counter(BCDB_PTRACE_COUNTER_APPLY_DELETE_TM_BEING_MODIFIED_COUNT, 1);
        bcdb_ptrace_inc_counter(BCDB_PTRACE_COUNTER_APPLY_DELETE_WAIT_INCIDENT_COUNT, 1);
    }
}

static inline void
bcdb_step1_note_wait_us(bool is_update, uint64 wait_us)
{
    if (!bcdb_ptrace_enabled())
        return;

    if (is_update)
        bcdb_ptrace_add_us(BCDB_PTRACE_METRIC_APPLY_UPDATE_WAIT_US, wait_us);
    else
        bcdb_ptrace_add_us(BCDB_PTRACE_METRIC_APPLY_DELETE_WAIT_US, wait_us);
}

static TM_Result
bcdb_table_tuple_update_step1(Relation relation,
                              ItemPointer tid,
                              TupleTableSlot *slot,
                              CommandId cid,
                              TM_FailureData *tmfd,
                              LockTupleMode *lockmode,
                              bool *update_indexes,
                              uint64 *elapsed_us)
{
    uint64 wall_start = bcdb_get_time();
    TM_Result result;

    /*
     * Always use wait=true here.  The old phase-trace probe first called
     * table_tuple_update(..., wait=false) to count TM_BeingModified incidents,
     * but heap_update asserts on TM_BeingModified || !wait in this call path.
     * That made tracing itself capable of aborting a backend and invalidating
     * distributed runs.  Wall time still captures actual wait cost.
     */
    result = table_tuple_update(relation, tid, slot,
                                cid,
                                InvalidSnapshot,
                                InvalidSnapshot,
                                true,
                                tmfd, lockmode, update_indexes);

    if (elapsed_us != NULL)
        *elapsed_us = bcdb_get_time() - wall_start;

    return result;
}

static TM_Result
bcdb_table_tuple_delete_step1(Relation relation,
                              ItemPointer tid,
                              CommandId cid,
                              TM_FailureData *tmfd,
                              bool changingPart,
                              uint64 *elapsed_us)
{
    uint64 wall_start = bcdb_get_time();
    TM_Result result;

    /*
     * As with updates, do not use a wait=false tracing probe here.  The real
     * deterministic apply path must wait on in-flight tuple modifiers.
     */
    result = table_tuple_delete(relation, tid,
                                cid,
                                InvalidSnapshot,
                                InvalidSnapshot,
                                true,
                                tmfd,
                                changingPart);

    if (elapsed_us != NULL)
        *elapsed_us = bcdb_get_time() - wall_start;

    return result;
}

#define PREDFMT " %d:%d:%d:%d "
#define PRINT_PREDICATELOCKTARGETTAG(locktag)         \
    GET_PREDICATELOCKTARGETTAG_DB(locktag),           \
        GET_PREDICATELOCKTARGETTAG_RELATION(locktag), \
        GET_PREDICATELOCKTARGETTAG_PAGE(locktag),     \
        GET_PREDICATELOCKTARGETTAG_OFFSET(locktag)

#define WSTableGetPartitionIdx(hashcode) ((hashcode) % WRITE_CONFLICT_MAP_NUM_PARTITIONS)
#define WSTableMapAPartitionLock(table, hashcode) (&((table)->map_locks[(hashcode) % WRITE_CONFLICT_MAP_NUM_PARTITIONS].lock))
#define WSTableMapBPartitionLock(table, hashcode) (&((table)->mapB_locks[(hashcode) % WRITE_CONFLICT_MAP_NUM_PARTITIONS].lock))
#define WSTablePartitionLock(hashcode) (&(ws_table->map_locks[(hashcode) % WRITE_CONFLICT_MAP_NUM_PARTITIONS].lock))
#define RSTableGetPartitionIdx(hashcode) ((hashcode) % WRITE_CONFLICT_MAP_NUM_PARTITIONS)
#define RSTablePartitionLock(hashcode) (&(rs_table->map_locks[(hashcode) % WRITE_CONFLICT_MAP_NUM_PARTITIONS].lock))

/*
 * WSTableLockAllPartitions / WSTableUnlockAllPartitions
 *
 * DT hash rotation uses shm_hash_clear(), which rewrites the dynahash bucket
 * and freelist state in bulk.  Normal readers/writers protect each
 * hash_search_with_hash_value() with the partition spinlock of the shard they
 * access (map_locks for map, mapB_locks for mapB).  Rotation only needs to lock
 * all partition spinlocks of the INACTIVE shard being cleared.  Concurrent
 * readers/writers operating on the other (active) shard proceed uninterrupted.
 *
 * Locks are acquired in ascending index and released in reverse order.
 */
static void
WSTableLockAllPartitions(WSTable *table, bool lock_map_b)
{
	int i;
	WSPartitionLock *locks = lock_map_b ? table->mapB_locks : table->map_locks;

	for (i = 0; i < WRITE_CONFLICT_MAP_NUM_PARTITIONS; i++)
		SpinLockAcquire(&locks[i].lock);
}

static void
WSTableUnlockAllPartitions(WSTable *table, bool lock_map_b)
{
	int i;
	WSPartitionLock *locks = lock_map_b ? table->mapB_locks : table->map_locks;

	for (i = WRITE_CONFLICT_MAP_NUM_PARTITIONS - 1; i >= 0; i--)
		SpinLockRelease(&locks[i].lock);
}

/*
 * WSTableClearShard
 *
 * Safely clear one DT write-set shard while excluding concurrent probes and
 * inserts targeting THAT specific shard.  The other shard remains available for
 * active execution.
 */
static void
WSTableClearShard(WSTable *table, HTAB *map, bool clears_map_b,
				  uint64 *lock_acquire_us, uint64 *clear_us)
{
	uint64 lock_start;
	uint64 clear_start;

	if (lock_acquire_us != NULL)
		*lock_acquire_us = 0;
	if (clear_us != NULL)
		*clear_us = 0;

	lock_start = bcdb_get_time();
	WSTableLockAllPartitions(table, clears_map_b);
	if (lock_acquire_us != NULL)
		*lock_acquire_us = bcdb_get_time() - lock_start;
	clear_start = bcdb_get_time();
	shm_hash_clear(map, MAX_WRITE_CONFLICT);
	if (clears_map_b)
		pg_atomic_write_u32(&table->mapB_nonempty, 0);
	if (clear_us != NULL)
		*clear_us = bcdb_get_time() - clear_start;
	WSTableUnlockAllPartitions(table, clears_map_b);
}

/*
 * dummy_hash
 *
 * Identity hash for use with the WSTable partitioned hash tables.
 * The caller (PredicateLockTargetTagHashCode) already computes a uint32
 * hash of the PREDICATELOCKTARGETTAG; storing that precomputed value as
 * the key and using this function avoids double-hashing.
 */
uint32
dummy_hash(const void *key, Size key_size)
{
    return *(uint32 *)key;
}

BCDBShmXact *
create_tx(char *hash, char *sql, BCTxID tx_id, BCBlockID snapshot_block, int isolation, bool pred_lock)
{
    HASHCTL info;
    BCDBShmXact *tx;
    bool found;
    char key[TX_HASH_SIZE];
    Size hash_len;

    MemSet(&info, 0, sizeof(info));
    Assert(tx_pool != NULL);
    // printf("safeDB %s : %s: %d txid %d hash %s \n", __FILE__, __FUNCTION__, __LINE__ , tx_id, hash);
    if (hash == NULL)
        ereport(ERROR, (errmsg("[ZL] cannot create transaction with NULL hash")));
    hash_len = strlen(hash);
    if (hash_len >= TX_HASH_SIZE)
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("[ZL] transaction hash too long (%zu, max %d): \"%s\"",
                        (size_t)hash_len, TX_HASH_SIZE - 1, hash)));

    /*
     * tx_pool uses fixed-size keys (TX_HASH_SIZE) with default memcmp
     * comparisons.  Always pass a TX_HASH_SIZE-sized, zero-padded key buffer
     * to avoid out-of-bounds reads on caller-provided cstrings and to make
     * comparisons deterministic.
     */
    MemSet(key, 0, sizeof(key));
    memcpy(key, hash, hash_len);
    SpinLockAcquire(tx_pool_lock);
    tx = hash_search(tx_pool, key, HASH_ENTER, &found);
    if (found)
    {
#if SAFEDBG2
        printf("safeDB %s : %s: %d duplicate hash %s\n", __FILE__, __FUNCTION__, __LINE__, hash);
#endif
        ereport(DEBUG3,
                (errmsg("[ZL] transaction (%s) exists", hash)));
        SpinLockRelease(tx_pool_lock);
        return NULL;
    }
    LWLockInitialize(&tx->lock, LWTRANCHE_TX);
    SpinLockRelease(tx_pool_lock);
	LWLockAcquire(&tx->lock, LW_EXCLUSIVE);

    /* hash_search() already copied key bytes into tx->hash */
    strcpy(tx->sql, sql);
    tx->block_id_snapshot = snapshot_block;
    tx->block_id_committed = BCDBMaxBid;
    tx->tx_id = tx_id;
    tx->status = TX_SCHEDULING;
    tx->queryDesc = NULL;
    tx->portal = NULL;
    tx->sxact = NULL;
    tx->worker_pid = 0;
    tx->why_doomed[0] = '\0';
    tx->xid = InvalidTransactionId;
    tx->snap_xmin = InvalidTransactionId;
    tx->isolation = isolation;
    tx->pred_lock = pred_lock;
    tx->queue_link.tqe_prev = NULL;
    tx->create_time = 0;
    tx->has_raw = false;
    tx->has_war = false;

	/*
	 * A tx-pool entry can be recycled after the previous transaction is
	 * deleted.  Initialise the complete safe-ledger metadata here rather
	 * than relying on hash-table allocation to return zeroed memory.  A stale
	 * raft_ledger_enabled bit is especially dangerous: it diverts an ordinary
	 * transaction away from the synchronous Merkle PRE_COMMIT path and makes
	 * the worker attempt ledger finalisation with log_index=0.
	 */
	tx->raft_ledger_enabled = false;
	tx->raft_log_index = 0;
	tx->raft_item_ordinal = 0;
	tx->raft_item_count = 0;
	memset(tx->raft_epoch_id, 0, BCDB_RAFT_DIGEST_BYTES);
	memset(tx->raft_entry_digest, 0, BCDB_RAFT_DIGEST_BYTES);
	memset(tx->raft_item_digest, 0, BCDB_RAFT_DIGEST_BYTES);
	memset(tx->raft_terminal_digest, 0, BCDB_RAFT_DIGEST_BYTES);
	tx->raft_terminal_format_version = 0;
	tx->raft_terminal_state = 0;
	tx->raft_terminal_update_confirmed = false;
	tx->raft_terminal_returning_verified = false;
	tx->raft_terminal_verified_top_xid = InvalidTransactionId;
	tx->raft_terminal_verified_nest_level = -1;
    SHA256_Init(&tx->state_hash);
    SIMPLEQ_INIT(&tx->optim_write_list);

    ConditionVariableInit(&tx->cond);
    // mapTxToShm[ MAP_TX(tx_id) ] = tx;
#if SAFEDBG2
    printf("safeDB %s : %s: %d \n", __FILE__, __FUNCTION__, __LINE__);
#endif
    LWLockRelease(&tx->lock);
    return tx;
}

/*
 * delete_tx
 *
 * Removes tx from the xid_map (via remove_tx_xid_map) then from tx_pool.
 * The xid_map removal must come first because it also acquires the per-tx
 * LWLock and drops it, ensuring no concurrent reader holds the lock while
 * the entry is freed.
 *
 * Safe to call with tx==NULL (no-op).
 */
void delete_tx(BCDBShmXact *tx)
{
    bool found;
    if (!tx)
        return;

    BCDB_XIDMAP_LOG("[BCDB_XIDMAP] delete_tx enter pid=%d tx_ptr=%p txid=%d xid=%u status=%d hash=%s",
                    (int)getpid(), (void *)tx, (int)tx->tx_id, (unsigned int)tx->xid,
                    (int)tx->status, tx->hash);
    DEBUGNOCHECK("[ZL] deleting tx %s", tx->hash);
    remove_tx_xid_map(tx->xid);

    SpinLockAcquire(tx_pool_lock);
    hash_search(tx_pool, tx->hash, HASH_REMOVE, &found);
    SpinLockRelease(tx_pool_lock);
    BCDB_XIDMAP_LOG("[BCDB_XIDMAP] delete_tx pool_remove pid=%d tx_ptr=%p txid=%d xid=%u found=%d",
                    (int)getpid(), (void *)tx, (int)tx->tx_id, (unsigned int)tx->xid,
                    found ? 1 : 0);
    if (found)
        DEBUGNOCHECK("[ZL] removed tx %s", tx->hash);
}

/*
 * create_tx_pool
 *
 * Allocates and initialises all shared-memory structures used by the
 * transaction subsystem.  Called once from the postmaster during
 * shared-memory setup, before any worker or backend process is forked.
 *
 * Allocations:
 *   - restart_counter_lock    one spinlock (currently unused counter)
 *   - tx_pool_lock / xid_map_lock  two spinlocks packed into one ShmemInitStruct
 *   - tx_pool                 hash table of BCDBShmXact, keyed by hash string
 *   - xid_map                 hash table of XidMapEntry, keyed by TransactionId
 *   - tx_queues               array of NUM_TX_QUEUE_PARTITION TxQueue structs,
 *                             each with its own spinlock + 2 condition variables
 *   - ws_table / rs_table     partitioned write-set and read-set conflict maps
 *                             (dual tables map+mapB for ping-pong; only ws_table
 *                             uses the Deterministic Execution (DT) scheme;
 *                             rs_table has one shard)
 */
void create_tx_pool(void)
{
    HASHCTL info;
    slock_t *tx_pool_lock_array;
    bool found;
	bool locks_found;
	bool failpoint_found;

    restart_counter_lock = ShmemInitStruct("restart_counter_lock", sizeof(slock_t), &found);
    // nexec_lock = ShmemInitStruct("nexec_lock", sizeof(slock_t) , &found);
#if SAFEDBG
    DEBUGNOCHECK("[BCDB] create_tx_pool (%s:%s:%d)", __FILE__, __FUNCTION__, __LINE__);
#endif
	tx_pool_lock_array = ShmemInitStruct("tx_pool_lock", sizeof(slock_t) * 2, &locks_found);
    tx_pool_lock = tx_pool_lock_array;
    xid_map_lock = tx_pool_lock + 1;
	bcdb_safe_failpoint_fired = ShmemInitStruct("bcdb_safe_failpoint_fired",
												sizeof(pg_atomic_uint32),
												&failpoint_found);
	if (!failpoint_found)
		pg_atomic_init_u32(bcdb_safe_failpoint_fired, 0);
    numExecPt = (int *)ShmemAlloc(sizeof(int));
    *numExecPt = 0;

	if (!locks_found)
    {
        SpinLockInit(tx_pool_lock);
        SpinLockInit(restart_counter_lock);
        // SpinLockInit(nexec_lock);
        SpinLockInit(xid_map_lock);
    }

    MemSet(&info, 0, sizeof(info));
    info.keysize = TX_HASH_SIZE;
    info.entrysize = sizeof(BCDBShmXact);
    info.hash = string_hash;
    tx_pool = ShmemInitHash("bcdb_tx_pool",
                            MAX_SHM_TX,
                            MAX_SHM_TX,
                            &info, HASH_ELEM | HASH_FUNCTION | HASH_FIXED_SIZE);

    info.keysize = sizeof(TransactionId);
    info.entrysize = sizeof(XidMapEntry);
    info.hash = uint32_hash;
    xid_map = ShmemInitHash("bcdb_xid_map",
                            MAX_SHM_TX,
                            MAX_SHM_TX,
                            &info, HASH_ELEM | HASH_FUNCTION | HASH_FIXED_SIZE);

    tx_queues = ShmemInitStruct("bcdb_tx_queue", sizeof(TxQueue) * NUM_TX_QUEUE_PARTITION, &found);
    for (int i = 0; i < NUM_TX_QUEUE_PARTITION; i++)
    {
        TAILQ_INIT(&tx_queues[i].list);
        SpinLockInit(&tx_queues[i].lock);
        tx_queues[i].size = 0;
        ConditionVariableInit(&tx_queues[i].empty_cond);
        ConditionVariableInit(&tx_queues[i].full_cond);
    }

    ws_table = ShmemInitStruct("bcdb_tx_ws_table", sizeof(WSTable), &found);
    if (!found)
	{
#if SAFEDBG
		DEBUGNOCHECK("[BCDB] init ws_table pid=%d (%s:%s:%d)",
					 (int)getpid(), __FILE__, __FUNCTION__, __LINE__);
#endif
		MemSet(&info, 0, sizeof(info));
		info.keysize = sizeof(PREDICATELOCKTARGETTAG);
		info.entrysize = sizeof(WSTableEntry);
		info.num_partitions = WRITE_CONFLICT_MAP_NUM_PARTITIONS;
		ws_table->map = ShmemInitHash("bcdb_write_conflict_map",
									  MAX_WRITE_CONFLICT,
									  MAX_WRITE_CONFLICT,
									  &info, HASH_ELEM | HASH_BLOBS | HASH_FIXED_SIZE | HASH_PARTITION);
		ws_table->mapB = ShmemInitHash("bcdb_write_conflict_mapDT",
									   MAX_WRITE_CONFLICT,
									   MAX_WRITE_CONFLICT,
									   &info, HASH_ELEM | HASH_BLOBS | HASH_FIXED_SIZE | HASH_PARTITION);
		ws_table->mapActive = ws_table->map;
		pg_atomic_init_u32(&ws_table->mapB_nonempty, 0);
		for (int i = 0; i < WRITE_CONFLICT_MAP_NUM_PARTITIONS; i++)
		{
			SpinLockInit(&(ws_table->map_locks[i].lock));
			SpinLockInit(&(ws_table->mapB_locks[i].lock));
		}
	}
	else
	{
#if SAFEDBG
		DEBUGNOCHECK("[BCDB] ws_table already exists (%s:%s:%d)",
					 __FILE__, __FUNCTION__, __LINE__);
#endif
	}

	rs_table = ShmemInitStruct("bcdb_tx_rs_table", sizeof(WSTable), &found);
	if (!found)
	{
		MemSet(&info, 0, sizeof(info));
		info.keysize = sizeof(PREDICATELOCKTARGETTAG);
		info.entrysize = sizeof(WSTableEntry);
		info.num_partitions = WRITE_CONFLICT_MAP_NUM_PARTITIONS;
		rs_table->map = ShmemInitHash("bcdb_read_conflict_map",
									  MAX_WRITE_CONFLICT,
									  MAX_WRITE_CONFLICT,
									  &info, HASH_ELEM | HASH_BLOBS | HASH_FIXED_SIZE | HASH_PARTITION);
		rs_table->mapB = NULL;
		rs_table->mapActive = rs_table->map;
		pg_atomic_init_u32(&rs_table->mapB_nonempty, 0);
		for (int i = 0; i < WRITE_CONFLICT_MAP_NUM_PARTITIONS; i++)
		{
			SpinLockInit(&(rs_table->map_locks[i].lock));
			SpinLockInit(&(rs_table->mapB_locks[i].lock));
		}
    }
}

Size tx_pool_size(void)
{
	Size ret = hash_estimate_size(MAX_SHM_TX, sizeof(BCDBShmXact));
	ret = add_size(ret, hash_estimate_size(MAX_SHM_TX, sizeof(XidMapEntry)));
	ret = add_size(ret, sizeof(slock_t) * 2);
	ret = add_size(ret, sizeof(pg_atomic_uint32));
	ret = add_size(ret, sizeof(TxQueue) * NUM_TX_QUEUE_PARTITION);
	ret = add_size(ret, sizeof(WSTable) * 2);
	ret = add_size(ret, hash_estimate_size(MAX_WRITE_CONFLICT, sizeof(WSTableEntry)));
	return ret;
}

void clear_tx_pool(void)
{
#if SAFEDBG
    DEBUGNOCHECK("[BCDB] clear_tx_pool (%s:%s:%d)", __FILE__, __FUNCTION__, __LINE__);
#endif
    shm_hash_clear(tx_pool, MAX_SHM_TX);
    shm_hash_clear(xid_map, MAX_SHM_TX);
    for (int i = 0; i < NUM_TX_QUEUE_PARTITION; i++)
        TAILQ_INIT(&tx_queues[i].list);
}

/*
 * rs_table_reserveDT
 *
 * Deterministic Execution (DT) path read-set reservation.  Intentionally
 * lightweight: just appends a WSTableEntryRecord to the process-local
 * rs_table_record linked list WITHOUT touching the shared rs_table hash.
 * The shared table is only queried (not written) during conflict_checkDT(),
 * so there is no per-read write contention.
 *
 * Called from predicate.c whenever PostgreSQL SSI records a predicate lock
 * for a BCDB transaction.
 *
/*
 * bcdb_compute_intkey_tag
 *
 * Unified computation of primary-key hash tags for BCDB write-set and read-set
 * conflict tracking. Uses fixed dbOid=0 and the table's relation Oid, hashing
 * the 32-bit integer key with hash_any().
 */
void bcdb_compute_intkey_tag(PREDICATELOCKTARGETTAG *tag, Oid relOid, int32 intKey)
{
	uint32 h = hash_any((unsigned char *) &intKey, sizeof(int32));
	SET_PREDICATELOCKTARGETTAG_TUPLE(*tag, 0, relOid,
									 (BlockNumber)(h >> 16),
									 (OffsetNumber)((h & 0xFFFF) | 1));
}

void rs_table_reserveDT(const PREDICATELOCKTARGETTAG *tag)
{
	if (!bcdb_dt_conflict_tracking || bcdb_tx_context == NULL || activeTx == NULL)
	{
		(void)tag;
		return;
	}

	WSTableEntryRecord *record;
	record = MemoryContextAlloc(bcdb_tx_context, sizeof(WSTableEntryRecord));
	record->tag = *tag;
	LIST_INSERT_HEAD(&rs_table_record, record, link);
}

/*
 * ws_table_reserveDT
 *
 * Deterministic Execution (DT) path write-set reservation.  Like
 * rs_table_reserveDT, this only appends to the process-local ws_table_record
 * list.  The actual write to the shared DT hash table happens later in
 * publish_ws_tableDT(), after the tx's serial ordering slot is decided.
 * This separation means:
 *   - No shared-table write contention during execution.
 *   - Only committed write-sets are ever published, keeping the table clean.
 *
 * Called from nodeModifyTable.c (INSERT/UPDATE/DELETE paths) and from
 * worker.c (re-reservation after a retry).
 *
 * NOTE: ws_table_reserve() (the old non-DT variant that wrote eagerly to
 * ws_table->map) has been removed — all its call sites were replaced by
 * this function.
 */
void ws_table_reserveDT(PREDICATELOCKTARGETTAG *tag)
{
	if (!bcdb_dt_conflict_tracking || bcdb_tx_context == NULL || activeTx == NULL)
	{
		(void)tag;
		return;
	}

	WSTableEntryRecord *record;
	record = MemoryContextAlloc(bcdb_tx_context, sizeof(WSTableEntryRecord));
	record->tag = *tag;
	LIST_INSERT_HEAD(&ws_table_record, record, link);
}

/*
 * bcdb_xid_preceded_snapshot
 *
 * T3-v2: returns true when the given AriaBC tx_id committed its PostgreSQL
 * transaction BEFORE the current backend's snapshot xmin.  In that case the
 * candidate tx's writes are already visible in our portal_run snapshot, so
 * there can be no undetected write-write conflict — the conflict check can
 * safely skip it.
 *
 * Safety:
 *   Writer ordering in worker.c finish path:
 *     1. __atomic_store_n(&result_commit_xid[slot], tx->xid, RELEASE)
 *     2. __atomic_store_n(&result_committed_txid[slot], tx_id,  RELEASE)
 *   Reader ordering here:
 *     1. ACQUIRE load of result_committed_txid[slot] (confirms committed)
 *     2. ACQUIRE load of result_commit_xid[slot]    (guaranteed visible)
 *   => When step 2 is read, step 1 of the writer is always visible.
 *
 * Returns false (conservative) when the XID is not yet set, the block is
 * unavailable, or the commit happened after our snapshot.
 */
static bool
bcdb_xid_preceded_snapshot(BCTxID cand_id)
{
    BCBlock *blk;
    int slots;
    int slot;
    BCTxID published;
    TransactionId cxid;

    if (!TransactionIdIsValid(activeTx->snap_xmin))
        return false;

    blk = bcdb_get_block1();
    if (blk == NULL)
        return false;

    slots = bcdb_get_runtime_result_ring_slots();
    if (slots < 1)
        slots = 1;
    slot = (int)(cand_id % (BCTxID)slots);
    if (slot < 0)
        slot += slots;

    published = __atomic_load_n(&blk->result_committed_txid[slot], __ATOMIC_ACQUIRE);
    if (published != cand_id)
        return false; /* not yet committed or slot is for a different tx_id */

    cxid = __atomic_load_n(&blk->result_commit_xid[slot], __ATOMIC_ACQUIRE);
    if (!TransactionIdIsValid(cxid))
        return false;

    return TransactionIdPrecedes(cxid, activeTx->snap_xmin);
}

static void
bcdb_log_dt_conflict_detail(const char *source,
                            PREDICATELOCKTARGETTAG *tag,
                            uint32 tuple_hash,
                            BCTxID cand_id)
{
    BCBlock *blk;
    int slots = 0;
    int slot = -1;
    BCTxID published = -1;
    TransactionId cxid = InvalidTransactionId;
    bool committed_before_snapshot = false;

    bcdb_last_conflict_txid = cand_id;

    if (!bcdb_flow_debug_enabled())
        return;

    blk = bcdb_get_block1();
    if (blk != NULL)
    {
        slots = bcdb_get_runtime_result_ring_slots();
        if (slots < 1)
            slots = 1;

        slot = (int)(cand_id % (BCTxID)slots);
        if (slot < 0)
            slot += slots;

        published = __atomic_load_n(&blk->result_committed_txid[slot],
                                    __ATOMIC_ACQUIRE);
        cxid = __atomic_load_n(&blk->result_commit_xid[slot],
                               __ATOMIC_ACQUIRE);
        if (published == cand_id &&
            TransactionIdIsValid(cxid) &&
            TransactionIdIsValid(activeTx->snap_xmin))
            committed_before_snapshot =
                TransactionIdPrecedes(cxid, activeTx->snap_xmin);
    }

    BCDB_FLOW_LOG("[BCDB_FLOW] dt_conflict map=%s pid=%d txid=%d cand_txid=%d baseline=%d snap_xmin=%u cand_slot=%d cand_published=%d cand_cxid=%u cand_before_snapshot=%d hash=%u tag_type=%d db=%u rel=%u page=%u off=%u",
                  source,
                  (int)getpid(),
                  activeTx ? (int)activeTx->tx_id : -1,
                  (int)cand_id,
                  activeTx ? (int)activeTx->tx_id_committed : -1,
                  (unsigned int)(activeTx ? activeTx->snap_xmin : InvalidTransactionId),
                  slot,
                  (int)published,
                  (unsigned int)cxid,
                  committed_before_snapshot ? 1 : 0,
                  tuple_hash,
                  (int)GET_PREDICATELOCKTARGETTAG_TYPE(*tag),
                  (unsigned int)GET_PREDICATELOCKTARGETTAG_DB(*tag),
                  (unsigned int)GET_PREDICATELOCKTARGETTAG_RELATION(*tag),
                  (unsigned int)GET_PREDICATELOCKTARGETTAG_PAGE(*tag),
                  (unsigned int)GET_PREDICATELOCKTARGETTAG_OFFSET(*tag));
}

/*
 * table_checkDT
 *
 * Core Deterministic Execution (DT) conflict check.  Queries BOTH shards of
 * the given WSTable (table->map and table->mapB) for the given tag.  A
 * conflict is detected when the entry's tx_id is strictly LESS than the
 * current tx's tx_id (meaning an earlier-ordered tx wrote that slot) AND
 * strictly GREATER than tx_id_committed (meaning it wasn't committed before
 * our snapshot — a committed write wouldn't constitute a unseen conflict).
 *
 * T3-v2 extension: even when tx_id > tx_id_committed (watermark lags),
 * if the candidate tx committed before our snapshot (XID < snap_xmin) it is
 * safe to skip — portal_run already observed its writes.
 *
 * Holds the per-partition spinlock only for the hash lookup to minimise
 * contention; the early-exit paths release it immediately on conflict.
 *
 * Used by ws_table_checkDT() and indirectly by conflict_checkDT().
 */
bool table_checkDT(PREDICATELOCKTARGETTAG *tag, WSTable *table)
{
    uint32 tuple_hash = PredicateLockTargetTagHashCode(tag);
    static int once_out = false;

    if (!once_out)
    {
        once_out = true;
#if SAFEDBG2
        printf("\nsafeDB %s : %s: %d  getpid %d tx_id %d\n",
               __FILE__, __FUNCTION__, __LINE__, getpid(), activeTx->tx_id);
#endif
    }

    {
        bool found;
        WSTableEntry *entry;
        BCTxID cand_id;
		slock_t *partition_lock = WSTableMapAPartitionLock(table, tuple_hash);
        uint64 probe_lock_start = bcdb_ptrace_timer_start();

        SpinLockAcquire(partition_lock);
        bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_WS_PROBE_MAPA_LOCK_WAIT_US, probe_lock_start);
        
        uint64 probe_hash_start = bcdb_ptrace_timer_start();
        entry = hash_search_with_hash_value(table->map, tag,
                                            tuple_hash, HASH_FIND, &found);
        if (found && (entry->tx_id < activeTx->tx_id) &&
            (entry->tx_id > activeTx->tx_id_committed))
        {
			cand_id = entry->tx_id;
			SpinLockRelease(partition_lock);
			bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_WS_PROBE_MAPA_HASH_LOOKUP_US, probe_hash_start);
			bcdb_last_conflict_txid = cand_id;
			bcdb_log_dt_conflict_detail("map", tag, tuple_hash, cand_id);
			return true;
		}
		SpinLockRelease(partition_lock);
		bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_WS_PROBE_MAPA_HASH_LOOKUP_US, probe_hash_start);
	}

	/*
	 * mapB is only cleared when it becomes active for a new epoch. Once a
	 * publish inserts there, the flag stays set until that clear, so we can
	 * skip the second probe without paying hash_get_num_entries(mapB) on every
	 * conflict check.
	 */
	if (pg_atomic_read_u32(&table->mapB_nonempty) != 0)
	{
		bool found;
		WSTableEntry *entry;
		BCTxID cand_id;
		slock_t *partition_lock = WSTableMapBPartitionLock(table, tuple_hash);
		uint64 probe_lock_start = bcdb_ptrace_timer_start();

		SpinLockAcquire(partition_lock);
		bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_WS_PROBE_MAPB_LOCK_WAIT_US, probe_lock_start);

		uint64 probe_hash_start = bcdb_ptrace_timer_start();
		entry = hash_search_with_hash_value(table->mapB, tag,
											tuple_hash, HASH_FIND, &found);
		if (found && (entry->tx_id < activeTx->tx_id) &&
			(entry->tx_id > activeTx->tx_id_committed))
		{
			cand_id = entry->tx_id;
			SpinLockRelease(partition_lock);
			bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_WS_PROBE_MAPB_HASH_LOOKUP_US, probe_hash_start);
			bcdb_last_conflict_txid = cand_id;
			bcdb_log_dt_conflict_detail("mapB", tag, tuple_hash, cand_id);
			return true;
        }
        SpinLockRelease(partition_lock);
        bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_WS_PROBE_MAPB_HASH_LOOKUP_US, probe_hash_start);
    }

check_done:;

    DEBUGMSG("safeDB tx %s check write %d win", activeTx->hash, tuple_hash);
    return false;
}

/*
 * ws_table_checkDT
 *
 * Convenience wrapper: checks the DT (Deterministic Execution) tables
 * for the given tag.  Returns true if a waw (write-after-write) conflict
 * is detected.
 */
bool ws_table_checkDT(PREDICATELOCKTARGETTAG *tag)
{
    return table_checkDT(tag, ws_table);
}

/*
 * ws_table_check  (non-DT path)
 *
 * Queries only ws_table->map (the single, non-DT shard).
 * Used by conflict_check() in the non-DT execution path.
 *
 * NOTE: Because ws_table_reserve() has been removed (all callers migrated
 * to ws_table_reserveDT), ws_table->map is never populated in the current
 * codebase.  This function will therefore always return false, making
 * conflict_check() a no-op for conflict detection.  Kept to avoid
 * breaking the non-DT code path structure.
 */
bool ws_table_check(PREDICATELOCKTARGETTAG *tag)
{
    bool found;
    WSTableEntry *entry;
    uint32 tuple_hash = PredicateLockTargetTagHashCode(tag);
    slock_t *partition_lock = WSTablePartitionLock(tuple_hash);

#if SAFEDBG
    printf("\nariaDB %s : %s: %d \n", __FILE__, __FUNCTION__, __LINE__);
#endif

    SpinLockAcquire(partition_lock);
    entry = hash_search_with_hash_value(ws_table->map, tag, tuple_hash, HASH_FIND, &found);
    if (found && entry->tx_id < activeTx->tx_id)
    {
        // DEBUGMSG("[ZL] tx %s check write %d failed, winner: %d", activeTx->hash, tuple_hash, entry->tx_id);
        SpinLockRelease(partition_lock);
        return true;
    }
    DEBUGMSG("[ZL] tx %s check write %d win", activeTx->hash, tuple_hash);
    SpinLockRelease(partition_lock);
    return false;
}

/*
 * rs_table_check  (non-DT path)
 *
 * Queries rs_table->map for the given tag to detect raw (read-after-write)
 * conflicts.  Used by conflict_check() in the non-DT path.
 *
 * Same caveat as ws_table_check: rs_table_reserve() (which wrote
 * rs_table->map) has been removed, so this function always returns false.
 */
bool rs_table_check(PREDICATELOCKTARGETTAG *tag)
{
    bool found;
    WSTableEntry *entry;
    uint32 tuple_hash = PredicateLockTargetTagHashCode(tag);
    slock_t *partition_lock = RSTablePartitionLock(tuple_hash);

#if SAFEDBG
    printf("ariaDB %s : %s: %d \n", __FILE__, __FUNCTION__, __LINE__);
#endif

    SpinLockAcquire(partition_lock);
    entry = hash_search_with_hash_value(rs_table->map, tag, tuple_hash, HASH_FIND, &found);
    if (found && entry->tx_id < activeTx->tx_id)
    {
        // DEBUGMSG("[ZL] tx %s check read %d failed, winner: %d", activeTx->hash, tuple_hash, entry->tx_id);
        SpinLockRelease(partition_lock);
        return true;
    }
    // DEBUGMSG("[ZL] tx %s check read %d win", activeTx->hash, tuple_hash);
    SpinLockRelease(partition_lock);
    return false;
}

/*
 * clean_ws_table_record and clean_rs_table_record have been removed.
 *
 * They were the non-DT per-entry cleanup functions that removed individual
 * entries from ws_table->map / rs_table->map using the ws/rs_table_record
 * local lists.  They had no callers: the DT path relies on
 * clean_rs_ws_table() (bulk hash clear) called from worker.c and tcop/
 * postgres.c.  The per-entry removal logic is unnecessary when the whole
 * table is cleared at block boundaries.
 */

/*
 * tx_queue_insert
 *
 * Enqueues tx onto the TxQueue shard identified by (partition % num_queue).
 * Blocks (using full_cond condition variable) if the queue already holds
 * QUEUEING_BLOCKS entries, providing back-pressure on frontend backends.
 *
 * num_queue is re-read from the sentinel BCBlock every call so it picks up
 * runtime changes to the worker count without restart.
 *
 * Callers (frontend backends) hold no locks when they call this — the queue's
 * own spinlock serialises all enqueue/dequeue operations.
 */
void tx_queue_insert(BCDBShmXact *tx, int32 partition)
{
    struct timeval tv1;
    tv1.tv_sec = 0;
    tv1.tv_usec = 0;
    bool found;

    int num_queue = OEP_mode ? blocksize * 2 : blocksize;
    num_queue = get_blksz(); // get_block_by_id(1, false)->blksize; ==nWorker

#if SAFEDBG
    DEBUGNOCHECK("safeDB %s:%s:%d partition %d num_queue %d txsql %s",
                 __FILE__, __FUNCTION__, __LINE__, partition, num_queue, tx->sql);
#endif
    TxQueue *queue = tx_queues + (partition % num_queue);
    ConditionVariablePrepareToSleep(&queue->full_cond);
    SpinLockAcquire(&queue->lock);
    while (queue->size > QUEUEING_BLOCKS)
    {
        SpinLockRelease(&queue->lock);
        ConditionVariableSleep(&queue->full_cond, WAIT_EVENT_BLOCK_COMMIT);
        SpinLockAcquire(&queue->lock);
    }
    ConditionVariableCancelSleep();
    TAILQ_INSERT_TAIL(&queue->list, tx, queue_link);
    /* #define TAILQ_INSERT_TAIL(head, elm, field) do {			\
        (elm)->field.tqe_next = NULL;					\
        (elm)->field.tqe_prev = (head)->tqh_last;			\
        *(head)->tqh_last = (elm);					\
        (head)->tqh_last = &(elm)->field.tqe_next;			\
        printf("safeDB %s : %s: %d partition %d num_queue %d \n", __FILE__, __FUNCTION__, __LINE__ , partition, num_queue);
    fflush(0);
        //(tx)->queue_link.tqe_next = NULL;
        printf("safeDB %s : %s: %d  \n", __FILE__, __FUNCTION__, __LINE__  );
    fflush(0);
        (tx)->queue_link.tqe_prev = (&queue->list)->tqh_last;
        printf("safeDB %s : %s: %d  \n", __FILE__, __FUNCTION__, __LINE__  );
    fflush(0);
        *(&queue->list)->tqh_last = (tx);
        printf("safeDB %s : %s: %d  \n", __FILE__, __FUNCTION__, __LINE__  );
        (&queue->list)->tqh_last = NULL; // &(tx)->queue_link.tqe_next;
        printf("safeDB %s : %s: %d  \n", __FILE__, __FUNCTION__, __LINE__  );
    */

    tx->queue_partition = partition;
    queue->size += 1;
#if SAFEDBG
	gettimeofday(&tv1, NULL);
    printf(" safeDB func %s hash %s time= %ld.%ld\n", __FUNCTION__, tx->hash, tv1.tv_sec, tv1.tv_usec);
#endif
    SpinLockRelease(&queue->lock);
    ConditionVariableSignal(&queue->empty_cond);
}

/*
 * tx_queue_next
 *
 * Dequeues and returns the next BCDBShmXact from the TxQueue shard
 * identified by (partition % num_queue).  Blocks (using empty_cond
 * condition variable) until at least one transaction is available.
 *
 * Called exclusively by worker processes.  Only one worker dequeues from
 * any given partition shard, so there is no consumer-side contention beyond
 * the spinlock.
 *
 * Signals full_cond after dequeue to unblock any producer held in
 * tx_queue_insert due to back-pressure.
 */
BCDBShmXact *
tx_queue_next(int32 partition)
{
    struct timeval tv1;
    BCDBShmXact *tx;
    int num_queue = OEP_mode ? blocksize * 2 : blocksize;
    TxQueue *queue;
    uint64 queue_wait_start;

    tv1.tv_sec = 0;
    tv1.tv_usec = 0;
    num_queue = get_blksz(); // get_block_by_id(1, false)->blksize; ==nWorker
    queue = tx_queues + (partition % num_queue);
    queue_wait_start = bcdb_ptrace_timer_start();
    ConditionVariablePrepareToSleep(&queue->empty_cond);
    SpinLockAcquire(&queue->lock);
#if SAFEDBG
    printf("safeDB %s : %s: %d partition %d num_queue %d \n", __FILE__, __FUNCTION__, __LINE__, partition, num_queue);
#endif
    /*
    //SpinLockAcquire(nexec_lock);
    *numExecPt -= 1;
    //SpinLockRelease(nexec_lock);
    */
#if SAFEDBG
	gettimeofday(&tv1, NULL);
	printf("\n safeDB func %s time= %ld.%ld\n", __FUNCTION__, tv1.tv_sec, tv1.tv_usec);
#endif
    while (queue->size <= 0)
    {

        SpinLockRelease(&queue->lock);
        ConditionVariableSleep(&queue->empty_cond, WAIT_EVENT_TX_READY_TO_COMMIT);
        SpinLockAcquire(&queue->lock);
    }
    ConditionVariableCancelSleep();
    if (queue_wait_start != 0)
        bcdb_ptrace_note_queue_pop_wait(bcdb_ptrace_now_us() - queue_wait_start);
    tx = TAILQ_FIRST(&queue->list);
    TAILQ_REMOVE(&queue->list, tx, queue_link);
    tx->queue_link.tqe_prev = NULL;
#if SAFEDBG
	gettimeofday(&tv1, NULL);
    printf("\n func %s hash %s time= %ld.%ld", __FUNCTION__, tx->hash, tv1.tv_sec, tv1.tv_usec);
    printf("safeDB %s : %s: %d pid %d tx %d\n", __FILE__, __FUNCTION__, __LINE__, getpid(), tx->tx_id);
#endif
    queue->size -= 1;
    SpinLockRelease(&queue->lock);
    ConditionVariableSignal(&queue->full_cond);
    return tx;
}

#define BCDB_MAX_CACHED_DESCS 8

typedef struct BCDBTupleDescCacheEntry
{
	Oid			relOid;
	TupleDesc	desc;
} BCDBTupleDescCacheEntry;

static BCDBTupleDescCacheEntry bcdb_td_cache[BCDB_MAX_CACHED_DESCS];
static int bcdb_td_cache_count = 0;
static BCTxID bcdb_td_cache_txid = -1;

void
bcdb_reset_tupledesc_cache(void)
{
	bcdb_td_cache_count = 0;
	bcdb_td_cache_txid = -1;
}

static void
bcdb_tupledesc_cache_reset_cb(void *arg)
{
	bcdb_reset_tupledesc_cache();
}

static TupleTableSlot *
clone_slot(TupleTableSlot *slot)
{
	TupleTableSlot *ret;
	TupleDesc	desc = NULL;
	Oid			relOid = slot->tts_tableOid;

	if (activeTx != NULL && activeTx->tx_id != bcdb_td_cache_txid)
	{
		bcdb_td_cache_count = 0;
		bcdb_td_cache_txid = activeTx->tx_id;
	}

	if (OidIsValid(relOid))
	{
		for (int i = 0; i < bcdb_td_cache_count; i++)
		{
			if (bcdb_td_cache[i].relOid == relOid)
			{
				desc = bcdb_td_cache[i].desc;
				break;
			}
		}
	}

	if (desc == NULL)
	{
		/*
		 * If this is the first cache entry in bcdb_tx_context, register a
		 * reset callback so that any MemoryContextReset(bcdb_tx_context)
		 * automatically resets the cache before memory is freed.
		 */
		if (bcdb_td_cache_count == 0 && bcdb_tx_context != NULL)
		{
			MemoryContextCallback *cb = (MemoryContextCallback *)
				MemoryContextAlloc(bcdb_tx_context, sizeof(MemoryContextCallback));
			cb->func = bcdb_tupledesc_cache_reset_cb;
			cb->arg = NULL;
			MemoryContextRegisterResetCallback(bcdb_tx_context, cb);
		}

		/*
		 * Create an independent copy in bcdb_tx_context so that the cloned
		 * slot's descriptor outlives the transient portal/executor context
		 * without pinning the portal's original TupleDesc.
		 */
		desc = CreateTupleDescCopy(slot->tts_tupleDescriptor);
		desc->tdrefcount = -1;
		if (OidIsValid(relOid) && bcdb_td_cache_count < BCDB_MAX_CACHED_DESCS)
		{
			bcdb_td_cache[bcdb_td_cache_count].relOid = relOid;
			bcdb_td_cache[bcdb_td_cache_count].desc = desc;
			bcdb_td_cache_count++;
		}
	}

	/*
	 * CRITICAL: Use TTSOpsHeapTuple (not the source slot's ops) to ensure the
	 * clone MATERIALIZES the data with its own heap-allocated copy.
	 *
	 * Because 'desc' has tdrefcount == -1, MakeTupleTableSlot does not register
	 * it with CurrentResourceOwner, avoiding leak warnings when the portal drops.
	 */
	ret = MakeTupleTableSlot(desc, &TTSOpsHeapTuple);
	ExecCopySlot(ret, slot);
	ret->tts_tableOid = relOid;
	return ret;
}

/*
 * get_tx_by_hash
 *
 * Looks up tx_pool by the transaction's string hash.  Acquires tx_pool_lock
 * for the duration of the hash_search (HASH_FIND).  Returns NULL if not found.
 * Does NOT acquire the per-tx LWLock; callers that need to mutate the entry
 * must acquire tx->lock themselves.
 */
BCDBShmXact *
get_tx_by_hash(const char *hash)
{
    BCDBShmXact *ret;
    SpinLockAcquire(tx_pool_lock);
    ret = hash_search(tx_pool, hash, HASH_FIND, NULL);
    SpinLockRelease(tx_pool_lock);
    return ret;
}

/*
 * get_tx_by_xid and get_tx_by_xid_locked have been restored.
 */
BCDBShmXact *
get_tx_by_xid(TransactionId xid)
{
	XidMapEntry *entry;
	bool found;
	BCDBShmXact *tx = NULL;

	if (!TransactionIdIsValid(xid))
		return NULL;

	SpinLockAcquire(xid_map_lock);
	entry = hash_search(xid_map, &xid, HASH_FIND, &found);
	if (found && entry != NULL)
		tx = entry->tx;
	SpinLockRelease(xid_map_lock);

	return tx;
}


/*
 * add_tx_xid_map
 *
 * Associates PostgreSQL TransactionId xid with the BCDBShmXact tx in the
 * xid_map hash table.  Called from the worker when it begins executing a
 * transaction (so that SSI callbacks — which only know the XID — can
 * reach the BCDBShmXact).
 *
 * Calls ereport(FATAL) if the XID is already mapped (indicates a bug).
 */
void add_tx_xid_map(TransactionId xid, BCDBShmXact *tx)
{
    XidMapEntry *entry;
    bool found;
    uint64 lock_wait_us = 0;
    uint64 lock_t0 = 0;

    if (bcdb_xidmap_debug_enabled())
        lock_t0 = bcdb_get_time();

    DEBUGNOCHECK("[ZL] add xid map: %d -> %s", (int)xid, tx->hash);
    SpinLockAcquire(xid_map_lock);
    if (bcdb_xidmap_debug_enabled())
        lock_wait_us = bcdb_get_time() - lock_t0;

    entry = hash_search(xid_map, &xid, HASH_ENTER, &found);
    if (!found)
    {
        entry->xid = xid;
        entry->tx = tx;
    }
    else
    {
        ereport(FATAL, (errmsg("[ZL] already occupied by %d", (int)entry->xid)));
    }
    SpinLockRelease(xid_map_lock);

    if (bcdb_xidmap_debug_enabled() && (lock_wait_us >= 500 || found))
        ereport(LOG,
                (errmsg("[BCDB_XIDMAP] add xid=%u pid=%d tx_ptr=%p txid=%d found=%d spin_wait_us=%lu",
                        (unsigned int)xid, (int)getpid(), (void *)tx, (int)tx->tx_id,
                        found ? 1 : 0, (unsigned long)lock_wait_us)));
}

/*
 * remove_tx_xid_map
 *
 * Removes the xid->tx mapping from xid_map.  Called by delete_tx() so the
 * global XID entry is cleaned up before the BCDBShmXact itself is freed.
 *
 * Acquires the per-tx LWLock (exclusive) while performing the HASH_REMOVE to
 * prevent concurrent readers (e.g. SSI callbacks) from accessing the entry
 * after it has been freed.  Calls ereport(FATAL) if the entry was already
 * absent (indicates a double-free or logic bug).
 *
 * Safe to call with InvalidTransactionId (no-op).
 */
void remove_tx_xid_map(TransactionId xid)
{
    bool found;
    XidMapEntry *entry;
	BCDBShmXact *mapped_tx = NULL;
    uint64 spin_t0 = 0;
    uint64 spin_wait_us = 0;

    if (!TransactionIdIsValid(xid))
        return;

    if (bcdb_xidmap_debug_enabled())
        spin_t0 = bcdb_get_time();

    DEBUGNOCHECK("[ZL] removing xid map: %d", (int)xid);
    SpinLockAcquire(xid_map_lock);
    if (bcdb_xidmap_debug_enabled())
        spin_wait_us = bcdb_get_time() - spin_t0;

    entry = hash_search(xid_map, &xid, HASH_FIND, &found);

    BCDB_XIDMAP_LOG("[BCDB_XIDMAP] remove enter pid=%d xid=%u found=%d entry_ptr=%p spin_wait_us=%lu",
                    (int)getpid(), (unsigned int)xid, found ? 1 : 0, (void *)entry,
                    (unsigned long)spin_wait_us);

    if (entry)
	{
		mapped_tx = entry->tx;
		hash_search(xid_map, &xid, HASH_REMOVE, &found);
		BCDB_XIDMAP_LOG("[BCDB_XIDMAP] remove post_remove pid=%d xid=%u removed=%d",
						(int)getpid(), (unsigned int)xid, found ? 1 : 0);
	}
	SpinLockRelease(xid_map_lock);

	if (mapped_tx != NULL)
    {
        uint64 lw_wait_us = 0;
        uint64 lw_t0 = 0;

        if (bcdb_xidmap_debug_enabled())
            lw_t0 = bcdb_get_time();

        BCDB_XIDMAP_LOG("[BCDB_XIDMAP] remove pre_lwlock pid=%d xid=%u tx_ptr=%p",
						(int)getpid(), (unsigned int)xid, (void *)mapped_tx);
		LWLockAcquire(&mapped_tx->lock, LW_EXCLUSIVE);
        if (bcdb_xidmap_debug_enabled())
            lw_wait_us = bcdb_get_time() - lw_t0;

        if (bcdb_xidmap_debug_enabled() && lw_wait_us >= 1000)
            ereport(LOG,
                    (errmsg("[BCDB_XIDMAP] remove lwlock_wait pid=%d xid=%u tx_ptr=%p wait_us=%lu",
							(int)getpid(), (unsigned int)xid, (void *)mapped_tx,
                            (unsigned long)lw_wait_us)));

		LWLockRelease(&mapped_tx->lock);
    }

    BCDB_XIDMAP_LOG("[BCDB_XIDMAP] remove exit pid=%d xid=%u found=%d",
                    (int)getpid(), (unsigned int)xid, found ? 1 : 0);

    if (!found)
    {
        BCDB_XIDMAP_LOG("[BCDB_XIDMAP] remove missing_entry pid=%d xid=%u",
                        (int)getpid(), (unsigned int)xid);
        ereport(FATAL, (errmsg("[ZL] xid map %d is already deleted!", (int)xid)));
    }
}

/*
 * Return true when xid belongs to an in-flight BCDB transaction that is
 * ordered before activeTx.  Dirty tuple snapshots can surface both directions:
 * a predecessor we must wait for and a successor this transaction must ignore.
 */
static bool
bcdb_dirty_xid_is_ordered_predecessor(TransactionId xid)
{
    XidMapEntry *entry;
    TransactionId topxid;
    BCTxID mapped_txid = -1;

    if (!TransactionIdIsValid(xid) || activeTx == NULL)
        return false;

	/*
	 * SnapshotDirty can report the subtransaction XID that inserted the heap
	 * tuple. apply_optim_insert() uses an internal subtransaction, and
	 * GetCurrentTransactionId() records exactly that observed XID in xid_map.
	 * Try it before falling back to the top transaction ID.
	 *
	 * IMPORTANT: SubTransGetTopmostTransaction() calls SubTransGetParent()
	 * which acquires SubtransControlLock (an LWLock) and may do SLRU I/O.
	 * We must NOT call it while holding xid_map_lock (a spinlock), because
	 * spinlocks must never be held while sleeping.  So we split this into
	 * two spinlock-protected lookups with the subtrans walk in between.
	 */

	/* First attempt: direct XID lookup under the spinlock. */
	SpinLockAcquire(xid_map_lock);
    entry = hash_search(xid_map, &xid, HASH_FIND, NULL);
    if (entry != NULL && entry->tx != NULL)
        mapped_txid = entry->tx->tx_id;
	SpinLockRelease(xid_map_lock);

	/* If the direct lookup missed, resolve to the topmost transaction ID
	 * outside any spinlock, then re-check.  SubTransGetTopmostTransaction
	 * can acquire LWLocks and do SLRU I/O, so it must run lock-free. */
	if (mapped_txid < 0)
    {
        topxid = SubTransGetTopmostTransaction(xid);
        if (topxid != xid)
        {
			SpinLockAcquire(xid_map_lock);
            entry = hash_search(xid_map, &topxid, HASH_FIND, NULL);
            if (entry != NULL && entry->tx != NULL)
                mapped_txid = entry->tx->tx_id;
			SpinLockRelease(xid_map_lock);
        }
    }

    return mapped_txid >= 0 && mapped_txid < activeTx->tx_id;
}

void store_optim_update(TupleTableSlot *slot, ItemPointer old_tid)
{
    OptimWriteEntry *write_entry;
    MemoryContext old_context;
    DEBUGMSG("[ZL] tx %s storing update to (%d %d %d)", activeTx->hash, slot->tts_tableOid, *(int *)&old_tid->ip_blkid, (int)old_tid->ip_posid);
    old_context = MemoryContextSwitchTo(bcdb_tx_context);
    write_entry = palloc(sizeof(OptimWriteEntry));
    write_entry->operation = CMD_UPDATE;
    write_entry->old_tid = *old_tid;
    write_entry->slot = clone_slot(slot);
    write_entry->cid = GetCurrentCommandId(true);
    write_entry->relOid = InvalidOid;
    write_entry->keyval = -1;
    SIMPLEQ_INSERT_TAIL(&activeTx->optim_write_list, write_entry, link);
    MemoryContextSwitchTo(old_context);
#if SAFEDBG1
    printf("safeDB %s : %s: %d tx %d cid %d\n",
           __FILE__, __FUNCTION__, __LINE__, activeTx->tx_id, write_entry->cid);
#endif
    // debugtup(slot, NULL);
}

void store_optim_insert(TupleTableSlot *slot)
{
    OptimWriteEntry *write_entry;
    MemoryContext old_context;
    bool key_is_null = true;
    int32 key_val = 0;

    if (slot != NULL)
    {
        Datum key_datum = slot_getattr(slot, 1, &key_is_null);
        if (!key_is_null)
            key_val = DatumGetInt32(key_datum);
    }

    if (bcdb_flow_debug_enabled())
        ereport(LOG,
                (errmsg("[BCDB_FLOW] store_insert_enter pid=%d txid=%d xid=%u rel=%u key_is_null=%d key=%d",
                        getpid(),
                        activeTx ? (int)activeTx->tx_id : -1,
                        (unsigned int)(activeTx ? activeTx->xid : InvalidTransactionId),
                        slot ? slot->tts_tableOid : InvalidOid,
                        key_is_null ? 1 : 0,
                        key_val)));

    DEBUGMSG("[ZL] tx %s storing insert to (rel: %d)", activeTx->hash, slot->tts_tableOid);
    old_context = MemoryContextSwitchTo(bcdb_tx_context);
    write_entry = palloc(sizeof(OptimWriteEntry));
    write_entry->operation = CMD_INSERT;
    write_entry->slot = clone_slot(slot);
    ItemPointerSetInvalid(&write_entry->old_tid);
    write_entry->cid = GetCurrentCommandId(true);
    write_entry->relOid = InvalidOid;
    write_entry->keyval = -1;
    SIMPLEQ_INSERT_TAIL(&activeTx->optim_write_list, write_entry, link);
    MemoryContextSwitchTo(old_context);

    if (bcdb_flow_debug_enabled())
        ereport(LOG,
                (errmsg("[BCDB_FLOW] store_insert_done pid=%d txid=%d xid=%u rel=%u",
                        getpid(),
                        activeTx ? (int)activeTx->tx_id : -1,
                        (unsigned int)(activeTx ? activeTx->xid : InvalidTransactionId),
                        slot ? slot->tts_tableOid : InvalidOid)));
}

void store_optim_delete(Oid relOid, ItemPointer tupleid, TupleTableSlot *slot)
{
    OptimWriteEntry *write_entry;
    MemoryContext old_context;
    DEBUGMSG("[ZL] tx %s storing delete (rel: %d)", activeTx->hash, relOid);
    old_context = MemoryContextSwitchTo(bcdb_tx_context);
    write_entry = palloc(sizeof(OptimWriteEntry));
    write_entry->operation = CMD_DELETE;
    write_entry->slot = slot ? clone_slot(slot) : NULL;
	write_entry->old_tid = *tupleid;
	write_entry->relOid = relOid;
	write_entry->cid = GetCurrentCommandId(true);
	write_entry->keyval = -1;
	if (slot != NULL && !TTS_EMPTY(slot))
	{
		bool isnull = true;
		Datum k = slot_getattr(slot, 1, &isnull);
		if (!isnull)
			write_entry->keyval = DatumGetInt32(k);
	}
	SIMPLEQ_INSERT_TAIL(&activeTx->optim_write_list, write_entry, link);
	MemoryContextSwitchTo(old_context);
}

void store_optim_delete_by_key(Oid relOid, int32 keyval, CommandId cid)
{
    OptimWriteEntry *write_entry;
    MemoryContext old_context;

    DEBUGMSG("[ZL] tx %s storing deferred delete-by-key (rel: %d key: %d)",
             activeTx->hash, relOid, keyval);

    old_context = MemoryContextSwitchTo(bcdb_tx_context);
    write_entry = palloc(sizeof(OptimWriteEntry));
    write_entry->operation = CMD_DELETE;
    write_entry->slot = NULL;
    ItemPointerSetInvalid(&write_entry->old_tid);
    write_entry->relOid = relOid;
    write_entry->keyval = keyval;
    write_entry->cid = cid;
    SIMPLEQ_INSERT_TAIL(&activeTx->optim_write_list, write_entry, link);
    MemoryContextSwitchTo(old_context);
}

bool apply_optim_insert(TupleTableSlot *slot, CommandId cid)
{
    uint64 apply_insert_start = bcdb_ptrace_timer_start();
    Relation relation = RelationIdGetRelation(slot->tts_tableOid);

	if (!enable_merkle_index && merkle_relation_has_index(relation))
	{
		RelationClose(relation);
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("Merkle maintenance is disabled for relation %u",
						slot->tts_tableOid),
				 errhint("Set enable_merkle_index=on before modifying a Merkle-indexed table.")));
	}

    DEBUGMSG("[ZL] tx %s applying optim insert (rel: %d)", activeTx->hash, relation->rd_id);
    bcdb_ptrace_inc_counter(BCDB_PTRACE_COUNTER_APPLY_INSERT_COUNT, 1);

	/*
	 * Apply the heap + index insert directly inside the enclosing
	 * bcdb_apply_retry subtransaction.  Any duplicate-key or constraint error
	 * is caught by bcdb_apply_optim_writes_with_retry()'s PG_CATCH block,
	 * avoiding inner subtransaction allocation and SubtransControlLock contention.
	 */
	table_tuple_insert(relation, slot, cid, 0, NULL);
	heap_apply_index(relation, slot, true, true);
	RelationClose(relation);

	bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_APPLY_INSERT_US,
						   apply_insert_start);
	return true;
}

static bool
bcdb_lookup_current_tid_from_slot(Relation relation, TupleTableSlot *slot,
								  TupleTableSlot *outSlot, ItemPointer out_tid)
{
	List *btreeIndexList;
	ListCell *blc;
	bool re_found = false;

	if (slot == NULL || TTS_EMPTY(slot))
		return false;

	btreeIndexList = RelationGetIndexList(relation);
	foreach (blc, btreeIndexList)
	{
		Oid btreeOid = lfirst_oid(blc);
		Relation btreeRel = index_open(btreeOid, AccessShareLock);

		if (btreeRel->rd_rel->relam != MERKLE_AM_OID &&
			btreeRel->rd_index->indisunique &&
			btreeRel->rd_index->indnkeyatts >= 1 &&
			btreeRel->rd_index->indnkeyatts <= INDEX_MAX_KEYS)
		{
			int nkeys = btreeRel->rd_index->indnkeyatts;
			ScanKeyData skey[INDEX_MAX_KEYS];
			bool can_use = true;
			int k;

			for (k = 0; k < nkeys; k++)
			{
				int attnum = btreeRel->rd_index->indkey.values[k];
				bool isnull = false;
				Datum val;
				Oid atttypid;
				RegProcedure eqproc;

				if (attnum <= 0)
				{
					can_use = false;
					break;
				}

				val = slot_getattr(slot, attnum, &isnull);
				if (isnull)
				{
					can_use = false;
					break;
				}

				atttypid = TupleDescAttr(RelationGetDescr(btreeRel), k)->atttypid;
				if (atttypid == INT4OID)
					eqproc = F_INT4EQ;
				else if (atttypid == INT8OID)
					eqproc = F_INT8EQ;
				else if (atttypid == INT2OID)
					eqproc = F_INT2EQ;
				else
				{
					can_use = false;
					break;
				}

				ScanKeyInit(&skey[k], k + 1, BTEqualStrategyNumber, eqproc, val);
			}

			if (can_use)
			{
				IndexScanDesc iscan;

				iscan = index_beginscan(relation, btreeRel, SnapshotSelf, nkeys, 0);
				index_rescan(iscan, skey, nkeys, NULL, 0);
				if (index_getnext_slot(iscan, ForwardScanDirection, outSlot))
				{
					*out_tid = outSlot->tts_tid;
					re_found = true;
				}
				index_endscan(iscan);
			}
		}

		index_close(btreeRel, AccessShareLock);
		if (re_found)
			break;
	}
	list_free(btreeIndexList);

	return re_found;
}

bool apply_optim_update(ItemPointer tid, TupleTableSlot *slot, CommandId cid)
{
    uint64 apply_update_start = bcdb_ptrace_timer_start();
    uint64 merkle_prep_start = 0;
    TM_FailureData tmfd;
    TM_Result result;
    LockTupleMode lockmode;
    bool update_indexes;
    Relation relation = RelationIdGetRelation(slot->tts_tableOid);
    List *indexList = NIL;
    ListCell *lc;
    TupleTableSlot *oldSlot = NULL;
    TupleTableSlot *newSlot = NULL;
	TupleTableSlot *actual_slot = slot;
	TupleTableSlot *temp_slot = NULL;
    MerkleHash oldHash;
    MerkleHash newHash;
    bool hasOldHash = false;
    bool hasNewHash = false;
    int pendingCount = 0;
    int pendingCapacity = 0;
	typedef struct PendingMerkleUpdate
	{
		Oid indexOid;
		uint8 old_key_hash[8];
	} PendingMerkleUpdate;
    PendingMerkleUpdate *pending = NULL;

	if (!enable_merkle_index && merkle_relation_has_index(relation))
	{
		RelationClose(relation);
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("Merkle maintenance is disabled for relation %u",
						slot->tts_tableOid),
				 errhint("Set enable_merkle_index=on before modifying a Merkle-indexed table.")));
	}

    DEBUGMSG("[ZL] tx %s applying optim update (%d %d %d)", activeTx->hash, relation->rd_id, *(int *)&tid->ip_blkid, (int)tid->ip_posid);
    bcdb_ptrace_inc_counter(BCDB_PTRACE_COUNTER_APPLY_UPDATE_COUNT, 1);
#if SAFEDBG1
    printf("safeDB %s : %s: %d tm-ok %d tx %d cid %d\n",
           __FILE__, __FUNCTION__, __LINE__, TM_Ok, activeTx->tx_id, cid);
#endif

	if (ItemPointerIsValid(tid) &&
		ItemPointerGetBlockNumberNoCheck(tid) != InvalidBlockNumber)
	{
		oldSlot = table_slot_create(relation, NULL);
        /*
         * Strict serial semantics: hash/leaf MUST match what we will remove
         * from the Merkle tree. Use SnapshotSelf so we only act on tuples
         * that are part of our committed serial view (and avoid hashing a
         * dead/non-visible tuple version).
         */
        if (!table_tuple_fetch_row_version(relation, tid, SnapshotSelf, oldSlot))
        {
			/*
			 * The row was moved to a new TID by an earlier committed update.
			 * Look up the current TID using the unique primary key index matching all key attributes.
			 */
			bool re_found = bcdb_lookup_current_tid_from_slot(relation, slot, oldSlot, tid);
			if (!re_found)
			{
				ExecDropSingleTupleTableSlot(oldSlot);
				RelationClose(relation);
				if (merkle_prep_start != 0)
					bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_APPLY_MERKLE_PREP_US,
										   merkle_prep_start);
				bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_APPLY_UPDATE_US,
									   apply_update_start);
				return true; /* 0 rows updated: successful no-op */
			}

			/*
			 * Monotonic preservation for district table:
			 * When a concurrent update (e.g., payment_proc) moves the district row,
			 * its slot may hold a stale d_next_o_id. Ensure d_next_o_id (attr 5,
			 * 0-indexed column 4) is never regressed by replacing it with old_val
			 * from the current committed tuple (oldSlot).
			 */
			if (strcmp(RelationGetRelationName(relation), "district") == 0)
			{
				bool old_null = false, cur_null = false;
				Datum old_next = slot_getattr(oldSlot, 5, &old_null);
				Datum cur_next = slot_getattr(slot, 5, &cur_null);
				if (!old_null && !cur_null)
				{
					int32 old_val = DatumGetInt32(old_next);
					int32 cur_val = DatumGetInt32(cur_next);
					if (old_val > cur_val)
					{
						TupleDesc td = RelationGetDescr(relation);
						Datum *repl_values = (Datum *) palloc0(sizeof(Datum) * td->natts);
						bool *repl_nulls = (bool *) palloc0(sizeof(bool) * td->natts);
						bool *repl_do = (bool *) palloc0(sizeof(bool) * td->natts);
						HeapTuple oldTup;
						HeapTuple newTup;

						repl_values[4] = Int32GetDatum(old_val);
						repl_do[4] = true;
						oldTup = ExecFetchSlotHeapTuple(slot, false, NULL);
						newTup = heap_modify_tuple(oldTup, td, repl_values, repl_nulls, repl_do);
						pfree(repl_values);
						pfree(repl_nulls);
						pfree(repl_do);

						temp_slot = MakeSingleTupleTableSlot(td, &TTSOpsHeapTuple);
						ExecStoreHeapTuple(newTup, temp_slot, true);
						actual_slot = temp_slot;
					}
				}
			}
		}

		if (enable_merkle_index)
		{
			merkle_prep_start = bcdb_ptrace_timer_start();
			/* Compute old hash from the same oldSlot image used for leafing */
			merkle_compute_slot_hash(relation, oldSlot, &oldHash);

			hasOldHash = !merkle_hash_is_zero(&oldHash);
			if (hasOldHash)
			{
				indexList = RelationGetIndexList(relation);
				pendingCapacity = list_length(indexList);
				if (pendingCapacity > 0)
				{
					/*
					 * Allocate in a short-lived child context so that this array
					 * is not retained for the lifetime of the transaction.
					 * Using TopTransactionContext directly would cause every
					 * apply_optim_update() call to accumulate a small array until
					 * commit/abort, which is wasteful in long multi-statement
					 * transactions.  We create a dedicated context under the
					 * current context (which is safe here since we are inside the
					 * Merkle prep block, before the heap update), and delete it
					 * explicitly on every exit path.
					 */
					pending = (PendingMerkleUpdate *) MemoryContextAllocZero(
						TopTransactionContext,
						sizeof(PendingMerkleUpdate) * pendingCapacity);
				}

				foreach (lc, indexList)
				{
					Oid indexOid = lfirst_oid(lc);
					Relation indexRel = index_open(indexOid, RowExclusiveLock);

					if (indexRel->rd_rel->relam == MERKLE_AM_OID)
					{
						IndexInfo *indexInfo;
						Datum values[INDEX_MAX_KEYS];
						bool isnull[INDEX_MAX_KEYS];
						MerkleRoute route;

						indexInfo = RelationGetIndexInfo(indexRel);
						FormIndexDatum(indexInfo, oldSlot, NULL, values, isnull);
						merkle_compute_route(indexRel, values, isnull,
											 indexInfo->ii_NumIndexKeyAttrs, &route);

						pending[pendingCount].indexOid = indexOid;
						memcpy(pending[pendingCount].old_key_hash, route.route_digest, 8);
						pendingCount++;
					}

					index_close(indexRel, RowExclusiveLock);
				}
			}
			bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_APPLY_MERKLE_PREP_US,
								   merkle_prep_start);
		}
    }

    /*
     * HANG DEBUG: time the table_tuple_update call so we can see when
     * wait=true actually blocks on a hot row (district, customer, stock).
     * Fires unconditionally when wait > 1 ms so apply serialization on hot
     * rows is immediately visible in server.log.  At 8 threads the plan
     * showed apply p90=18 ms; this log makes each incident traceable.
     */
    {
        uint64 apply_update_elapsed = 0;

		result = bcdb_table_tuple_update_step1(relation, tid, actual_slot,
											   cid,
											   &tmfd,
											   &lockmode,
											   &update_indexes,
											   &apply_update_elapsed);

		slot->tts_tid = actual_slot->tts_tid;

        if (bcdb_apply_wait_debug_enabled() &&
            apply_update_elapsed >= 1000) /* log if blocked > 1 ms */
            ereport(LOG,
                    (errmsg("[BCDB_HANG] apply_update_wait pid=%d txid=%d rel=%u waited_us=%lu result=%d",
                            (int)getpid(),
                            activeTx ? (int)activeTx->tx_id : -1,
                            (unsigned int)(relation ? relation->rd_id : 0),
                            (unsigned long)apply_update_elapsed,
                            (int)result)));
    }

    if (result != TM_Ok)
    {
		if (temp_slot)
			ExecDropSingleTupleTableSlot(temp_slot);
        if (oldSlot)
            ExecDropSingleTupleTableSlot(oldSlot);
        if (newSlot)
			ExecDropSingleTupleTableSlot(newSlot);
		if (indexList)
			list_free(indexList);
		if (pending != NULL)
		{
			pfree(pending);
			pending = NULL;
		}
		RelationClose(relation);
		bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_APPLY_UPDATE_US,
							   apply_update_start);
#if SAFEDBG1
		printf("safeDB %s : %s: %d ret %d tx %d tx %s doomed because of ww-conflict \n",
			   __FILE__, __FUNCTION__, __LINE__, result, activeTx->tx_id, activeTx->hash);
		printf("safeDB %s : %s: %d   tmfd.xmax %d, tmfd.cmax %d  ww-conflict \n",
			   __FILE__, __FUNCTION__, __LINE__, tmfd.xmax, tmfd.cmax);
#endif
		return false;
	}

	if (update_indexes)
		heap_apply_index_phase(relation, actual_slot, false, false, HEAP_INDEX_NO_MERKLE);

	/*
	 * Merkle UPDATE maintenance:
	 * Always apply Merkle delta, even for HOT (heap-only) updates where
	 * update_indexes is false. Merkle indexes hash full-row contents, so any
	 * UPDATE that changes data must be reflected in the tree.
	 *
	 * Important: leafing (key→leaf) may change for multi-key Merkle indexes
	 * (e.g. (ycsb_key, field1)). We therefore compute old and new leaf IDs
	 * from the OLD and NEW heap tuple images, not from the executor slot.
	 */
	if (enable_merkle_index && hasOldHash && ItemPointerIsValid(&slot->tts_tid) &&
		ItemPointerGetBlockNumberNoCheck(&slot->tts_tid) != InvalidBlockNumber)
	{
		uint64 merkle_update_start = bcdb_ptrace_timer_start();
		/*
		 * Fetch NEW row image from heap and hash from that image so that
		 * merkle_verify (which hashes heap tuples) matches exactly.
		 */
		newSlot = table_slot_create(relation, NULL);
		if (!table_tuple_fetch_row_version(relation, &slot->tts_tid, SnapshotSelf, newSlot))
		{
			if (temp_slot)
				ExecDropSingleTupleTableSlot(temp_slot);
			if (oldSlot)
				ExecDropSingleTupleTableSlot(oldSlot);
			if (newSlot)
				ExecDropSingleTupleTableSlot(newSlot);
			if (indexList)
				list_free(indexList);
			if (pending != NULL)
			{
				pfree(pending);
				pending = NULL;
			}
			RelationClose(relation);
			bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_APPLY_MERKLE_UPDATE_US,
								   merkle_update_start);
			bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_APPLY_UPDATE_US,
                                   apply_update_start);
            return false;
        }

        merkle_compute_slot_hash(relation, newSlot, &newHash);
        hasNewHash = !merkle_hash_is_zero(&newHash);

		if (hasNewHash)
		{
            int i;

            for (i = 0; i < pendingCount; i++)
            {
                Relation indexRel = index_open(pending[i].indexOid, RowExclusiveLock);

                if (indexRel->rd_rel->relam == MERKLE_AM_OID)
                {
                    IndexInfo *indexInfo;
                    Datum values[INDEX_MAX_KEYS];
                    bool isnull[INDEX_MAX_KEYS];
					MerkleRoute route;

					indexInfo = RelationGetIndexInfo(indexRel);
                    FormIndexDatum(indexInfo, newSlot, NULL, values, isnull);
					merkle_compute_route(indexRel, values, isnull,
										 indexInfo->ii_NumIndexKeyAttrs, &route);

					uint8 new_key_hash[8] = {0};
					memcpy(new_key_hash, route.route_digest, 8);

					if (memcmp(pending[i].old_key_hash, new_key_hash, 8) == 0)
					{
						MerkleHash delta = oldHash;
						merkle_hash_xor(&delta, &newHash);
						merkle_stage_delta_event(indexRel, MERKLE_DELTA_UPDATE_SAME_LEAF,
												 pending[i].old_key_hash, new_key_hash, &delta);
						bcdb_ptrace_inc_counter(BCDB_PTRACE_COUNTER_MERKLE_UPDATE_COUNT, 1);
					}
					else
					{
						merkle_stage_delta_event(indexRel, MERKLE_DELTA_DELETE,
												 pending[i].old_key_hash, NULL, &oldHash);
						merkle_stage_delta_event(indexRel, MERKLE_DELTA_INSERT,
												 NULL, new_key_hash, &newHash);
						bcdb_ptrace_inc_counter(BCDB_PTRACE_COUNTER_MERKLE_UPDATE_COUNT, 2);
					}
                }

                index_close(indexRel, RowExclusiveLock);
            }
        }
        bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_APPLY_MERKLE_UPDATE_US,
                               merkle_update_start);
    }

	if (temp_slot)
		ExecDropSingleTupleTableSlot(temp_slot);
	if (oldSlot)
		ExecDropSingleTupleTableSlot(oldSlot);
	if (newSlot)
		ExecDropSingleTupleTableSlot(newSlot);
	if (indexList)
		list_free(indexList);
	if (pending != NULL)
	{
		pfree(pending);
		pending = NULL;
	}

	RelationClose(relation);
	bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_APPLY_UPDATE_US,
						   apply_update_start);
	return true;
}

bool apply_optim_delete(Oid relOid, ItemPointer tupleid, TupleTableSlot *storedSlot, CommandId cid, int32 keyval)
{
    uint64 apply_delete_start = bcdb_ptrace_timer_start();
    uint64 merkle_prep_start = 0;
    Relation relation = RelationIdGetRelation(relOid);
    TM_FailureData tmfd;
    TM_Result result;
    TupleTableSlot *oldSlot = NULL;
    List *indexList = NIL;
    ListCell *lc;
    MerkleHash oldHash;
	bool hasOldHash = false;
	int pendingCount = 0;
	int pendingCapacity = 0;
	typedef struct PendingMerkleDelete
	{
		Oid indexOid;
		uint8 old_key_hash[8];
	} PendingMerkleDelete;
	PendingMerkleDelete *pending = NULL;
	ItemPointerData currentTid;
	bool oldSlotOwned = false;

	if (!enable_merkle_index && merkle_relation_has_index(relation))
	{
		RelationClose(relation);
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("Merkle maintenance is disabled for relation %u", relOid),
				 errhint("Set enable_merkle_index=on before modifying a Merkle-indexed table.")));
	}

	DEBUGMSG("[ZL] tx %s applying optim delete (rel: %d)", activeTx->hash, relOid);
	bcdb_ptrace_inc_counter(BCDB_PTRACE_COUNTER_APPLY_DELETE_COUNT, 1);

    if (!enable_merkle_index)
    {
        result = bcdb_table_tuple_delete_step1(relation,
                                               tupleid,
                                               cid,
                                               &tmfd,
                                               false,
                                               NULL);

        if (result != TM_Ok)
        {
            RelationClose(relation);
            bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_APPLY_DELETE_US,
                                   apply_delete_start);
            return false;
        }

        RelationClose(relation);
        bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_APPLY_DELETE_US,
                               apply_delete_start);
        return true;
    }

    ItemPointerCopy(tupleid, &currentTid);
    merkle_prep_start = bcdb_ptrace_timer_start();

    if (storedSlot != NULL && !TTS_EMPTY(storedSlot))
    {
        oldSlot = storedSlot;
    }
    else
    {
        oldSlot = table_slot_create(relation, NULL);
        oldSlotOwned = true;

        /*
         * Strict serial semantics: apply DELETE against the original
         * optimistic tuple TID only. If we must fetch at apply time,
         * use SnapshotSelf (not SnapshotAny) to avoid hashing a dead
         * tuple version from another transaction.
         */
        if (!table_tuple_fetch_row_version(relation, &currentTid, SnapshotSelf, oldSlot))
        {
			int32 key_to_lookup = keyval;
			bool re_found = false;
			if (storedSlot != NULL && !TTS_EMPTY(storedSlot))
			{
				re_found = bcdb_lookup_current_tid_from_slot(relation, storedSlot, oldSlot, &currentTid);
			}
			else if (key_to_lookup != -1)
			{
				List *btreeIndexList = RelationGetIndexList(relation);
				ListCell *blc;
				foreach (blc, btreeIndexList)
				{
					Oid btreeOid = lfirst_oid(blc);
					Relation btreeRel = index_open(btreeOid, AccessShareLock);
					if (btreeRel->rd_rel->relam != MERKLE_AM_OID &&
						btreeRel->rd_index->indisunique &&
						btreeRel->rd_index->indnkeyatts == 1)
					{
						IndexScanDesc iscan;
						ScanKeyData skey[1];
						ScanKeyInit(&skey[0], 1, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(key_to_lookup));
						iscan = index_beginscan(relation, btreeRel, SnapshotSelf, 1, 0);
						index_rescan(iscan, skey, 1, NULL, 0);
						if (index_getnext_slot(iscan, ForwardScanDirection, oldSlot))
						{
							currentTid = oldSlot->tts_tid;
							re_found = true;
						}
						index_endscan(iscan);
						index_close(btreeRel, AccessShareLock);
						if (re_found) break;
					}
					else
					{
						index_close(btreeRel, AccessShareLock);
					}
				}
				list_free(btreeIndexList);
			}
			if (!re_found)
			{
				if (oldSlotOwned && oldSlot)
					ExecDropSingleTupleTableSlot(oldSlot);
				RelationClose(relation);
				bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_APPLY_MERKLE_PREP_US,
									   merkle_prep_start);
				bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_APPLY_DELETE_US,
									   apply_delete_start);
				return true; /* 0 rows deleted: successful no-op */
			}
		}
	}

    /* CRITICAL FIX: Use merkle_compute_row_hash instead of merkle_compute_slot_hash */
    merkle_compute_row_hash(relation, &currentTid, &oldHash);
    hasOldHash = !merkle_hash_is_zero(&oldHash);

	if (hasOldHash)
	{
		indexList = RelationGetIndexList(relation);
		pendingCapacity = list_length(indexList);
		if (pendingCapacity > 0)
		{
			/*
			 * Allocate in a short-lived child context so that this array is
			 * freed as soon as apply_optim_delete() returns, rather than
			 * accumulating in TopTransactionContext for the duration of the
			 * transaction.
			 */
			pending = (PendingMerkleDelete *) MemoryContextAllocZero(
				TopTransactionContext,
				sizeof(PendingMerkleDelete) * pendingCapacity);
		}

		foreach (lc, indexList)
		{
			Oid indexOid = lfirst_oid(lc);
			Relation indexRel = index_open(indexOid, RowExclusiveLock);

            if (indexRel->rd_rel->relam == MERKLE_AM_OID)
            {
                IndexInfo *indexInfo;
                Datum values[INDEX_MAX_KEYS];
                bool isnull[INDEX_MAX_KEYS];
					MerkleRoute route;

				indexInfo = RelationGetIndexInfo(indexRel);
                FormIndexDatum(indexInfo, oldSlot, NULL, values, isnull);
				merkle_compute_route(indexRel, values, isnull,
									 indexInfo->ii_NumIndexKeyAttrs, &route);

                pending[pendingCount].indexOid = indexOid;
				memcpy(pending[pendingCount].old_key_hash, route.route_digest, 8);

                pendingCount++;
            }

            index_close(indexRel, RowExclusiveLock);
        }
    }
    bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_APPLY_MERKLE_PREP_US,
                           merkle_prep_start);

    /* Now delete the heap tuple using the (possibly updated) currentTid */
    result = bcdb_table_tuple_delete_step1(relation,
                                           &currentTid,
                                           cid,
										   &tmfd,
										   false,
										   NULL);

	if (result != TM_Ok)
	{
		if (oldSlotOwned && oldSlot)
			ExecDropSingleTupleTableSlot(oldSlot);
		if (indexList)
			list_free(indexList);
		if (pending != NULL)
		{
			pfree(pending);
			pending = NULL;
		}
		RelationClose(relation);
		bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_APPLY_DELETE_US,
							   apply_delete_start);
		return false;
	}

    if (hasOldHash)
    {
        uint64 merkle_update_start = bcdb_ptrace_timer_start();
        int i;
        for (i = 0; i < pendingCount; i++)
        {
            Relation indexRel = index_open(pending[i].indexOid, RowExclusiveLock);
            if (indexRel->rd_rel->relam == MERKLE_AM_OID)
            {
				merkle_stage_delta_event(indexRel, MERKLE_DELTA_DELETE, pending[i].old_key_hash, NULL, &oldHash);
                bcdb_ptrace_inc_counter(BCDB_PTRACE_COUNTER_MERKLE_UPDATE_COUNT, 1);
            }
            index_close(indexRel, RowExclusiveLock);
        }
        bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_APPLY_MERKLE_UPDATE_US,
                               merkle_update_start);
    }

	if (oldSlotOwned && oldSlot)
		ExecDropSingleTupleTableSlot(oldSlot);
	if (indexList)
		list_free(indexList);
	if (pending != NULL)
	{
		pfree(pending);
		pending = NULL;
	}

	RelationClose(relation);
	bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_APPLY_DELETE_US,
						   apply_delete_start);
	return true;
}

/*
 * apply_deferred_delete_by_key — handle DELETE-0 (concurrent visibility race).
 *
 * Called when a DELETE found 0 rows during the optimistic phase because the
 * target row was inserted by a concurrent transaction that had not yet
 * committed.  Lever-D can let the serial apply gate run before the matching
 * earlier INSERT finishes its PostgreSQL commit, so lookup must wait on dirty
 * predecessor rows before it decides that the DELETE is genuinely a no-op.
 *
 * This duplicates the core logic of apply_optim_delete but starts from a
 * primary-key value instead of a stored TupleTableSlot.
 */
bool apply_deferred_delete_by_key(Oid relOid, int keyval)
{
    uint64 apply_delete_start = bcdb_ptrace_timer_start();
    uint64 delete_lookup_start = bcdb_ptrace_timer_start();
    uint64 merkle_prep_start = 0;
    Relation relation = RelationIdGetRelation(relOid);
    TupleTableSlot *oldSlot = NULL;
    ItemPointerData currentTid;
    TM_FailureData tmfd;
    TM_Result result;
    MerkleHash oldHash;
    bool hasOldHash = false;
    bool found = false;
    List *indexList = NIL;
    ListCell *lc;
    int pendingCount = 0;
    int pendingCapacity = 0;
	typedef struct PendingMerkleDelete
	{
		Oid indexOid;
		uint8 old_key_hash[8];
	} PendingMerkleDelete;
    PendingMerkleDelete *pending = NULL;

    bcdb_ptrace_inc_counter(BCDB_PTRACE_COUNTER_APPLY_DELETE_COUNT, 1);
    oldSlot = table_slot_create(relation, NULL);
    BCDB_FLOW_LOG("[BCDB_FLOW] deferred_delete_lookup_enter pid=%d txid=%d xid=%u rel=%u key=%d",
                  getpid(),
                  activeTx ? (int)activeTx->tx_id : -1,
                  (unsigned int)(activeTx ? activeTx->xid : InvalidTransactionId),
                  (unsigned int)relOid,
                  keyval);

    /*
     * Btree lookup by primary key.
     *
     * A SnapshotSelf lookup alone can miss an earlier deterministic INSERT
     * that has published its BCDB slot but is still finishing its PostgreSQL
     * transaction.  SnapshotDirty lets us find that tuple by the unique index,
     * wait for the in-flight creator/deleter, and then retry with a stable row
     * image before hashing and deleting it.
     */
    {
        List *btreeIndexList = RelationGetIndexList(relation);
        bool retry_lookup;

        do
        {
            ListCell *blc;
            TransactionId wait_xid = InvalidTransactionId;

            found = false;
            retry_lookup = false;
            ExecClearTuple(oldSlot);

            foreach (blc, btreeIndexList)
            {
                Oid btreeOid = lfirst_oid(blc);
                Relation btreeRel = index_open(btreeOid, AccessShareLock);

                if (btreeRel->rd_rel->relam != MERKLE_AM_OID &&
                    btreeRel->rd_index->indisunique &&
					btreeRel->rd_index->indnkeyatts == 1)
                {
                    IndexScanDesc iscan;
                    ScanKeyData skey[1];
                    SnapshotData dirty_snapshot;

                    ScanKeyInit(&skey[0],
                                1,
                                BTEqualStrategyNumber,
                                F_INT4EQ,
                                Int32GetDatum(keyval));

                    InitDirtySnapshot(dirty_snapshot);
                    iscan = index_beginscan(relation, btreeRel,
                                            &dirty_snapshot, 1, 0);
                    index_rescan(iscan, skey, 1, NULL, 0);

                    while (index_getnext_slot(iscan, ForwardScanDirection, oldSlot))
                    {
                        TransactionId candidate_wait_xid = InvalidTransactionId;
                        bool candidate_is_predecessor = false;

                        ItemPointerCopy(&oldSlot->tts_tid, &currentTid);

                        if (TransactionIdIsValid(dirty_snapshot.xmin))
                            candidate_wait_xid = dirty_snapshot.xmin;
                        else if (TransactionIdIsValid(dirty_snapshot.xmax))
                            candidate_wait_xid = dirty_snapshot.xmax;

						if (TransactionIdIsValid(candidate_wait_xid))
						{
							candidate_is_predecessor =
								bcdb_dirty_xid_is_ordered_predecessor(candidate_wait_xid);
							BCDB_FLOW_LOG("[BCDB_FLOW] deferred_delete_dirty_candidate pid=%d txid=%d rel=%u key=%d tid_block=%u tid_off=%u xmin=%u xmax=%u wait_xid=%u predecessor=%d",
										  MyProcPid,
										  activeTx ? (int)activeTx->tx_id : -1,
										  (unsigned int)relOid,
										  keyval,
										  ItemPointerGetBlockNumberNoCheck(&currentTid),
										  ItemPointerGetOffsetNumberNoCheck(&currentTid),
										  (unsigned int)dirty_snapshot.xmin,
										  (unsigned int)dirty_snapshot.xmax,
										  (unsigned int)candidate_wait_xid,
										  candidate_is_predecessor ? 1 : 0);
							if (candidate_is_predecessor)
							{
								wait_xid = candidate_wait_xid;
								found = true;
								break;
							}

							/*
							 * A later deterministic INSERT can sort before
							 * the row this DELETE needs in the dirty btree
							 * scan. Skip it and keep looking for an older
							 * committed/predecessor row before preserving
							 * the original DELETE-0 result.
							 */
							ExecClearTuple(oldSlot);
							continue;
						}

						BCDB_FLOW_LOG("[BCDB_FLOW] deferred_delete_stable_candidate pid=%d txid=%d rel=%u key=%d tid_block=%u tid_off=%u",
									  MyProcPid,
									  activeTx ? (int)activeTx->tx_id : -1,
									  (unsigned int)relOid,
									  keyval,
									  ItemPointerGetBlockNumberNoCheck(&currentTid),
									  ItemPointerGetOffsetNumberNoCheck(&currentTid));
						found = true;
						break;
					}

					index_endscan(iscan);
				}

				index_close(btreeRel, AccessShareLock);
				if (found)
					break;
			}

			if (TransactionIdIsValid(wait_xid) &&
				bcdb_dirty_xid_is_ordered_predecessor(wait_xid))
			{
				BCDB_FLOW_LOG("[BCDB_FLOW] deferred_delete_wait_predecessor pid=%d txid=%d rel=%u key=%d wait_xid=%u tid_block=%u tid_off=%u",
							  MyProcPid,
							  activeTx ? (int)activeTx->tx_id : -1,
							  (unsigned int)relOid,
							  keyval,
							  (unsigned int)wait_xid,
							  ItemPointerGetBlockNumberNoCheck(&currentTid),
							  ItemPointerGetOffsetNumberNoCheck(&currentTid));
				XactLockTableWait(wait_xid, relation, &currentTid, XLTW_FetchUpdated);
				retry_lookup = true;
			}
			else if (!found &&
					 activeTx != NULL &&
					 activeTx->tx_id > 0 &&
					 bcdb_serial_gate_source != BCDB_GATE_SRC_LAST_COMMITTED &&
					 get_last_committed_txid(activeTx) < (activeTx->tx_id - 1))
			{
				BCDB_FLOW_LOG("[BCDB_FLOW] deferred_delete_wait_prev_commit pid=%d txid=%d rel=%u key=%d last_committed=%d",
							  MyProcPid,
							  (int)activeTx->tx_id,
							  (unsigned int)relOid,
							  keyval,
							  (int)get_last_committed_txid(activeTx));
				bcdb_wait_for_prev_committed(activeTx);
				retry_lookup = true;
			}
		} while (retry_lookup);

        list_free(btreeIndexList);
    }
    bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_APPLY_DELETE_LOOKUP_US,
                           delete_lookup_start);

    if (!found)
    {
        BCDB_FLOW_LOG("[BCDB_FLOW] deferred_delete_lookup_miss pid=%d txid=%d xid=%u rel=%u key=%d",
                      getpid(),
                      activeTx ? (int)activeTx->tx_id : -1,
                      (unsigned int)(activeTx ? activeTx->xid : InvalidTransactionId),
                      (unsigned int)relOid,
                      keyval);
        /*
         * Row genuinely doesn't exist (e.g., first DELETE of this key
         * that already committed and the INSERT hasn't come yet).
         * Nothing to XOR-out, nothing to delete. This is normal.
         */
        if (oldSlot)
            ExecDropSingleTupleTableSlot(oldSlot);
        RelationClose(relation);
        bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_APPLY_DELETE_US,
                               apply_delete_start);
        return true;
    }

    /* Use merkle_compute_row_hash to get the old hash before deletion */
    merkle_prep_start = bcdb_ptrace_timer_start();
    merkle_compute_row_hash(relation, &currentTid, &oldHash);
    hasOldHash = !merkle_hash_is_zero(&oldHash);

    /* XOR-out from all Merkle indexes */
    if (hasOldHash)
    {
        indexList = RelationGetIndexList(relation);
        pendingCapacity = list_length(indexList);
        if (pendingCapacity > 0)
            pending = palloc0(sizeof(PendingMerkleDelete) * pendingCapacity);

        foreach (lc, indexList)
        {
            Oid indexOid = lfirst_oid(lc);
            Relation indexRel = index_open(indexOid, RowExclusiveLock);

            if (indexRel->rd_rel->relam == MERKLE_AM_OID)
            {
                IndexInfo *indexInfo;
                Datum values[INDEX_MAX_KEYS];
                bool isnull[INDEX_MAX_KEYS];
				MerkleRoute route;

				indexInfo = RelationGetIndexInfo(indexRel);
                FormIndexDatum(indexInfo, oldSlot, NULL, values, isnull);
				merkle_compute_route(indexRel, values, isnull,
									 indexInfo->ii_NumIndexKeyAttrs, &route);

                pending[pendingCount].indexOid = indexOid;
				memcpy(pending[pendingCount].old_key_hash, route.route_digest, 8);

                pendingCount++;
            }

            index_close(indexRel, RowExclusiveLock);
        }
    }
    bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_APPLY_MERKLE_PREP_US,
                           merkle_prep_start);

    /* Heap delete */
    result = bcdb_table_tuple_delete_step1(relation,
                                           &currentTid,
                                           GetCurrentCommandId(true),
                                           &tmfd,
                                           false,
                                           NULL);
    BCDB_FLOW_LOG("[BCDB_FLOW] deferred_delete_heap_result pid=%d txid=%d xid=%u rel=%u key=%d tid_block=%u tid_off=%u result=%d",
                  getpid(),
                  activeTx ? (int)activeTx->tx_id : -1,
                  (unsigned int)(activeTx ? activeTx->xid : InvalidTransactionId),
                  (unsigned int)relOid,
                  keyval,
                  ItemPointerGetBlockNumberNoCheck(&currentTid),
                  ItemPointerGetOffsetNumberNoCheck(&currentTid),
                  (int)result);

    /* Apply Merkle XOR-outs after successful heap delete */
    if (result == TM_Ok && hasOldHash)
    {
        uint64 merkle_update_start = bcdb_ptrace_timer_start();
        for (int i = 0; i < pendingCount; i++)
        {
            Relation indexRel = index_open(pending[i].indexOid, RowExclusiveLock);
            if (indexRel->rd_rel->relam == MERKLE_AM_OID)
            {
				merkle_stage_delta_event(indexRel, MERKLE_DELTA_DELETE, pending[i].old_key_hash, NULL, &oldHash);
                bcdb_ptrace_inc_counter(BCDB_PTRACE_COUNTER_MERKLE_UPDATE_COUNT, 1);
            }
            index_close(indexRel, RowExclusiveLock);
        }
        bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_APPLY_MERKLE_UPDATE_US,
                               merkle_update_start);
    }

    if (oldSlot)
        ExecDropSingleTupleTableSlot(oldSlot);
    if (indexList)
        list_free(indexList);
    if (pending)
        pfree(pending);

    RelationClose(relation);
    bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_APPLY_DELETE_US,
                           apply_delete_start);
    return (result == TM_Ok);
}

bool apply_optim_writes(void)
{
    /*
     * Non-destructive traversal: caller owns queue cleanup.
     * Lever D v2 retries apply in a subtransaction, so entries/slots must
     * remain intact across attempts until the worker decides final cleanup.
     */
    OptimWriteEntry *write_entry;

    SIMPLEQ_FOREACH(write_entry, &activeTx->optim_write_list, link)
    {
        switch (write_entry->operation)
        {
        case CMD_UPDATE:
            if (!apply_optim_update(&write_entry->old_tid, write_entry->slot, write_entry->cid))
                return false;
            break;
        case CMD_INSERT:
            if (!apply_optim_insert(write_entry->slot, write_entry->cid))
            {
                /*
                 * INSERT failed (duplicate key).  This happens when
                 * tx_id assignment doesn't preserve workload line order:
                 * a DELETE-INSERT pair for the same key gets swapped so
                 * the INSERT runs first and finds the original row still
                 * present.  Signal failure so the worker retries the
                 * apply stage in Lever D v2.
                 */
                return false;
            }
            break;
        case CMD_DELETE:
            if (ItemPointerIsValid(&write_entry->old_tid))
            {
				if (!apply_optim_delete(write_entry->relOid, &write_entry->old_tid,
										write_entry->slot, write_entry->cid, write_entry->keyval))
                    return false;
            }
            else
            {
                if (!apply_deferred_delete_by_key(write_entry->relOid, write_entry->keyval))
                    return false;
            }
            break;
        default:
            ereport(ERROR, (errmsg("[ZL] tx %s applying unknown operation", activeTx->hash)));
        }
    }
    return true;
}

/*
 * check_stale_read has been removed.
 *
 * It walked activeTx->sxact->outConflicts (SSI rw-conflict list) and raised
 * a serialization failure if any conflicting transaction committed in an
 * earlier blockchain block.  It was never called from any code path (it was
 * intended as an optional SSI-layer guard but was superseded by the DT
 * conflict detection).  If SSI-based stale-read detection is needed in
 * future, re-introduce it here and wire it into the worker commit path.
 */

/*
 * conflict_checkDT
 *
 * Deterministic Execution (DT) path per-transaction conflict check.  Called
 * by the worker immediately after establishing the transaction's serial
 * ordering slot, before apply_optim_writes().
 *
 * Walk both ws_table_record and rs_table_record (the local write-set and
 * read-set lists built during optimistic execution) and call
 * ws_table_checkDT() for each tag.  Returns 1 if any conflict is found
 * (caller will retry the transaction), 0 if clean.
 *
 * The Deterministic Execution (DT) path design (map and mapB) means the
 * check sees BOTH the "old" shard and the "new" shard for the current
 * epoch, so no conflicts are missed across a hash-table rotation boundary.
 */
int conflict_checkDT()
{
    uint64 ws_check_start;
    uint64 rs_check_start;
    WSTableEntryRecord *record;

    if (!bcdb_dt_conflict_tracking)
        return 0;

    bcdb_reset_last_conflict_txid();

#if SAFEDBG2
    const int ccMax = 1;
    static int ccCount = 0;
    static int cc2Count = 0;

    if (ccCount++ == ccMax)
    {
        ccCount = 0;
        printf("safeDB %s : %s: %d -- one in 20\n", __FILE__, __FUNCTION__, __LINE__);
    }
#endif

    ws_check_start = bcdb_ptrace_timer_start();
    LIST_FOREACH(record, &ws_table_record, link)
    {
        bcdb_ptrace_inc_counter(BCDB_PTRACE_COUNTER_WS_CONFLICT_CHECKS, 1);
        // ws_table_check
        if (ws_table_checkDT(&record->tag))
        {
            BCDB_FLOW_LOG("[BCDB_FLOW] conflict_check_ws_hit pid=%d txid=%d cand_txid=%d",
                          (int)getpid(),
                          activeTx ? (int)activeTx->tx_id : -1,
                          (int)bcdb_get_last_conflict_txid());
            bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_CONFLICT_WS_US,
                                   ws_check_start);
            // printf("safeDB %s : %s: %d tx %s %d conflict due to waw \n",
            // __FILE__, __FUNCTION__, __LINE__ ,  activeTx->hash, activeTx->tx_id);
            return 1;
        }
        // ereport(ERROR,
        //		(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
        //		 errmsg("tx %s aborted due to waw", activeTx->hash)));
    }

    bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_CONFLICT_WS_US, ws_check_start);

    rs_check_start = bcdb_ptrace_timer_start();
    LIST_FOREACH(record, &rs_table_record, link)
    {
        bcdb_ptrace_inc_counter(BCDB_PTRACE_COUNTER_RS_CONFLICT_CHECKS, 1);
        // ws_table_check
        if (ws_table_checkDT(&record->tag))
        {
            BCDB_FLOW_LOG("[BCDB_FLOW] conflict_check_rs_hit pid=%d txid=%d cand_txid=%d",
                          (int)getpid(),
                          activeTx ? (int)activeTx->tx_id : -1,
                          (int)bcdb_get_last_conflict_txid());
            bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_CONFLICT_RS_US,
                                   rs_check_start);
            return 1;
        }
        // ereport(ERROR,
        //(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
        // errmsg("tx %s aborted due to raw", activeTx->hash)));
    }
    bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_CONFLICT_RS_US, rs_check_start);

#if SAFEDBG2
    if (cc2Count++ == ccMax)
    {
        cc2Count = 0;
        printf("safeDB %s : %s: %d -- one in 20\n", __FILE__, __FUNCTION__, __LINE__);
    }
#endif
    return 0;
}

/*
 * conflict_check  (non-DT path)
 *
 * Legacy conflict check used when OEP_mode=false or the Deterministic
 * Execution (DT) scheme is not active.  Walks ws_table_record and
 * rs_table_record using ws_table_check() / rs_table_check() against
 * ws_table->map.
 *
 * Design note: because ws_table_reserve() (which populated ws_table->map)
 * has been removed, ws_table_check() and rs_table_check() always return
 * false.  This function is therefore a no-op for conflict detection in the
 * current codebase.  It is kept to avoid breaking the non-DT code path
 * in worker.c.  If the non-DT path needs to be made functional again,
 * ws_table_reserve() and rs_table_reserve() should be re-introduced.
 */
void conflict_check(void)
{
    WSTableEntryRecord *record;

    LIST_FOREACH(record, &ws_table_record, link)
    {
        if (ws_table_check(&record->tag))
            ereport(ERROR,
                    (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                     errmsg("tx %s aborted due to waw", activeTx->hash)));
    }

    LIST_FOREACH(record, &ws_table_record, link)
    {
        if (rs_table_check(&record->tag))
        {
            WSTableEntryRecord *raw_record;
            LIST_FOREACH(raw_record, &rs_table_record, link)
            {
                if (ws_table_check(&raw_record->tag))
                    ereport(ERROR,
                            (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                             errmsg("tx %s aborted due to raw and war", activeTx->hash)));
            }
            break;
        }
    }
}

/*
 * publish_ws_tableDT
 *
 * Publishes the current transaction's write-set (ws_table_record list) into
 * the shared Deterministic Execution (DT) write-set hash table so that
 * future transactions can detect waw conflicts via ws_table_checkDT().
 *
 * Deterministic Execution (DT) path ping-pong:
 *   id / HASHTAB_SWITCH_THRESHOLD determines which shard (map vs mapB) is
 *   "active".  When the threshold crosses a new epoch, the inactive shard is
 *   bulk-cleared (shm_hash_clear) and becomes the new active shard.  This
 *   avoids clearing the table while active readers/writers are using it.
 *
 *   Requirement: HASHTAB_SWITCH_THRESHOLD >= 2 * NUM_WORKERS - 1 so that
 *   no worker is still reading the old shard when it is cleared.
 *
 * For each write-set record, HASH_ENTER stores the latest writer tx_id seen in
 * the active shard.  That is enough for later conflict checks: if the latest
 * earlier writer committed before our snapshot, every older writer is also safe;
 * if it did not, the current tx must retry.
 *
 * Called from worker.c after conflict_checkDT() returns clean.
 */
void publish_ws_tableDT(int id)
{
    uint64 publish_start = bcdb_ptrace_timer_start();
    int threshold;
    int min_threshold;
    int workers;
    bool using_map_b = false;
    bool published_any = false;
    bool found;
    WSTableEntry *entry;
    PREDICATELOCKTARGETTAG *tag;
    uint32 tuple_hash = 0;
    WSTableEntryRecord *record;
    slock_t *partition_lock;
    int x = 0;

    if (!bcdb_dt_conflict_tracking)
        return;

    workers = bcdb_worker_count;
    if (workers <= 0)
        workers = BCDB_DEFAULT_WORKER_COUNT;
    if (workers <= 0)
        workers = 1;

    threshold = bcdb_dt_hashtab_switch_threshold;
    min_threshold = 2 * workers - 1;
    if (threshold < min_threshold)
    {
        ereport(ERROR,
                (errmsg("HASHTAB_SWITCH_THRESHOLD (%d) must be >= %d",
                        threshold, min_threshold)));
    }

    x = id / threshold; // min 2* num_w -1
    if (x % 2 == 0)
    {
        ws_table->mapActive = ws_table->map;
        if (id % threshold == 0)
        {
			uint64 hash_clear_start = bcdb_ptrace_timer_start();
			uint64 profile_clear_start = bcdb_get_time();
			uint64 lock_acquire_us = 0;
			uint64 clear_us = 0;
			WSTableClearShard(ws_table, ws_table->map, false,
							  &lock_acquire_us, &clear_us);
			bcdb_ptrace_add_us(BCDB_PTRACE_METRIC_PUBLISH_ROTATION_LOCK_US, lock_acquire_us);
			bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_PUBLISH_HASH_CLEAR_US,
								   hash_clear_start);
			bcdb_ptrace_inc_counter(BCDB_PTRACE_COUNTER_PUBLISH_HASH_CLEAR_COUNT, 1);
			if (bcdb_ws_rotation_profile_enabled())
			{
				uint64 total_us = bcdb_get_time() - profile_clear_start;
				bcdb_ws_rotation_note(lock_acquire_us, clear_us, total_us);
				ereport(LOG,
						(errmsg("PROFILE_BCDB_WS_ROTATION pid=%d tx_id=%d shard=map rotation_count=%llu ws_rotation_lock_acquire_us=%llu ws_rotation_clear_us=%llu ws_rotation_total_us=%llu ws_rotation_max_us=%llu ws_rotation_stalled_publish_count=%llu",
								MyProcPid, id,
								(unsigned long long) bcdb_ws_rotation_count,
								(unsigned long long) lock_acquire_us,
								(unsigned long long) clear_us,
								(unsigned long long) total_us,
								(unsigned long long) bcdb_ws_rotation_max_us,
								(unsigned long long) bcdb_ws_rotation_stalled_publish_count)));
			}
            // shm_hash_clear(rs_table->map, MAX_WRITE_CONFLICT);
        }
    }
    else
    {
        ws_table->mapActive = ws_table->mapB;
        using_map_b = true;
        if (id % threshold == 0)
        {
			uint64 hash_clear_start = bcdb_ptrace_timer_start();
			uint64 profile_clear_start = bcdb_get_time();
			uint64 lock_acquire_us = 0;
			uint64 clear_us = 0;
			WSTableClearShard(ws_table, ws_table->mapB, true,
							  &lock_acquire_us, &clear_us);
			bcdb_ptrace_add_us(BCDB_PTRACE_METRIC_PUBLISH_ROTATION_LOCK_US, lock_acquire_us);
			bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_PUBLISH_HASH_CLEAR_US,
								   hash_clear_start);
			bcdb_ptrace_inc_counter(BCDB_PTRACE_COUNTER_PUBLISH_HASH_CLEAR_COUNT, 1);
			if (bcdb_ws_rotation_profile_enabled())
			{
				uint64 total_us = bcdb_get_time() - profile_clear_start;
				bcdb_ws_rotation_note(lock_acquire_us, clear_us, total_us);
				ereport(LOG,
						(errmsg("PROFILE_BCDB_WS_ROTATION pid=%d tx_id=%d shard=mapB rotation_count=%llu ws_rotation_lock_acquire_us=%llu ws_rotation_clear_us=%llu ws_rotation_total_us=%llu ws_rotation_max_us=%llu ws_rotation_stalled_publish_count=%llu",
								MyProcPid, id,
								(unsigned long long) bcdb_ws_rotation_count,
								(unsigned long long) lock_acquire_us,
								(unsigned long long) clear_us,
								(unsigned long long) total_us,
								(unsigned long long) bcdb_ws_rotation_max_us,
								(unsigned long long) bcdb_ws_rotation_stalled_publish_count)));
			}
            // shm_hash_clear(rs_table->mapB, MAX_WRITE_CONFLICT);
        }
    } // clean_rs_ws_table(id); // reset before HASH_ENTER get-write-set !!!

    LIST_FOREACH(record, &ws_table_record, link)
    {
        uint64 lock_start;
        tag = &(record->tag);
        tuple_hash = PredicateLockTargetTagHashCode(tag);
		partition_lock = using_map_b ? WSTableMapBPartitionLock(ws_table, tuple_hash) : WSTableMapAPartitionLock(ws_table, tuple_hash);
        
        lock_start = bcdb_ptrace_timer_start();
        SpinLockAcquire(partition_lock);
        bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_PUBLISH_PARTITION_LOCK_US, lock_start);
        entry = (WSTableEntry *)hash_search_with_hash_value(ws_table->mapActive,
                                                            tag,
                                                            tuple_hash,
                                                            HASH_ENTER,
                                                            &found);
        if (!found || entry->tx_id < activeTx->tx_id)
            entry->tx_id = activeTx->tx_id;
        SpinLockRelease(partition_lock);
        published_any = true;
        bcdb_ptrace_inc_counter(BCDB_PTRACE_COUNTER_WS_PUBLISH_ENTRIES, 1);
    }

    if (using_map_b && published_any)
        pg_atomic_write_u32(&ws_table->mapB_nonempty, 1);
    bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_PUBLISH_WS_US, publish_start);
}

/*
 * clean_rs_ws_table
 *
 * Bulk-clears both the write-set and read-set conflict maps (ws_table->map
 * and rs_table->map).  Called at the start of a new block epoch from
 * worker.c and tcop/postgres.c to reset the non-DT conflict tables.
 *
 * Note: only ws_table->map is cleared here (not mapB), because the DT
 * path manages mapB rotation inside publish_ws_tableDT().
 */
void clean_rs_ws_table(void)
{
    shm_hash_clear(ws_table->map, MAX_WRITE_CONFLICT);
    shm_hash_clear(rs_table->map, MAX_WRITE_CONFLICT);
}

void
bcdb_emit_ledger_boundary(const char *phase)
{
	if (is_bcdb_worker && activeTx != NULL)
	{
		/*
		 * Use the D1 struct fields directly when ledger is enabled;
		 * fall back to SQL-string parsing for legacy/debug mode.
		 */
		uint64 raft_log_index = 0;
		uint32 item_ordinal = 0;

		if (activeTx->raft_ledger_enabled)
		{
			raft_log_index = activeTx->raft_log_index;
			item_ordinal   = activeTx->raft_item_ordinal;
		}
		else if (activeTx->sql[0] != '\0')
		{
			const char *p = strstr(activeTx->sql, "raft_log_index=");
			if (p)
				raft_log_index = strtoull(p + 15, NULL, 10);
			p = strstr(activeTx->sql, "item_ordinal=");
			if (p)
				item_ordinal = (uint32) atoi(p + 13);
		}

		ereport(LOG,
				(errmsg("RAFT_LEDGER_BOUNDARY backend_pid=%d bcdb_tx_id=%ld raft_log_index=%llu item_ordinal=%u top_level_xid=%u subxact_depth=%d phase=%s",
						getpid(),
						(long)activeTx->tx_id,
						(unsigned long long)raft_log_index,
						(unsigned)item_ordinal,
						GetTopTransactionIdIfAny(),
						GetCurrentTransactionNestLevel(),
						phase)));
	}
}
