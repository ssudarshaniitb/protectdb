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
#include "bcdb/bcdb_dsa.h"
#include "utils/hsearch.h"
#include <stddef.h>
#include <access/genam.h>
#include "access/xact.h"
#include "access/heapam.h"
#include "access/merkle.h"
#include "catalog/pg_am_d.h"
#include "storage/bufmgr.h"
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
//#define MAP_TX(id) ((id) % TX_MAP_SZ)
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

//slock_t      *nexec_lock;

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
BCDBShmXact  *activeTx;
slock_t      *restart_counter_lock;
int          *numExecPt ;
HTAB         *tx_pool;
slock_t      *tx_pool_lock;
HTAB         *xid_map;
slock_t      *xid_map_lock;
TxQueue       *tx_queues;
WSTable       *ws_table;
WSTable       *rs_table;
WSTableRecord  ws_table_record;
WSTableRecord  rs_table_record;
extern HTAB   *PredicateLockTargetHash;
extern HTAB   *PredicateLockHash;

static TupleTableSlot* clone_slot(TupleTableSlot* slot);

#define PREDFMT " %d:%d:%d:%d "
#define PRINT_PREDICATELOCKTARGETTAG(locktag) \
              GET_PREDICATELOCKTARGETTAG_DB(locktag), \
              GET_PREDICATELOCKTARGETTAG_RELATION(locktag), \
              GET_PREDICATELOCKTARGETTAG_PAGE(locktag), \
              GET_PREDICATELOCKTARGETTAG_OFFSET(locktag)

#define WSTableGetPartitionIdx(hashcode) ((hashcode) % WRITE_CONFLICT_MAP_NUM_PARTITIONS)
#define WSTablePartitionLock(hashcode) (&(ws_table->map_locks[(hashcode) % WRITE_CONFLICT_MAP_NUM_PARTITIONS]))
#define RSTableGetPartitionIdx(hashcode) ((hashcode) % WRITE_CONFLICT_MAP_NUM_PARTITIONS)
#define RSTablePartitionLock(hashcode) (&(rs_table->map_locks[(hashcode) % WRITE_CONFLICT_MAP_NUM_PARTITIONS]))

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
    return *(uint32*)key;
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
    //printf("safeDB %s : %s: %d txid %d hash %s \n", __FILE__, __FUNCTION__, __LINE__ , tx_id, hash);
    if (hash == NULL)
        ereport(ERROR, (errmsg("[ZL] cannot create transaction with NULL hash")));
    hash_len = strlen(hash);
    if (hash_len >= TX_HASH_SIZE)
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("[ZL] transaction hash too long (%zu, max %d): \"%s\"",
                        (size_t) hash_len, TX_HASH_SIZE - 1, hash)));

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
    LWLockAcquire(&tx->lock, LW_EXCLUSIVE);
    SpinLockRelease(tx_pool_lock);

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
    tx->isolation = isolation;
    tx->pred_lock = pred_lock;
    tx->queue_link.tqe_prev = NULL;
    tx->create_time = 0;
    tx->has_raw = false;
    tx->has_war = false;
    SHA256_Init(&tx->state_hash);
    SIMPLEQ_INIT(&tx->optim_write_list);

    ConditionVariableInit(&tx->cond);
    // mapTxToShm[ MAP_TX(tx_id) ] = tx;
#if SAFEDBG2
    printf("safeDB %s : %s: %d \n", __FILE__, __FUNCTION__, __LINE__ );
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
void
delete_tx(BCDBShmXact *tx)
{
    bool found;
    if (!tx)
        return;

    DEBUGNOCHECK("[ZL] deleting tx %s", tx->hash);
    remove_tx_xid_map(tx->xid);

    SpinLockAcquire(tx_pool_lock);
    hash_search(tx_pool, tx->hash, HASH_REMOVE, &found);
    SpinLockRelease(tx_pool_lock);
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
void
create_tx_pool(void)
{
    HASHCTL info;
    slock_t *tx_pool_lock_array;
    bool    found;

    restart_counter_lock = ShmemInitStruct("restart_counter_lock", sizeof(slock_t) , &found);
    //nexec_lock = ShmemInitStruct("nexec_lock", sizeof(slock_t) , &found);
#if SAFEDBG
    DEBUGNOCHECK("[BCDB] create_tx_pool (%s:%s:%d)", __FILE__, __FUNCTION__, __LINE__);
#endif
    tx_pool_lock_array = ShmemInitStruct("tx_pool_lock", sizeof(slock_t) * 2, &found);
    tx_pool_lock = tx_pool_lock_array;
    xid_map_lock = tx_pool_lock + 1;
    numExecPt = (int  *) ShmemAlloc( sizeof(int ));
    *numExecPt = 0;

    if (!found)
    {
        SpinLockInit(tx_pool_lock);
        SpinLockInit(restart_counter_lock);
        //SpinLockInit(nexec_lock);
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
                     (int) getpid(), __FILE__, __FUNCTION__, __LINE__);
#endif
        info.keysize = sizeof(PREDICATELOCKTARGETTAG);
        info.entrysize = sizeof(WSTableEntry);
        info.hash = dummy_hash;
        info.num_partitions = WRITE_CONFLICT_MAP_NUM_PARTITIONS;
        ws_table->map = ShmemInitHash("bcdb_write_conflict_map",
                                     MAX_WRITE_CONFLICT,
                                     MAX_WRITE_CONFLICT,
                                     &info, HASH_ELEM | HASH_FUNCTION | HASH_FIXED_SIZE | HASH_PARTITION);
        ws_table->mapB = ShmemInitHash("bcdb_write_conflict_mapDT",
									MAX_WRITE_CONFLICT,
									MAX_WRITE_CONFLICT,
									&info, HASH_ELEM | HASH_FUNCTION | HASH_FIXED_SIZE | HASH_PARTITION);
        ws_table->mapActive = ws_table->map;
        for (int i=0; i < WRITE_CONFLICT_MAP_NUM_PARTITIONS; i++)
            SpinLockInit(&(ws_table->map_locks[i]));
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
        info.keysize = sizeof(PREDICATELOCKTARGETTAG);
        info.entrysize = sizeof(WSTableEntry);
        info.hash = dummy_hash;
        info.num_partitions = WRITE_CONFLICT_MAP_NUM_PARTITIONS;
        rs_table->map = ShmemInitHash("bcdb_read_conflict_map",
                                     MAX_WRITE_CONFLICT,
                                     MAX_WRITE_CONFLICT,
                                     &info, HASH_ELEM | HASH_FUNCTION | HASH_FIXED_SIZE | HASH_PARTITION);
        for (int i=0; i < WRITE_CONFLICT_MAP_NUM_PARTITIONS; i++)
            SpinLockInit(&(rs_table->map_locks[i]));
    }
}

Size
tx_pool_size(void)
{
    Size ret = hash_estimate_size(MAX_SHM_TX, sizeof(BCDBShmXact));
    ret = add_size(ret, hash_estimate_size(MAX_SHM_TX, sizeof(XidMapEntry)));
    ret = add_size(ret, sizeof(slock_t) * 2);
    ret = add_size(ret, sizeof(TxQueue) * NUM_TX_QUEUE_PARTITION);
    ret = add_size(ret, sizeof(WSTable));
    ret = add_size(ret, hash_estimate_size(MAX_WRITE_CONFLICT, sizeof(WSTableEntry)));
    return ret; 
}

void
clear_tx_pool(void)
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
 * NOTE: rs_table_reserve() (the old non-DT variant that actually wrote to
 * rs_table->map) has been removed — all its call sites were replaced by
 * this function.
 */
void
rs_table_reserveDT(const PREDICATELOCKTARGETTAG *tag)
{
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
void
ws_table_reserveDT(PREDICATELOCKTARGETTAG *tag)
{
    WSTableEntryRecord *record;

    record = MemoryContextAlloc(bcdb_tx_context, sizeof(WSTableEntryRecord));
    record->tag = *tag;
    LIST_INSERT_HEAD(&ws_table_record, record, link);
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
 * Holds the per-partition spinlock only for the hash lookup to minimise
 * contention; the early-exit paths release it immediately on conflict.
 *
 * Used by ws_table_checkDT() and indirectly by conflict_checkDT().
 */
bool
table_checkDT(PREDICATELOCKTARGETTAG *tag, WSTable *table)
{
    bool found;
    WSTableEntry* entry;
    uint32  tuple_hash = PredicateLockTargetTagHashCode(tag);
    slock_t *partition_lock = WSTablePartitionLock(tuple_hash);
    static int once_out = false;

    if(!once_out) {
        once_out = true;
#if SAFEDBG2
        printf("\nsafeDB %s : %s: %d  getpid %d tx_id %d\n",
                __FILE__, __FUNCTION__, __LINE__ , getpid(), activeTx->tx_id);
#endif
    }

    SpinLockAcquire(partition_lock);
    entry = hash_search_with_hash_value(table->map, tag,
					tuple_hash, HASH_FIND, &found);
    if (found && (entry->tx_id < activeTx->tx_id) &&
				(entry->tx_id >  activeTx->tx_id_committed))
    {
#if SAFEDBG
        int winner_tx_id = entry->tx_id;
#endif
        SpinLockRelease(partition_lock);
#if SAFEDBG
        ereport(DEBUG3,
                (errmsg("safeDB tx %d hash %s check write %u failed, winner: %d",
                        activeTx->tx_id, activeTx->hash, tuple_hash, winner_tx_id)));
#endif
        return true;
    }

    entry = hash_search_with_hash_value(table->mapB, tag,
							tuple_hash, HASH_FIND, &found);
    if (found && (entry->tx_id < activeTx->tx_id) &&
				(entry->tx_id >  activeTx->tx_id_committed))
    {
#if SAFEDBG
        int winner_tx_id = entry->tx_id;
#endif
        SpinLockRelease(partition_lock);
#if SAFEDBG
        ereport(DEBUG3,
                (errmsg("safeDB tx %d hash %s check write %u failed, winner: %d",
                        activeTx->tx_id, activeTx->hash, tuple_hash, winner_tx_id)));
#endif
        return true;
    }
    SpinLockRelease(partition_lock);
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
bool
ws_table_checkDT(PREDICATELOCKTARGETTAG *tag)
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
bool
ws_table_check(PREDICATELOCKTARGETTAG *tag)
{
    bool found;
    WSTableEntry* entry;
    uint32  tuple_hash = PredicateLockTargetTagHashCode(tag);
    slock_t *partition_lock = WSTablePartitionLock(tuple_hash);

#if SAFEDBG
    printf("\nariaDB %s : %s: %d \n", __FILE__, __FUNCTION__, __LINE__ );
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
bool
rs_table_check(PREDICATELOCKTARGETTAG *tag)
{
    bool found;
    WSTableEntry* entry;
    uint32  tuple_hash = PredicateLockTargetTagHashCode(tag);
    slock_t *partition_lock = RSTablePartitionLock(tuple_hash);

#if SAFEDBG
    printf("ariaDB %s : %s: %d \n", __FILE__, __FUNCTION__, __LINE__ );
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
void
tx_queue_insert(BCDBShmXact *tx, int32 partition)
{
    struct timeval tv1;
    tv1.tv_sec = 0; tv1.tv_usec = 0;
    bool    found;

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
        //SpinLockAcquire(nexec_lock);
    gettimeofday(&tv1, NULL);
#if SAFEDBG
    printf(" safeDB func %s hash %s time= %ld.%ld\n", __FUNCTION__, tx->hash, tv1.tv_sec, tv1.tv_usec);
#endif
        //SpinLockRelease(nexec_lock);
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
BCDBShmXact*
tx_queue_next(int32 partition)
{
    struct timeval tv1;
    tv1.tv_sec = 0; tv1.tv_usec = 0;

    BCDBShmXact *tx;
    int num_queue = OEP_mode ? blocksize * 2 : blocksize;
    num_queue = get_blksz(); // get_block_by_id(1, false)->blksize; ==nWorker
    TxQueue *queue = tx_queues + (partition % num_queue);
    ConditionVariablePrepareToSleep(&queue->empty_cond);
    SpinLockAcquire(&queue->lock);
#if SAFEDBG
	printf("safeDB %s : %s: %d partition %d num_queue %d \n", __FILE__, __FUNCTION__, __LINE__ , partition, num_queue);
#endif
    /*
	//SpinLockAcquire(nexec_lock);
    *numExecPt -= 1;
    //SpinLockRelease(nexec_lock);
    */
    gettimeofday(&tv1, NULL);
    // printf("\n safeDB func %s time= %ld.%ld\n", __FUNCTION__, tv1.tv_sec, tv1.tv_usec);
	    //printf("safeDB %s : %s: %d nExec= %d \n\n", __FILE__, __FUNCTION__, __LINE__ ,  *numExecPt);
//	}
    while (queue->size <= 0)
    {

        SpinLockRelease(&queue->lock);
        ConditionVariableSleep(&queue->empty_cond, WAIT_EVENT_TX_READY_TO_COMMIT);
        SpinLockAcquire(&queue->lock);
    }
    ConditionVariableCancelSleep();
    tx = TAILQ_FIRST(&queue->list);
    TAILQ_REMOVE(&queue->list, tx, queue_link);
    tx->queue_link.tqe_prev = NULL;
    /*
	if (((tx)->queue_link.tqe_next) == NULL)	{
	printf("safeDB %s : %s: %d partition %d num_queue %d \n", __FILE__, __FUNCTION__, __LINE__ , partition, num_queue);
	printf("safeDB ** next NULL ** %s : %s: %d partition %d num_queue %d \n", __FILE__, __FUNCTION__, __LINE__ , partition, num_queue); }
	printf("safeDB %s : %s: %d partition %d num_queue %d \n", __FILE__, __FUNCTION__, __LINE__ , partition, num_queue);
#define TAILQ_REMOVE(head, elm, field) do {				\
	if (((elm)->field.tqe_next) != NULL)				\
	if (((tx)->queue_link.tqe_next) != NULL)				\
	SpinLockAcquire(nexec_lock);
    if( ( *numExecPt + blocksize) ==0) 
	{
	shm_hash_clear(ws_table->map, MAX_WRITE_CONFLICT);
	shm_hash_clear(ws_table->mapB, MAX_WRITE_CONFLICT);
	printf(" safeDB %s : %s: %d nExec= 0 !!! reset htab \n\n", __FILE__, __FUNCTION__, __LINE__ );
	}
	*numExecPt += 1;
	SpinLockRelease(nexec_lock);
	printf("safeDB %s : %s: %d nExec= %d tx %d\n\n", __FILE__, __FUNCTION__, __LINE__ ,  *numExecPt , tx->tx_id);
    */
    gettimeofday(&tv1, NULL);
#if SAFEDBG
    printf("\n func %s hash %s time= %ld.%ld", __FUNCTION__, tx->hash, tv1.tv_sec, tv1.tv_usec);
	printf("safeDB %s : %s: %d pid %d tx %d\n", __FILE__, __FUNCTION__, __LINE__ ,  getpid() , tx->tx_id);
#endif
    queue->size -= 1;
    SpinLockRelease(&queue->lock);
    ConditionVariableSignal(&queue->full_cond);
    return tx; 
}

static TupleTableSlot*
clone_slot(TupleTableSlot* slot)
{
    TupleTableSlot* ret;
    TupleDesc newdesc;

    /*
     * CRITICAL: Use TTSOpsHeapTuple (not the source slot's ops) to ensure the
     * clone MATERIALIZES the data with its own heap-allocated copy.
     *
     * If the source is a BufferHeapTupleTableSlot with a pinned buffer,
     * ExecCopySlot into another BufferHeapTupleTableSlot would call
     * tts_buffer_heap_store_tuple(), which pins THE SAME heap buffer and
     * registers it with CurrentResourceOwner.  When the optimistic-phase
     * transaction ends, the ResourceOwner releases that pin.  But the clone
     * survives (in bcdb_tx_context) until serial-apply, where
     * ExecDropSingleTupleTableSlot tries to ReleaseBuffer on the already-
     * unpinned buffer → TRAP "ref != NULL" in UnpinBuffer.
     *
     * TTSOpsHeapTuple slots always copy the tuple into palloc'd memory,
     * so they never hold buffer pins and are safe across transaction
     * boundaries.
     */
    ret = MakeTupleTableSlot(slot->tts_tupleDescriptor, &TTSOpsHeapTuple);
    ExecCopySlot(ret, slot);
    ret->tts_tableOid = slot->tts_tableOid;
    /*
     * Create an independent copy of the TupleDesc for the cloned slot.
     * MakeTupleTableSlot already pinned the original TupleDesc, so we must
     * release that pin before replacing the pointer with the copy.
     * Without this, the original TupleDesc's refcount would never reach zero,
     * causing TupleDesc reference leak warnings on every BCDB transaction.
     */
    newdesc = CreateTupleDescCopy(slot->tts_tupleDescriptor);
    ReleaseTupleDesc(ret->tts_tupleDescriptor);
    ret->tts_tupleDescriptor = newdesc;
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
BCDBShmXact*
get_tx_by_hash(const char *hash)
{
    BCDBShmXact *ret;
    SpinLockAcquire(tx_pool_lock);
    ret = hash_search(tx_pool, hash, HASH_FIND, NULL);
    SpinLockRelease(tx_pool_lock);
    return ret;
}

/*
 * get_tx_by_xid and get_tx_by_xid_locked have been removed.
 *
 * get_tx_by_xid: looked up xid_map without acquiring the per-tx LWLock.
 *   It had no callers in the live codebase.
 *
 * get_tx_by_xid_locked: looked up xid_map AND acquired tx->lock before
 *   returning (for safe mutation of the entry).  Also had no callers; it
 *   was intended for SSI predicate-lock conflict callbacks but was never
 *   wired up.  If SSI callbacks need this in future, re-introduce it here.
 */

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
void
add_tx_xid_map(TransactionId xid, BCDBShmXact *tx)
{
    XidMapEntry *entry;
    bool    found;

    DEBUGNOCHECK("[ZL] add xid map: %d -> %s", (int)xid, tx->hash);
    SpinLockAcquire(xid_map_lock);
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
void
remove_tx_xid_map(TransactionId xid)
{
    bool found;
    XidMapEntry *entry;
    if (!TransactionIdIsValid(xid))
        return;

    DEBUGNOCHECK("[ZL] removing xid map: %d", (int)xid);
    SpinLockAcquire(xid_map_lock);
    entry = hash_search(xid_map, &xid, HASH_FIND, &found);
    if (entry)
    {
        LWLockAcquire(&entry->tx->lock, LW_EXCLUSIVE);
        hash_search(xid_map, &xid, HASH_REMOVE, &found);
        LWLockRelease(&entry->tx->lock);
    }
    SpinLockRelease(xid_map_lock);
    if (!found)
        ereport(FATAL, (errmsg("[ZL] xid map %d is already deleted!", (int)xid)));
}

void
store_optim_update(TupleTableSlot* slot, ItemPointer old_tid)
{
    OptimWriteEntry *write_entry;
    MemoryContext    old_context;
    DEBUGMSG("[ZL] tx %s storing update to (%d %d %d)", activeTx->hash, slot->tts_tableOid, *(int*)&old_tid->ip_blkid, (int)old_tid->ip_posid);
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
            __FILE__, __FUNCTION__, __LINE__ , activeTx->tx_id, write_entry->cid );
#endif
    //debugtup(slot, NULL);
}

void
store_optim_insert(TupleTableSlot* slot)
{
    OptimWriteEntry *write_entry;
    MemoryContext    old_context;
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
}

void
store_optim_delete(Oid relOid, ItemPointer tupleid, TupleTableSlot *slot)
{
    OptimWriteEntry *write_entry;
    MemoryContext    old_context;
    DEBUGMSG("[ZL] tx %s storing delete (rel: %d)", activeTx->hash, relOid);
    old_context = MemoryContextSwitchTo(bcdb_tx_context);
    write_entry = palloc(sizeof(OptimWriteEntry));
    write_entry->operation = CMD_DELETE;
    write_entry->slot = slot ? clone_slot(slot) : NULL;
    write_entry->old_tid = *tupleid;
    write_entry->relOid = relOid;
    write_entry->cid = GetCurrentCommandId(true);
    write_entry->keyval = -1;
    SIMPLEQ_INSERT_TAIL(&activeTx->optim_write_list, write_entry, link);
    MemoryContextSwitchTo(old_context);
}

void
store_optim_delete_by_key(Oid relOid, int32 keyval, CommandId cid)
{
    OptimWriteEntry *write_entry;
    MemoryContext    old_context;

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

bool
apply_optim_insert(TupleTableSlot* slot, CommandId cid)
{
    Relation relation = RelationIdGetRelation(slot->tts_tableOid);
    MemoryContext old_context = CurrentMemoryContext;
    ResourceOwner old_owner = CurrentResourceOwner;
    bool insert_ok = false;
    ErrorData *edata = NULL;

    DEBUGMSG("[ZL] tx %s applying optim insert (rel: %d)", activeTx->hash, relation->rd_id);

    /*
     * Run the heap + index insert inside a subtransaction so we can catch a
     * duplicate-key ERROR (or any ERROR) without aborting the caller's serial
     * transaction.
     *
     * Note: Merkle indexes mutate shared buffers directly, so they rely on the
     * explicit (sub)xact undo mechanism in merkleutil.c to rollback XOR writes
     * on subtransaction abort.
     */

    BeginInternalSubTransaction("bcdb_insert");
    PG_TRY();
    {
        table_tuple_insert(relation, slot, cid, 0, NULL);

        heap_apply_index(relation, slot, true, true);

        ReleaseCurrentSubTransaction();
        MemoryContextSwitchTo(old_context);
        CurrentResourceOwner = old_owner;
        insert_ok = true;
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(old_context);
        edata = CopyErrorData();
        FlushErrorState();
        MemoryContextSwitchTo(old_context);
        RollbackAndReleaseCurrentSubTransaction();
        MemoryContextSwitchTo(old_context);
        CurrentResourceOwner = old_owner;
    }
    PG_END_TRY();

    RelationClose(relation);

    if (edata != NULL)
    {
        if (edata->sqlerrcode == ERRCODE_UNIQUE_VIOLATION)
        {
            FreeErrorData(edata);
            return false;
        }

        ReThrowError(edata);
    }

    return insert_ok;
}

void
apply_optim_update(ItemPointer tid, TupleTableSlot* slot, CommandId cid)
{
    TM_FailureData tmfd;
    TM_Result result;
    LockTupleMode lockmode;
    bool update_indexes;
    Relation relation = RelationIdGetRelation(slot->tts_tableOid);
    List *indexList = NIL;
    ListCell *lc;
    TupleTableSlot *oldSlot = NULL;
    TupleTableSlot *newSlot = NULL;
    MerkleHash oldHash;
    MerkleHash newHash;
    bool hasOldHash = false;
    bool hasNewHash = false;
    int pendingCount = 0;
    int pendingCapacity = 0;
    typedef struct PendingMerkleUpdate
    {
        Oid indexOid;
        int oldLeafId;
    } PendingMerkleUpdate;
    PendingMerkleUpdate *pending = NULL;

    DEBUGMSG("[ZL] tx %s applying optim update (%d %d %d)", activeTx->hash, relation->rd_id, *(int*)&tid->ip_blkid, (int)tid->ip_posid);
#if SAFEDBG1
    printf("safeDB %s : %s: %d tm-ok %d tx %d cid %d\n",
            __FILE__, __FUNCTION__, __LINE__ , TM_Ok, activeTx->tx_id, cid );
#endif
    
    if (enable_merkle_index && ItemPointerIsValid(tid) &&
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
            RelationClose(relation);
            ereport(ERROR,
                    (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                     errmsg("tx %s doomed because old row image was not fetchable", activeTx->hash)));
        }

        /* Compute old hash from the same oldSlot image used for leafing */
        merkle_compute_slot_hash(relation, oldSlot, &oldHash);

        hasOldHash = !merkle_hash_is_zero(&oldHash);

        if (hasOldHash)
        {
            indexList = RelationGetIndexList(relation);
            pendingCapacity = list_length(indexList);
            if (pendingCapacity > 0)
                pending = palloc0(sizeof(PendingMerkleUpdate) * pendingCapacity);

            foreach(lc, indexList)
            {
                Oid indexOid = lfirst_oid(lc);
                Relation indexRel = index_open(indexOid, RowExclusiveLock);

                if (indexRel->rd_rel->relam == MERKLE_AM_OID)
                {
                    IndexInfo  *indexInfo;
                    Datum       values[INDEX_MAX_KEYS];
                    bool        isnull[INDEX_MAX_KEYS];
                    int         totalLeaves;

                    indexInfo = BuildIndexInfo(indexRel);
                    FormIndexDatum(indexInfo, oldSlot, NULL, values, isnull);
                    merkle_read_meta(indexRel, NULL, NULL, NULL, NULL, &totalLeaves, NULL, NULL, NULL);

                    pending[pendingCount].indexOid = indexOid;
                    pending[pendingCount].oldLeafId =
                        merkle_compute_partition_id(values, isnull,
                                                    indexInfo->ii_NumIndexKeyAttrs,
                                                    RelationGetDescr(indexRel),
                                                    totalLeaves);
                    pendingCount++;
                }

                index_close(indexRel, RowExclusiveLock);
            }
        }
    }
    
    result = table_tuple_update(relation, tid, slot,
                       cid,
                       InvalidSnapshot,
                       InvalidSnapshot,
                       false, /* do not wait for commit */
                       &tmfd, &lockmode, &update_indexes);

    if (result != TM_Ok)
    {
        if (oldSlot)
            ExecDropSingleTupleTableSlot(oldSlot);
        if (newSlot)
            ExecDropSingleTupleTableSlot(newSlot);
        if (indexList)
            list_free(indexList);
        if (pending)
            pfree(pending);
        RelationClose(relation);
#if SAFEDBG1
        printf("safeDB %s : %s: %d ret %d tx %d tx %s doomed because of ww-conflict \n",
               __FILE__, __FUNCTION__, __LINE__ , result, activeTx->tx_id, activeTx->hash );
        printf("safeDB %s : %s: %d   tmfd.xmax %d, tmfd.cmax %d  ww-conflict \n",
               __FILE__, __FUNCTION__, __LINE__ , tmfd.xmax, tmfd.cmax  );
#endif
        ereport(ERROR,
                (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                 errmsg("tx %s doomed because of ww-conflict", activeTx->hash)));
    }

    if (update_indexes)
        heap_apply_index_phase(relation, slot, false, false, HEAP_INDEX_NO_MERKLE);

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
        /*
         * Fetch NEW row image from heap and hash from that image so that
         * merkle_verify (which hashes heap tuples) matches exactly.
         */
        newSlot = table_slot_create(relation, NULL);
        if (!table_tuple_fetch_row_version(relation, &slot->tts_tid, SnapshotSelf, newSlot))
        {
            if (oldSlot)
                ExecDropSingleTupleTableSlot(oldSlot);
            if (indexList)
                list_free(indexList);
            if (pending)
                pfree(pending);
            RelationClose(relation);
            ereport(ERROR,
                    (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                     errmsg("tx %s doomed because new row image was not fetchable", activeTx->hash)));
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
                    int totalLeaves;
                    int newLeafId;

                    indexInfo = BuildIndexInfo(indexRel);
                    FormIndexDatum(indexInfo, newSlot, NULL, values, isnull);
                    merkle_read_meta(indexRel, NULL, NULL, NULL, NULL, &totalLeaves, NULL, NULL, NULL);
                    newLeafId = merkle_compute_partition_id(values, isnull,
                                                           indexInfo->ii_NumIndexKeyAttrs,
                                                           RelationGetDescr(indexRel),
                                                           totalLeaves);

                    if (newLeafId == pending[i].oldLeafId)
                    {
                        MerkleHash delta = oldHash;
                        merkle_hash_xor(&delta, &newHash);
                        merkle_update_tree_path(indexRel, pending[i].oldLeafId, &delta, true);
                    }
                    else
                    {
                        merkle_update_tree_path(indexRel, pending[i].oldLeafId, &oldHash, false);
                        merkle_update_tree_path(indexRel, newLeafId, &newHash, true);
                    }
                }

                index_close(indexRel, RowExclusiveLock);
            }
        }
    }

    if (oldSlot)
        ExecDropSingleTupleTableSlot(oldSlot);
    if (newSlot)
        ExecDropSingleTupleTableSlot(newSlot);
    if (indexList)
        list_free(indexList);
    if (pending)
        pfree(pending);

    ExecDropSingleTupleTableSlot(slot);

    RelationClose(relation);
}

void
apply_optim_delete(Oid relOid, ItemPointer tupleid, TupleTableSlot *storedSlot, CommandId cid)
{
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
        int partitionId;
    } PendingMerkleDelete;
    PendingMerkleDelete *pending = NULL;
    ItemPointerData currentTid;
    bool oldSlotOwned = false;

    DEBUGMSG("[ZL] tx %s applying optim delete (rel: %d)", activeTx->hash, relOid);

    if (!enable_merkle_index)
    {
        result = table_tuple_delete(relation, tupleid,
                           cid,
                           InvalidSnapshot,
                           InvalidSnapshot,
                           false,
                           &tmfd,
                           false);

        if (result != TM_Ok)
        {
            RelationClose(relation);
            ereport(ERROR,
                    (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                     errmsg("tx %s doomed because of delete conflict", activeTx->hash)));
        }

        RelationClose(relation);
        return;
    }

    ItemPointerCopy(tupleid, &currentTid);

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
            if (oldSlotOwned && oldSlot)
                ExecDropSingleTupleTableSlot(oldSlot);
            RelationClose(relation);
            ereport(ERROR,
                    (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                     errmsg("tx %s doomed because deferred delete could not fetch tuple tid=(%u,%u)",
                            activeTx->hash,
                            ItemPointerGetBlockNumberNoCheck(&currentTid),
                            ItemPointerGetOffsetNumberNoCheck(&currentTid))));
        }
    }

    /* CRITICAL FIX: Use merkle_compute_row_hash instead of merkle_compute_slot_hash */
    merkle_compute_row_hash(relation, &currentTid, &oldHash);
    hasOldHash = !merkle_hash_is_zero(&oldHash);

    if (!hasOldHash)
    {
        bool d_isnull;
        Datum d_key;
        d_key = slot_getattr(oldSlot, 1, &d_isnull);
    }

        if (hasOldHash)
        {
            indexList = RelationGetIndexList(relation);
            pendingCapacity = list_length(indexList);
            if (pendingCapacity > 0)
                pending = palloc0(sizeof(PendingMerkleDelete) * pendingCapacity);

            foreach(lc, indexList)
            {
                Oid indexOid = lfirst_oid(lc);
                Relation indexRel = index_open(indexOid, RowExclusiveLock);

                if (indexRel->rd_rel->relam == MERKLE_AM_OID)
                {
                    IndexInfo  *indexInfo;
                    Datum       values[INDEX_MAX_KEYS];
                    bool        isnull[INDEX_MAX_KEYS];
                    int         totalLeaves;

                    indexInfo = BuildIndexInfo(indexRel);
                    FormIndexDatum(indexInfo, oldSlot, NULL, values, isnull);
                    merkle_read_meta(indexRel, NULL, NULL, NULL, NULL, &totalLeaves, NULL, NULL, NULL);

                    pending[pendingCount].indexOid = indexOid;
                    pending[pendingCount].partitionId =
                        merkle_compute_partition_id(values, isnull,
                                                    indexInfo->ii_NumIndexKeyAttrs,
                                                    RelationGetDescr(indexRel),
                                                    totalLeaves);

                    pendingCount++;
                }

                index_close(indexRel, RowExclusiveLock);
            }
        }

    /* Now delete the heap tuple using the (possibly updated) currentTid */
    result = table_tuple_delete(relation, &currentTid,
                       cid,
                       InvalidSnapshot,
                       InvalidSnapshot,
                       false, /* do not wait for commit */
                       &tmfd,
                       false);

    if (result != TM_Ok)
    {
        if (oldSlot)
            ExecDropSingleTupleTableSlot(oldSlot);
        if (indexList)
            list_free(indexList);
        if (pending)
            pfree(pending);
        RelationClose(relation);
        ereport(ERROR,
                (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                 errmsg("tx %s doomed because of delete conflict", activeTx->hash)));
    }

    if (hasOldHash)
    {
        int i;
        for (i = 0; i < pendingCount; i++)
        {
            Relation indexRel = index_open(pending[i].indexOid, RowExclusiveLock);
            if (indexRel->rd_rel->relam == MERKLE_AM_OID)
                merkle_update_tree_path(indexRel, pending[i].partitionId, &oldHash, false);
            index_close(indexRel, RowExclusiveLock);
        }
    }

    if (oldSlotOwned && oldSlot)
        ExecDropSingleTupleTableSlot(oldSlot);
    if (indexList)
        list_free(indexList);
    if (pending)
        pfree(pending);

    RelationClose(relation);
}

/*
 * apply_deferred_delete_by_key — handle DELETE-0 (concurrent visibility race).
 *
 * Called when a DELETE found 0 rows during the optimistic phase because the
 * target row was inserted by a concurrent transaction that had not yet
 * committed.  By the time this function runs (serial phase), all prior
 * transactions have committed, so the row is reachable via btree lookup.
 *
 * This duplicates the core logic of apply_optim_delete but starts from a
 * primary-key value instead of a stored TupleTableSlot.
 */
void
apply_deferred_delete_by_key(Oid relOid, int keyval)
{
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
        int partitionId;
    } PendingMerkleDelete;
    PendingMerkleDelete *pending = NULL;

    oldSlot = table_slot_create(relation, NULL);

    /* Btree lookup by primary key using SnapshotSelf */
    {
        List *btreeIndexList = RelationGetIndexList(relation);
        ListCell *blc;

        foreach(blc, btreeIndexList)
        {
            Oid btreeOid = lfirst_oid(blc);
            Relation btreeRel = index_open(btreeOid, AccessShareLock);

            if (btreeRel->rd_rel->relam != MERKLE_AM_OID &&
                btreeRel->rd_index->indisunique &&
                btreeRel->rd_index->indnkeyatts >= 1)
            {
                IndexScanDesc iscan;
                ScanKeyData skey[1];

                ScanKeyInit(&skey[0],
                            1,
                            BTEqualStrategyNumber,
                            F_INT4EQ,
                            Int32GetDatum(keyval));

                iscan = index_beginscan(relation, btreeRel,
                                        SnapshotSelf, 1, 0);
                index_rescan(iscan, skey, 1, NULL, 0);

                if (index_getnext_slot(iscan, ForwardScanDirection, oldSlot))
                {
                    ItemPointerCopy(&oldSlot->tts_tid, &currentTid);
                    found = true;
                }

                index_endscan(iscan);
            }

            index_close(btreeRel, AccessShareLock);
            if (found)
                break;
        }

        list_free(btreeIndexList);
    }

    if (!found)
    {
        /*
         * Row genuinely doesn't exist (e.g., first DELETE of this key
         * that already committed and the INSERT hasn't come yet).
         * Nothing to XOR-out, nothing to delete. This is normal.
         */
        if (oldSlot)
            ExecDropSingleTupleTableSlot(oldSlot);
        RelationClose(relation);
        return;
    }

    /* Use merkle_compute_row_hash to get the old hash before deletion */
    merkle_compute_row_hash(relation, &currentTid, &oldHash);
    hasOldHash = !merkle_hash_is_zero(&oldHash);

    /* XOR-out from all Merkle indexes */
    if (hasOldHash)
    {
        indexList = RelationGetIndexList(relation);
        pendingCapacity = list_length(indexList);
        if (pendingCapacity > 0)
            pending = palloc0(sizeof(PendingMerkleDelete) * pendingCapacity);

        foreach(lc, indexList)
        {
            Oid indexOid = lfirst_oid(lc);
            Relation indexRel = index_open(indexOid, RowExclusiveLock);

            if (indexRel->rd_rel->relam == MERKLE_AM_OID)
            {
                IndexInfo  *indexInfo;
                Datum       values[INDEX_MAX_KEYS];
                bool        isnull[INDEX_MAX_KEYS];
                int         totalLeaves;

                indexInfo = BuildIndexInfo(indexRel);
                FormIndexDatum(indexInfo, oldSlot, NULL, values, isnull);
                merkle_read_meta(indexRel, NULL, NULL, NULL, NULL, &totalLeaves, NULL, NULL, NULL);

                pending[pendingCount].indexOid = indexOid;
                pending[pendingCount].partitionId =
                    merkle_compute_partition_id(values, isnull,
                                                indexInfo->ii_NumIndexKeyAttrs,
                                                RelationGetDescr(indexRel),
                                                totalLeaves);

                pendingCount++;
            }

            index_close(indexRel, RowExclusiveLock);
        }
    }

    /* Heap delete */
    result = table_tuple_delete(relation, &currentTid,
                                GetCurrentCommandId(true),
                                InvalidSnapshot,
                                InvalidSnapshot,
                                false,
                                &tmfd,
                                false);

    /* Apply Merkle XOR-outs after successful heap delete */
    if (result == TM_Ok && hasOldHash)
    {
        for (int i = 0; i < pendingCount; i++)
        {
            Relation indexRel = index_open(pending[i].indexOid, RowExclusiveLock);
            if (indexRel->rd_rel->relam == MERKLE_AM_OID)
                merkle_update_tree_path(indexRel, pending[i].partitionId, &oldHash, false);
            index_close(indexRel, RowExclusiveLock);
        }
    }

    if (oldSlot)
        ExecDropSingleTupleTableSlot(oldSlot);
    if (indexList)
        list_free(indexList);
    if (pending)
        pfree(pending);

    RelationClose(relation);
}bool
apply_optim_writes(void)
{
    OptimWriteEntry *write_entry;

    while ((write_entry = SIMPLEQ_FIRST(&activeTx->optim_write_list)))
    {
        switch (write_entry->operation)
        {
            case CMD_UPDATE:
                apply_optim_update(&write_entry->old_tid, write_entry->slot, write_entry->cid);
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
                     * whole transaction with a fresh snapshot.
                     */
                    ExecDropSingleTupleTableSlot(write_entry->slot);
                    SIMPLEQ_REMOVE_HEAD(&activeTx->optim_write_list, link);
                    return false;
                }
                ExecDropSingleTupleTableSlot(write_entry->slot);
                break;
            case CMD_DELETE:
                if (ItemPointerIsValid(&write_entry->old_tid))
                {
                    apply_optim_delete(write_entry->relOid, &write_entry->old_tid,
                                       write_entry->slot, write_entry->cid);
                }
                else
                {
                    apply_deferred_delete_by_key(write_entry->relOid, write_entry->keyval);
                }
                if (write_entry->slot)
                    ExecDropSingleTupleTableSlot(write_entry->slot);
                break;
            default:
                ereport(ERROR, (errmsg("[ZL] tx %s applying unknown operation", activeTx->hash)));
        }
        SIMPLEQ_REMOVE_HEAD(&activeTx->optim_write_list, link);
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
int
conflict_checkDT()
{
const int ccMax = 1;
static int ccCount = 0;
static int cc2Count = 0;

    WSTableEntryRecord *record;
    
    if(ccCount++ == ccMax) {
	ccCount = 0;
#if SAFEDBG2
	printf("safeDB %s : %s: %d -- one in 20\n", __FILE__, __FUNCTION__, __LINE__ );
#endif
    }

    LIST_FOREACH(record, &ws_table_record, link)
    {
        // ws_table_check
        if (ws_table_checkDT( &record->tag)) {
	    // printf("safeDB %s : %s: %d tx %s %d conflict due to waw \n", 
		// __FILE__, __FUNCTION__, __LINE__ ,  activeTx->hash, activeTx->tx_id);
	    return 1;
	}
    		//ereport(ERROR,
 		//		(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
   		//		 errmsg("tx %s aborted due to waw", activeTx->hash)));
    }

    LIST_FOREACH(record, &rs_table_record, link)
    {
        //ws_table_check
        if (ws_table_checkDT( &record->tag))
	    return 1;
    		//ereport(ERROR,
 				//(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
   				 //errmsg("tx %s aborted due to raw", activeTx->hash)));
    }

    if(cc2Count++ == ccMax) {
	cc2Count = 0;
#if SAFEDBG2
	printf("safeDB %s : %s: %d -- one in 20\n", __FILE__, __FUNCTION__, __LINE__ );
#endif
    }
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
void
conflict_check(void)
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
 * For each write-set record, HASH_ENTER stores entry->tx_id = min(existing,
 * activeTx->tx_id) so that the earliest ordering tx_id "wins" the slot.
 *
 * Called from worker.c after conflict_checkDT() returns clean.
 */
void
publish_ws_tableDT(int id)
{
    bool found;
    WSTableEntry* entry;
    PREDICATELOCKTARGETTAG *tag;
    uint32  tuple_hash = 0;
    WSTableEntryRecord *record;
    slock_t *partition_lock;
    int x = 0;
    
    if( HASHTAB_SWITCH_THRESHOLD <  2 * NUM_WORKERS - 1) {
        ereport(ERROR,
                (errmsg("HASHTAB_SWITCH_THRESHOLD (%d) must be >= %d",
                        HASHTAB_SWITCH_THRESHOLD, 2 * NUM_WORKERS - 1)));
    }

    x = id  / HASHTAB_SWITCH_THRESHOLD; // min 2* num_w -1
    if(x % 2 == 0) {
	    ws_table->mapActive = ws_table->map;
	    if(id % HASHTAB_SWITCH_THRESHOLD == 0 ) {
		    shm_hash_clear(ws_table->map, MAX_WRITE_CONFLICT);
		    //shm_hash_clear(rs_table->map, MAX_WRITE_CONFLICT);
	    }
    }
    else {
	    ws_table->mapActive = ws_table->mapB;
	    if(id % HASHTAB_SWITCH_THRESHOLD == 0 ) {
		    shm_hash_clear(ws_table->mapB, MAX_WRITE_CONFLICT);
		    // shm_hash_clear(rs_table->mapB, MAX_WRITE_CONFLICT);
	    }
    } // clean_rs_ws_table(id); // reset before HASH_ENTER get-write-set !!!

    LIST_FOREACH(record, &ws_table_record, link)
    {
	    tag = &(record->tag);
	    tuple_hash = PredicateLockTargetTagHashCode(tag);
        partition_lock = WSTablePartitionLock(tuple_hash);
        SpinLockAcquire(partition_lock);
        entry = (WSTableEntry *) hash_search_with_hash_value(ws_table->mapActive,
                                                                                 tag,
                                                                                 tuple_hash,
                                                                                 HASH_ENTER,
                                                                                 &found);
        if (!found || entry->tx_id > activeTx->tx_id)
            entry->tx_id = activeTx->tx_id;
        SpinLockRelease(partition_lock);
    }
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
void
clean_rs_ws_table(void)
{
    shm_hash_clear(ws_table->map, MAX_WRITE_CONFLICT);
    shm_hash_clear(rs_table->map, MAX_WRITE_CONFLICT);
}
