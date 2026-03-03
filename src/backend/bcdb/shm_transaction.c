#include "postgres.h"
#include "bcdb/shm_transaction.h"
#include "bcdb/utils/aligned_heap.h"
#include "utils/elog.h"
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
#include <bcdb/worker.h>
#include "bcdb/bcdb_dsa.h"
#include "utils/hsearch.h"
#include <stddef.h>
#include <access/genam.h>
#include "access/xact.h"
#include "access/heapam.h"
#include "storage/bufmgr.h"
#include "storage/predicate.h"
#include "utils/hashutils.h"
#include "access/itup.h"
#include "utils/hsearch.h"
#include "bcdb/worker_controller.h"
#include "storage/spin.h"
#include "storage/predicate_internals.h"
#include <time.h>
#include <stdio.h>

// BCDBShmXact  *mapTxToShm[TX_MAP_SZ];
//#define MAP_TX(id) ((id) % TX_MAP_SZ)
//slock_t      *nexec_lock;
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

    MemSet(&info, 0, sizeof(info));
    Assert(tx_pool != NULL);
    //printf("safeDB %s : %s: %d txid %d hash %s \n", __FILE__, __FUNCTION__, __LINE__ , tx_id, hash);
    SpinLockAcquire(tx_pool_lock);
    tx = hash_search(tx_pool, hash, HASH_ENTER, &found);
    if (found)
    {
        printf("safeDB %s : %s: %d \n", __FILE__, __FUNCTION__, __LINE__ );
        ereport(DEBUG3,
            (errmsg("[ZL] transaction (%s) exists", hash)));
        SpinLockRelease(tx_pool_lock);
        return NULL;
    }
    LWLockInitialize(&tx->lock, LWTRANCHE_TX);
    LWLockAcquire(&tx->lock, LW_EXCLUSIVE);
    SpinLockRelease(tx_pool_lock);

    strcpy(tx->hash, hash);
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

void
compose_tx_hash(BCBlockID bid, BCTxID tx_id, char* out_hash)
{
    sprintf(out_hash, "%d_%d", bid, tx_id);
}

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

uint32
compose_tuple_hash(Oid relation_id, ItemPointer tid)
{
    return hash_uint32((uint32)relation_id) + hash_any((unsigned char*)tid, sizeof(ItemPointerData));
}

uint32
compose_index_hash(Oid relation_id, IndexTuple itup)
{
    return hash_uint32((uint32)relation_id) + 
           hash_any((unsigned char*)itup + IndexInfoFindDataOffset(itup->t_info),
                    IndexTupleSize(itup) - IndexInfoFindDataOffset(itup->t_info));
}

void
create_tx_pool(void)
{
    HASHCTL info;
    slock_t *tx_pool_lock_array;
    bool    found;

    restart_counter_lock = ShmemInitStruct("restart_counter_lock", sizeof(slock_t) , &found);
    //nexec_lock = ShmemInitStruct("nexec_lock", sizeof(slock_t) , &found);
    printf("\n\t $$$   safeDB %s : %s: %d \n", __FILE__, __FUNCTION__, __LINE__ );
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
	printf("\t safeDbg pid %d %s : %s: %d \n\n", getpid(),__FILE__, __FUNCTION__, __LINE__ );
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
	printf("safeDB %s : %s: %d found= %d\n", __FILE__, __FUNCTION__, __LINE__ , found);

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
    printf("safeDB %s : %s: %d \n", __FILE__, __FUNCTION__, __LINE__ );
    shm_hash_clear(tx_pool, MAX_SHM_TX);
    shm_hash_clear(xid_map, MAX_SHM_TX); 
    for (int i = 0; i < NUM_TX_QUEUE_PARTITION; i++)
        TAILQ_INIT(&tx_queues[i].list);
}

void
rs_table_reserveDT(const PREDICATELOCKTARGETTAG *tag)
{
    uint32  tuple_hash = PredicateLockTargetTagHashCode(tag);
    WSTableEntryRecord *record;

    //printf("safeDB %s : %s: %d txhash %s tx %d tuphash %d\n", __FILE__,
    //        __FUNCTION__, __LINE__ , activeTx->hash , activeTx->tx_id, tuple_hash);

    record= MemoryContextAlloc(bcdb_tx_context, sizeof(WSTableEntryRecord));
    record->tag = *tag;
    LIST_INSERT_HEAD(&rs_table_record, record, link);
}

void
rs_table_reserve(const PREDICATELOCKTARGETTAG *tag)
{
    bool found;
    WSTableEntry* entry;
    uint32  tuple_hash = PredicateLockTargetTagHashCode(tag);
    slock_t *partition_lock = RSTablePartitionLock(tuple_hash);
    WSTableEntryRecord *record;

    SpinLockAcquire(partition_lock);

    printf("\nsafeDB %s : %s: %d \n", __FILE__, __FUNCTION__, __LINE__ );
    entry = hash_search_with_hash_value(rs_table->map, tag, tuple_hash, HASH_ENTER, &found);
    if (!found)
    {
	printf("ariaDB [ZL] tx %s reserving read %d success, because not found\n", activeTx->hash, tuple_hash);
        entry->tx_id = activeTx->tx_id;
        DEBUGMSG("[ZL] tx %s reserving read %d success, because not found", activeTx->hash, tuple_hash);
    }
    else
    {
	printf("\nariaDB %s : %s: %d found %d\n", __FILE__, __FUNCTION__, __LINE__ , found);
        if (entry->tx_id > activeTx->tx_id)
        {
            DEBUGMSG("[ZL] tx %s reserving read %d success, replacing %d", activeTx->hash, tuple_hash, entry->tx_id);
            entry->tx_id = activeTx->tx_id;
            printf("ariaDB tx %s reserv read %d success, replacing %d\n",
                    activeTx->hash, tuple_hash, entry->tx_id);
        }
    }
    SpinLockRelease(partition_lock);

    record= MemoryContextAlloc(bcdb_tx_context, sizeof(WSTableEntryRecord));
    record->tag = *tag;
    LIST_INSERT_HEAD(&rs_table_record, record, link);
}

void
ws_table_reserveDT(PREDICATELOCKTARGETTAG *tag)
{
    WSTableEntryRecord *record;

    //printf("safeDB %s : %s: %d txid= %d \n",
      //     __FILE__, __FUNCTION__, __LINE__, activeTx->tx_id );

    record= MemoryContextAlloc(bcdb_tx_context, sizeof(WSTableEntryRecord));
    record->tag = *tag;
    LIST_INSERT_HEAD(&ws_table_record, record, link);
}

void
ws_table_reserve(PREDICATELOCKTARGETTAG *tag)
{
    bool found;
    WSTableEntry* entry;
    uint32  tuple_hash = PredicateLockTargetTagHashCode(tag);
    slock_t *partition_lock = WSTablePartitionLock(tuple_hash);
    WSTableEntryRecord *record;

    printf("ariaDB %s : %s: %d \n", __FILE__, __FUNCTION__, __LINE__ );

    SpinLockAcquire(partition_lock);
    entry = hash_search_with_hash_value(ws_table->map, tag, tuple_hash, HASH_ENTER, &found);
    if (!found)
    {
        entry->tx_id = activeTx->tx_id;
        DEBUGMSG("[ZL] tx %s reserving write %d success, because not found", activeTx->hash, tuple_hash);
	printf("ariaDB [ZL] tx %s reserving write %d success, because not found\n", activeTx->hash, tuple_hash);
    }
    else
    {
        if (entry->tx_id > activeTx->tx_id)
        {
            DEBUGMSG("[ZL] tx %s reserving write %d success, replacing %d", activeTx->hash, tuple_hash, entry->tx_id);
            entry->tx_id = activeTx->tx_id;
            printf("ariaDB tx %s reserv write %d success, replacing %d\n",
                   activeTx->hash, tuple_hash, entry->tx_id);
        }
    }

    if (entry->tx_id != activeTx->tx_id)
    {
	printf("ariaDB tx %s reserving write %d fail: winner %d\n",
               activeTx->hash, tuple_hash, entry->tx_id);
        DEBUGMSG("[ZL] tx %s reserving write %d fail: winner %d", activeTx->hash, tuple_hash, entry->tx_id);
    }
    SpinLockRelease(partition_lock);

    record= MemoryContextAlloc(bcdb_tx_context, sizeof(WSTableEntryRecord));
    record->tag = *tag;
    LIST_INSERT_HEAD(&ws_table_record, record, link);
}

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
	// TODO assert ?? activeTx-> last committed tx id <= entry->tx_id
        //DEBUGMSG("safeDB tx %s check write %d failed, winner: %d",
        printf("safeDB tx %d hash %s check write %d failed, winner: %d\n",
		activeTx->tx_id, activeTx->hash, tuple_hash, entry->tx_id);
        SpinLockRelease(partition_lock);
        return true;
    }

    entry = hash_search_with_hash_value(table->mapB, tag,
							tuple_hash, HASH_FIND, &found);
    if (found && (entry->tx_id < activeTx->tx_id) &&
				(entry->tx_id >  activeTx->tx_id_committed))
    {
        //DEBUGMSG("safeDB tx %s check write %d failed, winner: %d",
        printf("safeDB tx %d hash %s check write %d failed, winner: %d\n",
		activeTx->tx_id, activeTx->hash, tuple_hash, entry->tx_id);
        SpinLockRelease(partition_lock);
        return true;
    }
    SpinLockRelease(partition_lock);
    DEBUGMSG("safeDB tx %s check write %d win", activeTx->hash, tuple_hash);
    return false;
}

bool
ws_table_checkDT(PREDICATELOCKTARGETTAG *tag)
{
    return table_checkDT(tag, ws_table);
}

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
        DEBUGMSG("[ZL] tx %s check write %d failed, winner: %d", activeTx->hash, tuple_hash, entry->tx_id);
        SpinLockRelease(partition_lock);
        return true;
    }
    DEBUGMSG("[ZL] tx %s check write %d win", activeTx->hash, tuple_hash);
    SpinLockRelease(partition_lock);
    return false;
}

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
        DEBUGMSG("[ZL] tx %s check read %d failed, winner: %d", activeTx->hash, tuple_hash, entry->tx_id);
        SpinLockRelease(partition_lock);
        return true;
    }
    DEBUGMSG("[ZL] tx %s check read %d win", activeTx->hash, tuple_hash);
    SpinLockRelease(partition_lock);
    return false;
}

void
clean_ws_table_record(void)
{
    WSTableEntryRecord *record;
    while ((record = LIST_FIRST(&ws_table_record)))
    {
        bool found;
        WSTableEntry* entry;
        uint32  tuple_hash = PredicateLockTargetTagHashCode(&record->tag);
        slock_t *partition_lock = WSTablePartitionLock(tuple_hash);

        SpinLockAcquire(partition_lock);
        entry = hash_search_with_hash_value(ws_table->map, &record->tag, tuple_hash, HASH_FIND, &found);
        if (found)
        {
            if (entry->tx_id == activeTx->tx_id)
            {
                DEBUGMSG("[ZL] tx %s deleting write entry %d", activeTx->hash, tuple_hash);
                hash_search_with_hash_value(ws_table->map, &record->tag, tuple_hash, HASH_REMOVE, &found);
            }
        }
        SpinLockRelease(partition_lock);
        LIST_REMOVE(record, link);
    }
}

void
clean_rs_table_record(void)
{
    WSTableEntryRecord *record;
    while ((record = LIST_FIRST(&rs_table_record)))
    {
        bool found;
        WSTableEntry* entry;
        uint32  tuple_hash = PredicateLockTargetTagHashCode(&record->tag);
        slock_t *partition_lock = RSTablePartitionLock(tuple_hash);

        SpinLockAcquire(partition_lock);
        entry = hash_search_with_hash_value(rs_table->map, &record->tag, tuple_hash, HASH_FIND, &found);
        if (found)
        {
            if (entry->tx_id == activeTx->tx_id)
            {
                DEBUGMSG("[ZL] tx %s deleting read entry %d", activeTx->hash, tuple_hash);
                hash_search_with_hash_value(rs_table->map, &record->tag, tuple_hash, HASH_REMOVE, &found);
            }
        }
        SpinLockRelease(partition_lock);
        LIST_REMOVE(record, link);
    }
}
void
tx_queue_insert(BCDBShmXact *tx, int32 partition)
{
    struct timeval tv1;
    tv1.tv_sec = 0; tv1.tv_usec = 0;
    bool    found;

    int num_queue = OEP_mode ? blocksize * 2 : blocksize;
    num_queue = get_blksz(); // get_block_by_id(1, false)->blksize; ==nWorker

#if SAFEDBG
#endif
	printf("safeDB %s : %s: %d partition %d num_queue %d txsql %s \n", __FILE__, __FUNCTION__, __LINE__ , partition, num_queue, tx->sql);
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
    ret = MakeTupleTableSlot(slot->tts_tupleDescriptor, slot->tts_ops);
    ExecCopySlot(ret, slot);
    ret->tts_tableOid = slot->tts_tableOid;
    ret->tts_tupleDescriptor = CreateTupleDescCopy(slot->tts_tupleDescriptor);
    return ret;
}

BCDBShmXact*
get_tx_by_hash(const char *hash)
{
    BCDBShmXact *ret;
    SpinLockAcquire(tx_pool_lock);
    ret = hash_search(tx_pool, hash, HASH_FIND, NULL);
    SpinLockRelease(tx_pool_lock);
    return ret;
}

BCDBShmXact*
get_tx_by_xid(TransactionId xid)
{
    XidMapEntry *ret;

    SpinLockAcquire(xid_map_lock);
    ret = hash_search(xid_map, &xid, HASH_FIND, NULL);
    SpinLockRelease(xid_map_lock);
    if (!ret)
    {
        ereport(FATAL, (errmsg("[ZL] no conflict xid")));
    }
    return ret->tx;
}

BCDBShmXact*
get_tx_by_xid_locked(TransactionId xid, bool exclusive)
{
    XidMapEntry *ret;
    LWLockMode lockmode = exclusive ? LW_EXCLUSIVE : LW_SHARED;
    SpinLockAcquire(xid_map_lock);
    ret = hash_search(xid_map, &xid, HASH_FIND, NULL);
    if (!ret)
    {
        ereport(FATAL, (errmsg("[ZL] no conflict xid")));
    }
    LWLockAcquire(&ret->tx->lock, lockmode);
    SpinLockRelease(xid_map_lock);
    return ret->tx;
}

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
    write_entry = palloc(sizeof(WriteTuple));
    write_entry->operation = CMD_UPDATE;
    write_entry->old_tid = *old_tid;
    write_entry->slot = clone_slot(slot);
    write_entry->cid = GetCurrentCommandId(true);
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
    write_entry = palloc(sizeof(WriteTuple));
    write_entry->operation = CMD_INSERT;
    write_entry->slot = clone_slot(slot);
    write_entry->cid = GetCurrentCommandId(true);
    SIMPLEQ_INSERT_TAIL(&activeTx->optim_write_list, write_entry, link);
    MemoryContextSwitchTo(old_context);
}

void
apply_optim_insert(TupleTableSlot* slot, CommandId cid)
{
    Relation relation = RelationIdGetRelation(slot->tts_tableOid);

    DEBUGMSG("[ZL] tx %s applying optim insert (rel: %d)", activeTx->hash, relation->rd_id);
    table_tuple_insert(relation, slot, cid, 0, NULL);

    heap_apply_index(relation, slot, true, true);
    RelationClose(relation);
}

void
apply_optim_update(ItemPointer tid, TupleTableSlot* slot, CommandId cid)
{
    TM_FailureData tmfd;
    TM_Result result;
    LockTupleMode lockmode;
    bool update_indexes;
    Relation relation = RelationIdGetRelation(slot->tts_tableOid);

    DEBUGMSG("[ZL] tx %s applying optim update (%d %d %d)", activeTx->hash, relation->rd_id, *(int*)&tid->ip_blkid, (int)tid->ip_posid);
#if SAFEDBG1
    printf("safeDB %s : %s: %d tm-ok %d tx %d cid %d\n",
            __FILE__, __FUNCTION__, __LINE__ , TM_Ok, activeTx->tx_id, cid );
#endif
    result = table_tuple_update(relation, tid, slot,
                       cid,
						//GetTransactionSnapshot(),
                       InvalidSnapshot,
                       InvalidSnapshot,
                       false, /* do not wait for commit */
                       &tmfd, &lockmode, &update_indexes);

    if (result != TM_Ok)
    {
    RelationClose(relation);
#if SAFEDBG1
        printf("safeDB %s : %s: %d ret %d tx %d tx %s doomed because of ww-conflict \n",
               __FILE__, __FUNCTION__, __LINE__ , result, activeTx->tx_id, activeTx->hash );
        printf("safeDB %s : %s: %d   tmfd.xmax %d, tmfd.cmax %d  ww-conflict \n",
               __FILE__, __FUNCTION__, __LINE__ , tmfd.xmax, tmfd.cmax  );
#endif
	// heap_update()
/*
		ereport(ERROR,
				(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
    			 errmsg("tx %s doomed because of ww-conflict", activeTx->hash)));
bcdb_worker_process_tx
*/
    }


    else if (update_indexes)
        heap_apply_index(relation, slot, false, false);

    ExecClearTuple(slot);
                slot->tts_ops->release(slot);
                if (slot->tts_tupleDescriptor)
                {
                        ReleaseTupleDesc(slot->tts_tupleDescriptor);
                        slot->tts_tupleDescriptor = NULL;
                }
                pfree(slot);

    RelationClose(relation);
}

void
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
                apply_optim_insert(write_entry->slot, write_entry->cid);
                break;
            default:
                ereport(ERROR, (errmsg("[ZL] tx %s applying unknown operation", activeTx->hash)));
        }
        SIMPLEQ_REMOVE_HEAD(&activeTx->optim_write_list, link);
    }
}

bool
check_stale_read(void)
{
    RWConflict       conflict;
    LWLockAcquire(SerializableXactHashLock, LW_SHARED);
    conflict = (RWConflict)
		SHMQueueNext(&activeTx->sxact->outConflicts,
					 &activeTx->sxact->outConflicts,
					 offsetof(RWConflictData, outLink));
	while (conflict)
	{
		if (BCDB_TX(conflict->sxactIn)->block_id_committed < activeTx->block_id_committed)
    		ereport(ERROR,
    				(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
    				 errmsg("tx %s stale read found, %s update first", activeTx->hash, BCDB_TX(conflict->sxactIn)->hash)));

		conflict = (RWConflict)
			SHMQueueNext(&activeTx->sxact->outConflicts,
						 &conflict->outLink,
						 offsetof(RWConflictData, outLink));
	}
    LWLockRelease(SerializableXactHashLock);
    return true;
}

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
        if (ws_table_checkDT( &record->tag))
	printf("safeDB %s : %s: %d tx %s %d conflict due to waw \n", 
		__FILE__, __FUNCTION__, __LINE__ ,  activeTx->hash, activeTx->tx_id);
	    return 1;
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

void
conflict_check(void)
{
int numRec = 0;
const int ccMax = 20;
static int ccCount = 0;
static int cc2Count = 0;

    WSTableEntryRecord *record;
    
    if(ccCount++ == ccMax) {
	ccCount = 0;
	printf("ariaDB %s : %s: %d -- one in 20\n", __FILE__, __FUNCTION__, __LINE__ );
    }

    LIST_FOREACH(record, &ws_table_record, link)
    {
        numRec++;
        if (ws_table_check(&record->tag))
    		ereport(ERROR,
 				(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
   				 errmsg("tx %s aborted due to waw", activeTx->hash)));           
	printf("ariaDB %s : %s: %d -- ws_table_record numRec= %d \n",
			__FILE__, __FUNCTION__, __LINE__, numRec );
    }

    if(cc2Count++ == ccMax) {
	cc2Count = 0;
	printf("ariaDB %s : %s: %d -- one in 20\n",
			__FILE__, __FUNCTION__, __LINE__ );
    }

    LIST_FOREACH(record, &ws_table_record, link)
    {
        numRec = 0;
        if (rs_table_check(&record->tag))
        {
            WSTableEntryRecord *raw_record;
            LIST_FOREACH(raw_record, &rs_table_record, link)
            {
                numRec++;
                if (ws_table_check(&raw_record->tag))
            		ereport(ERROR,
         				(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
           				 errmsg("tx %s aborted due to raw and war", activeTx->hash)));
            }
	  printf("ariaDB %s : %s: %d -- rs_table_record numRec= %d \n",
			__FILE__, __FUNCTION__, __LINE__, numRec );
			break;
        }
    }

}

void
publish_ws_tableDT(int id)
{
    bool found;
    // WSTableEntry* entry;
    PREDICATELOCKTARGETTAG *tag;
    uint32  tuple_hash = 0;
    WSTableEntryRecord *record;
    int x = 0;
    
    if( HASHTAB_SWITCH_THRESHOLD <  2 * NUM_WORKERS - 1) {
	printf("safeDB ** ERROR %s : %s: %d increase threshold ** \n",
               __FILE__, __FUNCTION__, __LINE__ );
	return;
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
	    hash_search_with_hash_value(ws_table->mapActive, tag, tuple_hash,
									HASH_ENTER, &found);
    }
}

void
clean_rs_ws_table(void)
{
    shm_hash_clear(ws_table->map, MAX_WRITE_CONFLICT);
    shm_hash_clear(rs_table->map, MAX_WRITE_CONFLICT);
}
