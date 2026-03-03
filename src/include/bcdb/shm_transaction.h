#ifndef BCDB_SHM_TRANSACTION_H
#define BCDB_SHM_TRANSACTION_H

/*
 * Transaction structure in shared memrory
 */
#include "postgres.h"
#include <access/xact.h>
#include <executor/tuptable.h>
#include "executor/execdesc.h"
#include <utils/relcache.h>
#include <nodes/execnodes.h>
#include "storage/lwlock.h"
#include "nodes/execnodes.h"
#include "executor/tuptable.h"
#include "bcdb/utils/sys-queue.h"
#include "storage/predicate_internals.h"
#include "utils/portal.h"
#include "lib/dshash.h"
#include "bcdb/globals.h"
#include "storage/condition_variable.h"
#include "storage/predicate_internals.h"
#include "utils/hsearch.h"
#include <sys/types.h>
#include <semaphore.h>
#include "storage/spin.h"
#include "openssl/sha.h"
#include "access/merkle.h"

typedef enum 
{
    TX_SCHEDULING,
    TX_WAITING,
    TX_EXECUTING,
    TX_WAIT_FOR_BLOCK,
    TX_EXEC_WRITE_SET,
    TX_WAIT_FOR_COMMIT,
    TX_COMMITTING,
    TX_COMMITED,
    TX_ABORTED,
    TX_ABORTING,
    TX_RETRYING,
} TxStatus;

typedef struct _WriteTuple{

    Oid relationOid;

//    indicate whether it is INSERT,UPDATE,DELETE
    CmdType operation;

//    For update and delete
    ItemPointerData tupleid;

//    For udpate and insert
    TupleTableSlot* slot;

//    Linked list to point nex WriteTuple
    LIST_ENTRY(_WriteTuple) list_entry;

} WriteTuple;

typedef struct _ReadTuple{
    HeapTuple tuple;
    LIST_ENTRY(_ReadTuple) list_entry;
} ReadTuple;

typedef struct _WriteEntry
{
    CmdType         operation;
    ItemPointerData old_tid;
    HeapTuple       newtup;
    bool            hot_attr_updated;
    bool            keys_updated;
    bool            cid_is_combo;
    CommandId       cid;
    SIMPLEQ_ENTRY(_WriteEntry) link;
} WriteEntry;

typedef struct _OptimWriteEntry
{
    CmdType         operation;
    TupleTableSlot  *slot;
    ItemPointerData old_tid;
    Oid             relOid;     /* relation OID, used for CMD_DELETE */
    int32           keyval;     /* primary key value for deferred DELETE-0 reexec */
    CommandId       cid;
    SIMPLEQ_ENTRY(_OptimWriteEntry) link;
} OptimWriteEntry;

#define      TX_MAP_SZ  ((2 * NUM_WORKERS) - 1)
typedef struct _WSTable
{
    /* partition the available list and HTAB to avoid contention */
    HTAB               *map;
    HTAB               *mapB;
    HTAB               *mapActive;
    slock_t             map_locks[WRITE_CONFLICT_MAP_NUM_PARTITIONS];
} WSTable;

/* to do: move non-shared element out */
typedef struct _BCDBShmXact
{
    /* hash servers as a unique ID accross the blocks */
    char               hash[TX_HASH_SIZE];
    BCTxID             tx_id;
    BCTxID             tx_id_committed;

    WSTable       *ws_table;
    WSTable       *rs_table;

    BCBlockID          block_id_snapshot;
    BCBlockID volatile block_id_committed;
    char               sql[1024];

    TxStatus volatile  status;
    QueryDesc          *queryDesc;
    Portal             portal;

    int                isolation;
    bool               pred_lock;

    SIMPLEQ_ENTRY(_BCDBShmXact)              link;
    TAILQ_ENTRY(_BCDBShmXact)                queue_link;
    int32                                    queue_partition;
    SIMPLEQ_HEAD(_OptimWriteList, _OptimWriteEntry) optim_write_list;
    SERIALIZABLEXACT         *sxact;
    ConditionVariable         cond;
    LWLock                    lock;
    TransactionId             xid;
    pid_t                     worker_pid;
    /* void* is a dirty trick to avoid including frontend headers */
    void*           worker;
    char            why_doomed[258];
    SHA256_CTX      state_hash;
    bool            has_war;
    bool            has_raw;

    uint64          create_time;
    uint64          start_simulation_time;
    uint64          start_parsing_time;
    uint64          start_checking_time;
    uint64          end_checking_time;
    uint64          end_parsing_time;
    uint64          end_simulation_time;
    uint64          start_local_copy_time;
    uint64          end_local_copy_time;
    uint64          commit_time;
} BCDBShmXact;

typedef struct _TxQueue
{
    TAILQ_HEAD(_TxAttachedQueueHead, _BCDBShmXact) list;
    slock_t                                   lock;
    ConditionVariable                        empty_cond;
    ConditionVariable                        full_cond;
    int32 volatile                           size;
} TxQueue;

/*typedef struct _TxResult
{
    ConditionVariable                        done_cond;
    int32 volatile                           txid;
    char result[1024];
} TxResult;
*/

typedef struct _XidMapEntry
{
    TransactionId    xid;
    BCDBShmXact     *tx;
} XidMapEntry;

typedef struct _WSTableEntry
{
    PREDICATELOCKTARGETTAG tag;
    BCTxID  tx_id;
} WSTableEntry;

typedef struct _WSTableEntryRecord
{
    PREDICATELOCKTARGETTAG tag;
    LIST_ENTRY(_WSTableEntryRecord) link;
} WSTableEntryRecord;

typedef LIST_HEAD(_WSTableRecord, _WSTableEntryRecord) WSTableRecord;

/*
 * Merkle Change Set structures for batched Merkle updates
 */
typedef struct _PendingMerkleUpdate
{
    Oid         indexOid;
    int         partitionId;
    MerkleHash  hash;
    bool        is_insert;  /* true = XOR in, false = XOR out */
    LIST_ENTRY(_PendingMerkleUpdate) link;
} PendingMerkleUpdate;

typedef LIST_HEAD(_MerkleChangeSet, _PendingMerkleUpdate) MerkleChangeSet;

extern BCDBShmXact  *activeTx;
extern slock_t      *restart_counter_lock;

extern HTAB         *tx_pool;
extern TxQueue      *tx_queues;
extern WSTableRecord ws_table_record;
extern WSTableRecord rs_table_record;

extern BCDBShmXact* tx_queue_next(int32 partition);
extern void         tx_queue_insert(BCDBShmXact *tx, int32 partition);

extern void         create_tx_pool(void);
extern void         clear_tx_pool(void);
extern Size         tx_pool_size(void);
extern BCDBShmXact* get_tx_by_hash(const char *hash);
/* get_tx_by_xid and get_tx_by_xid_locked removed — no callers; see shm_transaction.c */
extern void         add_tx_xid_map(TransactionId id, BCDBShmXact *tx);
extern void         remove_tx_xid_map(TransactionId id);
extern BCDBShmXact* create_tx(char *hash, char *sql, BCTxID tx_id, BCBlockID snapshot_block, int isolation, bool pred_lock);
extern void         delete_tx(BCDBShmXact* tx);

/* compose_tuple_hash, compose_index_hash, compose_tx_hash removed — no callers */

extern uint32 dummy_hash(const void *key, Size key_size);
extern void store_optim_update(TupleTableSlot* slot, ItemPointer old_tid);
extern void store_optim_insert(TupleTableSlot* slot);
extern void store_optim_delete(Oid relOid, ItemPointer tupleid, TupleTableSlot *slot);
extern void store_optim_delete_by_key(Oid relOid, int32 keyval, CommandId cid);
extern void apply_optim_update(ItemPointer tid, TupleTableSlot* slot, CommandId cid);
extern bool apply_optim_insert(TupleTableSlot* slot, CommandId cid);
extern void apply_optim_delete(Oid relOid, ItemPointer tupleid, TupleTableSlot *storedSlot, CommandId cid);
extern void apply_deferred_delete_by_key(Oid relOid, int keyval);
extern bool apply_optim_writes(void);
/* check_stale_read removed — SSI stale-read check that was never called; see shm_transaction.c */
/* clean_ws_table_record, clean_rs_table_record removed — non-DT per-entry cleanup with no callers;
 * use clean_rs_ws_table() (bulk clear) instead */
extern bool rs_table_check(PREDICATELOCKTARGETTAG *tag);
extern void clean_rs_ws_table(void);

/* ws_table_reserve, rs_table_reserve removed — non-DT variants with no callers;
 * use ws_table_reserveDT / rs_table_reserveDT instead */
extern bool ws_table_check(PREDICATELOCKTARGETTAG *tag);
extern void conflict_check(void);

extern void rs_table_reserveDT( const PREDICATELOCKTARGETTAG *tag);
extern void ws_table_reserveDT( PREDICATELOCKTARGETTAG *tag);
extern bool ws_table_checkDT(PREDICATELOCKTARGETTAG *tag);
extern int conflict_checkDT(void);
extern void publish_ws_tableDT(int id);

/* Merkle change set functions */
extern void merkle_record_update(Oid indexOid, int partitionId, MerkleHash *hash, bool is_insert);
extern void apply_merkle_changeset(MerkleChangeSet *changeset);

#endif
