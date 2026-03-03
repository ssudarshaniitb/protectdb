#include "bcdb/shm_block.h"
#include "postgres.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"
#include <sys/queue.h>

HTAB          *block_pool;
slock_t       *block_pool_lock;
BlockMeta     *block_meta;

Size
block_pool_size()
{
    Size ret = sizeof(BlockMeta);
    ret = add_size(ret, sizeof(slock_t));
    ret = add_size(ret, hash_estimate_size(MAX_NUM_BLOCKS, sizeof(BCBlock)));
    return ret;
}

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
 

     //set_blksz(1); // cause segfault immediately on starting -- 
     // -- b4 even printing 2025-11-18 16:32:20.830 IST [1199537] LOG:  starting PostgreSQL 13devel on x86_64-pc-linux-gnu, compiled by gcc (Ubuntu 11.4.0-1ubuntu1~22.04.2) 11.4.0, 64-bit
     // 2025-11-18 16:32:20.831 IST [1199537] LOG:  listening on IPv4 address "127.0.0.1", port 5432

}

void set_last_committed_id(int tx_id)
{
    //BCBlock* blk = get_block_by_id( tx->block_id_committed, false);
    BCBlock* blk = get_block_by_id(1, true);
    blk->last_committed_tx_id = tx_id;
    block_meta->num_committed = tx_id;
#if SAFEDBG
    printf("safeDbg %s : %s: %d  blk %x txid= %d\n",
              __FILE__, __FUNCTION__, __LINE__, blk, block_meta->num_committed);
#endif
}
void set_last_committed_txid( BCDBShmXact *tx)
{
    //BCBlock* blk = get_block_by_id( tx->block_id_committed, false);
    BCBlock* blk = get_block_by_id(1, true);
    blk->last_committed_tx_id = tx->tx_id;
    block_meta->num_committed = tx->tx_id;
#if SAFEDBG2
    printf("safeDbg %s : %s: %d  blk %x txid= %d\n",
              __FILE__, __FUNCTION__, __LINE__, blk, block_meta->num_committed);
#endif
}

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
    //printf("ariaMyDbg %s : %s: %d bid %d, blk %x\n",
             // __FILE__, __FUNCTION__, __LINE__, id , blk);
    return blk->blksize ;
}

void set_num_tx_sub(int num)
{
    BCBlock* blk = get_block_by_id(1, false);
    //printf("ariaMyDbg %s : %s: %d bid %d, blk %x\n",
             // __FILE__, __FUNCTION__, __LINE__, id , blk);
    blk->num_tx_sub = num;
}

BCTxID get_num_tx_sub()
{
    BCBlock* blk = get_block_by_id(1, false);
    //printf("ariaMyDbg %s : %s: %d bid %d, blk %x\n",
             // __FILE__, __FUNCTION__, __LINE__, id , blk);
    return blk->num_tx_sub ;
}

void set_num_txqd(int num)
{
    BCBlock* blk = get_block_by_id(1, false);
    //printf("ariaMyDbg %s : %s: %d bid %d, blk %x\n",
             // __FILE__, __FUNCTION__, __LINE__, id , blk);
    blk->num_tx_qd = num;
}

BCTxID get_num_txqd()
{
    BCBlock* blk = get_block_by_id(1, false);
    //printf("ariaMyDbg %s : %s: %d bid %d, blk %x\n",
             // __FILE__, __FUNCTION__, __LINE__, id , blk);
    return blk->num_tx_qd ;
}

BCTxID get_last_committed_txid(BCDBShmXact *tx)
{
    //BCBlock* blk = get_block_by_id( tx->block_id_committed, false);
    BCBlock* blk = get_block_by_id(1, false);
    //printf("ariaMyDbg %s : %s: %d bid 1, blk %x\n",
              //__FILE__, __FUNCTION__, __LINE__, blk);
    return blk->last_committed_tx_id ;
}

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
              getpid(),__FILE__, __FUNCTION__, __LINE__, id , block);
            block->id = id;
            block->num_tx = 0;
            block->num_ready = 0;
            block->num_finished = 0;
            block->last_committed_tx_id = -1;
            ConditionVariableInit(&block->cond);
            ConditionVariableInit(&block->condRecovery);
            for(int i = 0; i < MAX_TX_PER_BLOCK; i++) {
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

void
delete_block_by_id(BCBlockID id)
{
    if (id == BCDBInvalidBid || id == BCDBMaxBid)
        return;
    SpinLockAcquire(block_pool_lock);
    hash_search(block_pool, &id, HASH_REMOVE, NULL);
    SpinLockRelease(block_pool_lock);
}

void
block_add_tx(BCBlock* block, BCDBShmXact* tx)
{
    Assert(tx->block_id_committed == BCDBInvalidBid || tx->block_id_committed == BCDBMaxBid);
    Assert(block->num_tx < MAX_TX_PER_BLOCK);
    block->txs[block->num_tx++] = tx;
}

char*
print_block_status(BCBlockID block_id)
{
    BCBlock         *block; 
    char            *ret;
    BCDBShmXact     *tx;
    int             offset;
    block = get_block_by_id((BCBlockID)block_id, false);
    ret = palloc(block->num_tx * (TX_HASH_SIZE + 10));

    offset = 0;
    for (int i=0; i < block->num_tx; i++)
    {
        tx = block->txs[i];
        offset += sprintf(ret + offset, "%s\t%d\n", tx->hash, tx->status);
    }
    return ret;
}
