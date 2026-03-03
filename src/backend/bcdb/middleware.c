//
// Created by Chris Liu on 6/5/2020.
//

#include "bcdb/shm_transaction.h"
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
#include <unistd.h>
#include <stdlib.h>
#include <string.h>

MemoryContext bcdb_middleware_context;
int32         tx_num = 0;
int32         blocksize = 0;
int32         numTxBurst = 0;
int32         burstTime = 0;
uint64        start_time;
static int  tx_id_counter = 0; // not bcdb

static BCDBShmXact *parse_tx(const char* json);
static void bcdb_middleware_attach_tx_to_block(BCDBShmXact *tx, BCBlock *block);
static BCBlock *parse_block_with_txs(const char *json);

void
bcdb_middleware_init(bool is_oep_mode, int32 block_size)
{
    MemoryContext    old_context;
    BCBlock *block;
    //int32 nWorkers = block_size;
    //nWorkers = 5;

    /* Aria does not have oep mode */
    is_bcdb_master = true;
    blocksize = block_size;
    bcdb_middleware_context = 
        AllocSetContextCreate(TopMemoryContext, 
                              "middleware memory context", 
                              ALLOCSET_DEFAULT_SIZES);
    old_context = MemoryContextSwitchTo(bcdb_middleware_context);
    block = get_block_by_id(1, true);
    if(blocksize != 0) set_blksz(blocksize);
    idle_worker_list_init(block_size);
    MemoryContextSwitchTo(old_context);
#if SAFEDBG2
	printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
#endif

    start_time = bcdb_get_time();
}

void
bcdb_middleware_init2(bool is_oep_mode, int32 block_size, int32 numTx, int32 timeSlot)
{
    MemoryContext    old_context;

    is_bcdb_master = true;
    blocksize = block_size;
    numTxBurst = numTx;
    burstTime = timeSlot;
    bcdb_middleware_context = 
        AllocSetContextCreate(TopMemoryContext, 
                              "middleware memory context", 
                              ALLOCSET_DEFAULT_SIZES);
    old_context = MemoryContextSwitchTo(bcdb_middleware_context);
    idle_worker_list_init(block_size);
    MemoryContextSwitchTo(old_context);
	printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());

    start_time = bcdb_get_time();
}

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

    sql = cJSON_GetObjectItemCaseSensitive(parsed, "sql");
    if (!cJSON_IsString(sql) || (sql->valuestring == NULL))
        goto error;

    hash = cJSON_GetObjectItemCaseSensitive(parsed, "hash");
    if (!cJSON_IsString(hash))
        goto error;
    
    isolation = XACT_SERIALIZABLE;
    pred_lock = true;

    tx = create_tx(hash->valuestring, sql->valuestring, BCDBInvalidTid, BCDBInvalidBid, isolation, pred_lock);

	printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
    create_time = cJSON_GetObjectItemCaseSensitive(parsed, "create_ts");

    if (cJSON_IsString(create_time))
    {
        char *endpt;
        tx->create_time = strtoll(create_time->valuestring, &endpt, 10);
    }

    if (tx == NULL)
    {
        ereport(ERROR,
            (errmsg("[ZL] cannot create transaction in shared memory")));
        return NULL;
    }
    cJSON_Delete(parsed);
    return tx;

error:
    ereport(ERROR,
        (errmsg("[ZL] Cannot parse transaction: %s", json)));
    /* no need to do clean here, because memory context will do that for us */
    return NULL;
}

BCBlock *
parse_block_with_txs(const char *json)
{
    cJSON *parsed;
    cJSON *tx_list;
    cJSON *block_id;
    cJSON *tx_json;
    BCBlock *block;
    int j = 0;
    //static int  tx_id_counter = 0; // not bcdb
    //int  tx_id_counter = 0; // not bcdb

	// printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
	//printf("ariaMyDbg %s : %s: %d \n", __FILE__, __FUNCTION__, __LINE__ );
    parsed = cJSON_Parse(json);
    if (!parsed)
        goto error;
    
	//printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
    block_id = cJSON_GetObjectItemCaseSensitive(parsed, "bid");
    
	//printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
    tx_list = cJSON_GetObjectItemCaseSensitive(parsed, "txs");
    if (!cJSON_IsArray(tx_list))
        goto error;

	//printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
    block = get_block_by_id(block_id->valueint, true);
#if SAFEDBG
	printf("ariaMyDbg %s : %s: %d blksz %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , get_blksz(), getpid());
#endif
    block->num_tx = cJSON_GetArraySize(tx_list);
    cJSON_ArrayForEach(tx_json, tx_list)
    {
        cJSON   *sql      = NULL;
        cJSON   *hash     = NULL;
        cJSON   *create_time = NULL;
        BCDBShmXact   *tx;
        int     isolation;
        bool    pred_lock = false;

        sql = cJSON_GetObjectItemCaseSensitive(tx_json, "sql");
	//printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
        if (!cJSON_IsString(sql) || (sql->valuestring == NULL))
            goto error;

	if(j < 5) {
		//printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
		cJSON_Print(sql);
	}
        hash = cJSON_GetObjectItemCaseSensitive(tx_json, "hash");
        if (!cJSON_IsString(hash))
            goto error;
	if(j < 5) {
		//printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
		cJSON_Print(hash);
		j++; 
	}
	//printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());

        isolation = XACT_SERIALIZABLE;
        pred_lock = true;

        tx = create_tx(hash->valuestring, sql->valuestring, BCDBInvalidTid, BCDBInvalidBid, isolation, pred_lock);

	//printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
        create_time = cJSON_GetObjectItemCaseSensitive(tx_json, "create_ts");

        if (cJSON_IsString(create_time))
        {
            char *endpt;
            tx->create_time = strtoll(create_time->valuestring, &endpt, 10);
        }
        
        tx_id_counter = get_num_tx_sub();
        tx->tx_id = tx_id_counter + (block->id - 1) * blocksize;
        tx->block_id_committed = block->id;
        block->txs[tx_id_counter] = tx;
        tx_id_counter += 1;
        set_num_tx_sub(tx_id_counter); // get_block_by_id
#if SAFEDBG
		printf("ariaMyDbg %s : %s: %d txid %d bid %d hash %s \n", __FILE__, __FUNCTION__, __LINE__ , tx->tx_id, block->id, hash->valuestring);
#endif
    }
    //if(blocksize != 0) set_blksz(blocksize);
    //block->blksize = blocksize;
	//printf("ariaMyDbg %s : %s: %d blksz %d \n", __FILE__, __FUNCTION__, __LINE__ , get_blksz());
    return block;

error:
    print_trace();
    ereport(FATAL,
        (errmsg("[ZL] cannot create block in shared memory")));
    return NULL;
}

int 
bcdb_middleware_submit_tx(const char* tx_string)
{
    BCDBShmXact *tx;
    tx = parse_tx(tx_string);
    tx_queue_insert(tx, tx_num++);
	printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
    return 0;
}

char *
bcdb_middleware_submit_block(const char* block_json)
{
    BCBlock     *block;
    struct timeval tv1;
    tv1.tv_sec = 0; tv1.tv_usec = 0;
    static int next_tx_id = 0;
    int tx_num2 = 0;
    //struct timeval tv1 ;
    //tv1.tv_sec = 0; tv1.tv_usec = 0;
    // static int tmp = 0;
    ++block_meta->global_bmax;
    block = parse_block_with_txs(block_json);
    tx_num2 =  get_num_txqd(); //  get_block_by_id
/*
if(tmp < 2) {
tmp++;
print_trace();
} else { return NULL; }
*/
#if SAFEDBG
	printf("ariaMyDbg %s : %s: %d pid %d txnum %d txnum2 %d next_tx_id %d blk-numtx %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid(), tx_num, tx_num2, next_tx_id, block->num_tx);
#endif
	printf("ariaMyDbg %s : %s: %d pid %d tx %s \n", __FILE__, __FUNCTION__, __LINE__ , getpid(), block->txs[tx_num2]->sql);
    for (int i= 0; i < block->num_tx; i++, next_tx_id++)
    {
      tx_queue_insert(block->txs[tx_num2], tx_num2);
      //tx_queue_insert(block->txs[next_tx_id], tx_num++);
      //tx_queue_insert(block->txs[i], tx_num++);
      tx_num2++;
    }
    set_num_txqd(tx_num2); //  get_block_by_id
	//printf("ariaMyDbg %s : %s: %d pid %d tx %s \n", __FILE__, __FUNCTION__, __LINE__ , getpid(), block->txs[tx_num2-1]->sql);
        block = get_block_by_id(1, false);
        Assert(block != NULL);
		gettimeofday(&tv1, NULL);
#if SAFEDBG
		printf("\n\n\t time= %ld.%ld  getpid %d\n", tv1.tv_sec, tv1.tv_usec, getpid());
          WaitConditionPidDbg(&block->done_conds[tx_num2 -1], getpid(),
#else
          WaitConditionPid(&block->done_conds[tx_num2 -1], getpid(),
#endif
                         ( block->last_committed_tx_id  == (tx_num2 -1) ));
/*
*/
#if SAFEDBG
		gettimeofday(&tv1, NULL);
		printf("\n\n\t time= %ld.%ld  getpid %d\n", tv1.tv_sec, tv1.tv_usec, getpid());
        printf("blkmid read result at %d= %s\n", ((tx_num2-1)%(2*block->blksize)), block->result[(tx_num2-1)%(2*block->blksize)]);
        printf("\n\t *** safeDB completed txid %d pid %d %s : %s: %d *** \n\n",
               tx_num2, getpid(), __FILE__, __FUNCTION__, __LINE__ );
#endif
        printf("\n\t *** safeDB txid %d pid %d result %s file %s : %s: %d *** \n\n", 
               tx_num2, getpid(), &block->result[tx_num2-1],__FILE__, __FUNCTION__, __LINE__ );
//ereport(INFO, (errmsg(&block->result[tx_num2-1])));
// TODO -- another way to convey results...
// wait-to-finish() ?? or 

//safeOut();
	//printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
return block->result[(tx_num2-1)%(2*block->blksize)];
}

void
bcdb_middleware_submit_block2(const char* block_json)
{
    BCBlock     *block;
    struct timeval tv1 ;
    tv1.tv_sec = 0; tv1.tv_usec = 0;

    ++block_meta->global_bmax;
	printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
    block = parse_block_with_txs(block_json);
    for (int i=0; i < block->num_tx; i++)
    {
      tx_queue_insert(block->txs[i], tx_num++);
	  if( (i % numTxBurst == 0)&&(i > 0)) {
		gettimeofday(&tv1, NULL);
		printf("\n\n\t time= %ld.%ld  getpid %d\n", tv1.tv_sec, tv1.tv_usec, getpid());
		printf("\t ariaMyDbg %s : %s: %d pid %d  sleeping %dms next burstSz %d from tx %d\n\n", __FILE__, __FUNCTION__, __LINE__ , getpid() ,burstTime, numTxBurst, i );
		usleep(burstTime);
	  }
    }
	printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
}

void 
bcdb_wait_tx_finish(char *tx_hash)
{
    BCDBShmXact *tx;
    tx = get_tx_by_hash(tx_hash);
    ConditionVariablePrepareToSleep(&tx->cond);
    while(tx->status != TX_COMMITED && tx->status != TX_ABORTED)
        ConditionVariableSleep(&tx->cond, WAIT_EVENT_TX_FINISH);
    ConditionVariableCancelSleep();
}

void
bcdb_middleware_wait_all_to_finish()
{
    WaitGlobalBmin(block_meta->global_bmax + 1);
    ereport(LOG, (errmsg("[ZL] total throughput: %.3f", (double)block_meta->num_committed * 1e6 / (bcdb_get_time() - start_time))));
}

void 
bcdb_middleware_set_txs_committed_block(char * tx_hash, int32 block_id)
{
    BCDBShmXact *tx;
    BCBlock     *block;
    tx = get_tx_by_hash(tx_hash);
    block = get_block_by_id(block_id, true);
    bcdb_middleware_attach_tx_to_block(tx, block);
}

void
bcdb_middleware_attach_tx_to_block(BCDBShmXact *tx, BCBlock *block)
{
    block_add_tx(block, tx);
    tx->block_id_committed = block->id;
}

void
block_cleaning(BCBlockID current_block_id)
{
    BCBlock *block_to_clean;
    uint64 cur_report_ts = bcdb_get_time();
    int32  cur_num_committed = block_meta->num_committed;
    float abort_rate = (float)block_meta->num_aborted / (block_meta->num_aborted + block_meta->num_committed);
	printf("\nariaMyDbg %s : %s: %d \n", __FILE__, __FUNCTION__, __LINE__ );
	printf("ariaMyDbg %s : %s: %d \n\n", __FILE__, __FUNCTION__, __LINE__ );

    if (current_block_id > CLEANING_DELAY_BLOCKS)
    {
        block_to_clean = get_block_by_id(current_block_id - CLEANING_DELAY_BLOCKS, false);
        if (block_to_clean != NULL)
        {
            for (int i=0; i < block_to_clean->num_tx; i++)
            {
#ifdef LOG_STATUS
                block_meta->log_counter += sprintf(block_meta->log + block_meta->log_counter, "%s %d\n", block_to_clean->txs[i]->hash, block_to_clean->txs[i]->status);
                if (block_meta->log_counter > 1024 * 1024 * 10)
                    ereport(FATAL, (errmsg("[ZL] log overflow")));
#endif
                delete_tx(block_to_clean->txs[i]);
            }
        }
        delete_block(block_to_clean);
    }

    if (cur_report_ts - block_meta->previous_report_ts > 1e6 * REPORT_INTERVAL)
    {
        if (block_meta->previous_report_ts != 0)
        {
            ereport(LOG, (errmsg("[ZL] throughput: %.3f", (cur_num_committed - block_meta->previous_report_commit) * 1e6 / (cur_report_ts - block_meta->previous_report_ts))));
            ereport(LOG, (errmsg("[ZL] abort rate: %.3f", abort_rate)));
        }
        block_meta->previous_report_ts = cur_report_ts;
        block_meta->previous_report_commit = cur_num_committed;
    }
}

void
allow_all_block_txs_to_commit(BCBlock *block)
{
    return;
}
/*
*/

void
bcdb_middleware_conflict_check(BCBlock *block)
{
    /* we assume no one is touching the conflict graph here */
    return;
}


void bcdb_middleware_allow_txs_exec_write_set_and_commit(BCBlock *block) {

//    bcdb_middleware_allow_execute_write_set(block);

    allow_all_block_txs_to_commit(block);
}

void bcdb_middleware_allow_txs_exec_write_set_and_commit_by_id(int32 id){
    BCBlock *block;
    
    block = get_block_by_id(id, false);
    Assert(block != NULL);
    bcdb_middleware_allow_txs_exec_write_set_and_commit(block);
}

bool bcdb_is_tx_commited(char * tx_hash){
    BCDBShmXact* target_tx = get_tx_by_hash(tx_hash);

    if(target_tx->status == TX_COMMITED){
        return true;
    }else{
        return false;
    }
}

void 
bcdb_clear_block_txs_store()
{
	printf("\nariaMyDbg %s : %s: %d \n", __FILE__, __FUNCTION__, __LINE__ );
	printf("ariaMyDbg %s : %s: %d \n\n", __FILE__, __FUNCTION__, __LINE__ );
    shm_hash_clear(block_pool, MAX_NUM_BLOCKS);
    clear_tx_pool();
    block_meta->global_bmin = 1;
    block_meta->global_bmax = 0;
    block_meta->debug_seq += 1;
    block_meta->num_committed = 0;
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
void bcdb_middleware_new_block_handler(BCBlock* block){
*/

/*
// assume dummy file contains jsons per line
Transaction* parsing_dummy_block_file(const char* file_path){
*/

/*
//dummy function called by frontend
void bcdb_middleware_dummy_block(const char* file_path, uint32 block_id){
*/

/*
void bcdb_middleware_dummy_submit_tx(const char* file_path){
*/

//Return false if 1)no tx with that hash or 2) tx is not finish execution

