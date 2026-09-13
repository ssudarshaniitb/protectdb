#ifndef BCDB_WORKER_H
#define BCDB_WORKER_H

#include "postgres.h"
#include <unistd.h>
#include <tcop/dest.h>
#include <utils/guc.h>
#include <tcop/tcopprot.h>
#include <pgstat.h>
#include <pg_trace.h>
#include <utils/memutils.h>
#include <utils/portal.h>
#include <utils/palloc.h>
#include <tcop/utility.h>
#include <utils/ps_status.h>
#include <miscadmin.h>
#include <utils/snapmgr.h>
#include <tcop/pquery.h>
#include <access/printtup.h>
#include "libpq/libpq.h"
#include "bcdb/shm_transaction.h"
#include "bcdb/shm_block.h"

typedef enum
{
    BCDB_PTRACE_METRIC_QUEUE_POP_WAIT_US = 0,
    BCDB_PTRACE_METRIC_PARSE_BARRIER_WAIT_US,
    BCDB_PTRACE_METRIC_SERIAL_SLOT_WAIT_US,
    BCDB_PTRACE_METRIC_PUBLISH_WS_US,
    BCDB_PTRACE_METRIC_PUBLISH_HASH_CLEAR_US,
    BCDB_PTRACE_METRIC_PRE_APPLY_WAIT_US,
    BCDB_PTRACE_METRIC_CONFLICT_WS_US,
    BCDB_PTRACE_METRIC_CONFLICT_RS_US,
    BCDB_PTRACE_METRIC_APPLY_INSERT_US,
    BCDB_PTRACE_METRIC_APPLY_UPDATE_US,
    BCDB_PTRACE_METRIC_APPLY_DELETE_US,
    BCDB_PTRACE_METRIC_APPLY_UPDATE_WAIT_US,
    BCDB_PTRACE_METRIC_APPLY_DELETE_WAIT_US,
    BCDB_PTRACE_METRIC_APPLY_DELETE_LOOKUP_US,
    BCDB_PTRACE_METRIC_APPLY_MERKLE_PREP_US,
    BCDB_PTRACE_METRIC_APPLY_MERKLE_UPDATE_US,
    BCDB_PTRACE_METRIC_FINISH_RESULT_US,
    BCDB_PTRACE_METRIC_FINISH_PREV_COMMIT_WAIT_US,
    BCDB_PTRACE_METRIC_FINISH_PUBLISH_US,
    BCDB_PTRACE_METRIC_PUBLISH_PARTITION_LOCK_US,
    BCDB_PTRACE_METRIC_PUBLISH_ROTATION_LOCK_US,
    BCDB_PTRACE_METRIC_WS_PROBE_MAPA_LOCK_WAIT_US,
    BCDB_PTRACE_METRIC_WS_PROBE_MAPA_HASH_LOOKUP_US,
    BCDB_PTRACE_METRIC_WS_PROBE_MAPB_LOCK_WAIT_US,
    BCDB_PTRACE_METRIC_WS_PROBE_MAPB_HASH_LOOKUP_US,
    BCDB_PTRACE_METRIC_COUNT
} bcdb_ptrace_metric_id;

typedef enum
{
    BCDB_PTRACE_COUNTER_WS_PUBLISH_ENTRIES = 0,
    BCDB_PTRACE_COUNTER_WS_CONFLICT_CHECKS,
    BCDB_PTRACE_COUNTER_RS_CONFLICT_CHECKS,
    BCDB_PTRACE_COUNTER_APPLY_INSERT_COUNT,
    BCDB_PTRACE_COUNTER_APPLY_UPDATE_COUNT,
    BCDB_PTRACE_COUNTER_APPLY_DELETE_COUNT,
    BCDB_PTRACE_COUNTER_APPLY_UPDATE_TM_BEING_MODIFIED_COUNT,
    BCDB_PTRACE_COUNTER_APPLY_DELETE_TM_BEING_MODIFIED_COUNT,
    BCDB_PTRACE_COUNTER_APPLY_UPDATE_WAIT_INCIDENT_COUNT,
    BCDB_PTRACE_COUNTER_APPLY_DELETE_WAIT_INCIDENT_COUNT,
    BCDB_PTRACE_COUNTER_MERKLE_UPDATE_COUNT,
    BCDB_PTRACE_COUNTER_APPLY_RETRY_COUNT,
    BCDB_PTRACE_COUNTER_PUBLISH_HASH_CLEAR_COUNT,
    BCDB_PTRACE_COUNTER_COUNT
} bcdb_ptrace_counter_id;

bool bcdb_ptrace_enabled(void);
uint64 bcdb_ptrace_now_us(void);
void bcdb_ptrace_add_us(bcdb_ptrace_metric_id metric, uint64 delta_us);
void bcdb_ptrace_inc_counter(bcdb_ptrace_counter_id counter, uint64 delta);
void bcdb_ptrace_note_queue_pop_wait(uint64 wait_us);

static inline uint64
bcdb_ptrace_timer_start(void)
{
    if (!bcdb_ptrace_enabled())
        return 0;
    return bcdb_ptrace_now_us();
}

static inline void
bcdb_ptrace_timer_stop(bcdb_ptrace_metric_id metric, uint64 start_us)
{
    if (start_us == 0)
        return;
    bcdb_ptrace_add_us(metric, bcdb_ptrace_now_us() - start_us);
}

bool bcdb_worker_init(void);
void bcdb_worker_process_tx(BCDBShmXact *tx);
void bcdb_worker_process_tx_dt(BCDBShmXact *tx, bool dual_table);
void bcdb_on_worker_exit(int code, Datum arg);
void bcdb_wait_for_prev_committed(BCDBShmXact *tx);
BCBlockID GetCurrentTxBlockId(void);
BCBlockID GetCurrentTxBlockIdSnapshot(void);



#endif
