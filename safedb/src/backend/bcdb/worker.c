#include "postgres.h"
#include "bcdb/worker.h"
#include "bcdb/shm_transaction.h"
#include "bcdb/shm_block.h"
#include "bcdb/utils/cJSON.h"
#include "libpq/libpq.h"
#include <unistd.h>
#include <tcop/dest.h>
#include <utils/guc.h>
#include <tcop/tcopprot.h>
#include <pgstat.h>
#include <pg_trace.h>
#include <utils/palloc.h>
#include <utils/portal.h>
#include <utils/memutils.h>
#include <tcop/utility.h>
#include <utils/ps_status.h>
#include <miscadmin.h>
#include <utils/snapmgr.h>
#include <tcop/pquery.h>
#include <access/printtup.h>
#include <storage/ipc.h>
#include <executor/executor.h>
#include <assert.h>
#include <storage/predicate.h>
#include "bcdb/middleware.h"
#include "bcdb/globals.h"
#include "bcdb/bcdb_dsa.h"
#include "bcdb/shm_block.h"
#include "storage/condition_variable.h"
#include "pgstat.h"
#include "parser/analyze.h"
#include <unistd.h>
#include <errno.h>
#include <ctype.h>
#include <string.h>
#include "access/heapam.h"
#include "access/hash.h"
#include "catalog/namespace.h"
#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include "access/xact.h"
#include "utils/resowner.h"

int current_block_id;
int total_num_restarts = 0; // worker specific -- move to shm for global
int t_sinceTx1 = 0;
FILE *fp;
struct timeval tx_start_time;
MemoryContext bcdb_tx_context;
MemoryContext bcdb_worker_context;

CommandDest dest;
char completionTag[COMPLETION_TAG_BUFSIZE];

static void get_write_set(BCDBShmXact *tx, Snapshot snapshot);

static inline const char *
skip_ws(const char *s)
{
    while (*s && isspace((unsigned char) *s))
        s++;
    return s;
}

static bool
completiontag_is_delete_0(const char *tag)
{
    const char *p = skip_ws(tag);

    if (pg_strncasecmp(p, "DELETE", 6) != 0)
        return false;
    p += 6;
    p = skip_ws(p);
    if (*p == '\0')
        return false;

    errno = 0;
    char *endptr = NULL;
    long n = strtol(p, &endptr, 10);
    if (endptr == p || errno != 0)
        return false;

    return (n == 0);
}

static const char *
find_token_ci(const char *haystack, const char *needle)
{
    size_t nlen = strlen(needle);

    for (const char *p = haystack; *p; p++)
    {
        if (pg_strncasecmp(p, needle, nlen) == 0)
            return p;
    }

    return NULL;
}

static bool
parse_ycsb_key_eq_int32(const char *sql, int32 *key_out)
{
    const char *p;

    p = find_token_ci(sql, "YCSB_KEY");
    if (p == NULL)
        return false;
    p += strlen("YCSB_KEY");

    p = skip_ws(p);
    if (*p != '=')
        return false;
    p++;

    p = skip_ws(p);
    if (*p == '\0')
        return false;

    errno = 0;
    char *endptr = NULL;
    long v = strtol(p, &endptr, 10);
    if (endptr == p || errno != 0)
        return false;
    if (v < PG_INT32_MIN || v > PG_INT32_MAX)
        return false;

    *key_out = (int32) v;
    return true;
}

static inline bool
is_ident_char(unsigned char c)
{
    return isalnum(c) || c == '_';
}

static const char *
find_keyword_ci(const char *haystack, const char *needle)
{
    size_t nlen = strlen(needle);

    for (const char *p = haystack; *p; p++)
    {
        if (pg_strncasecmp(p, needle, nlen) == 0)
        {
            unsigned char before = (p == haystack) ? 0 : (unsigned char) p[-1];
            unsigned char after = (unsigned char) p[nlen];

            if ((p == haystack || !is_ident_char(before)) &&
                (after == '\0' || !is_ident_char(after)))
                return p;
        }
    }

    return NULL;
}

static bool
parse_quoted_ident(const char **sp, char *out, Size outsz)
{
    const char *s = *sp;
    Size        n = 0;

    if (*s != '"')
        return false;
    s++;

    while (*s)
    {
        if (*s == '"')
        {
            if (s[1] == '"')
            {
                if (n + 1 >= outsz)
                    return false;
                out[n++] = '"';
                s += 2;
                continue;
            }

            s++;
            out[n] = '\0';
            *sp = s;
            return (n > 0);
        }

        if (n + 1 >= outsz)
            return false;
        out[n++] = *s++;
    }

    return false;
}

static bool
parse_delete_from_relname(const char *sql, char *relname_out, Size relname_out_sz)
{
    const char *p;
    const char *from;

    p = skip_ws(sql);
    if (pg_strncasecmp(p, "DELETE", 6) != 0)
        return false;

    from = find_keyword_ci(p + 6, "FROM");
    if (from == NULL)
        return false;

    p = skip_ws(from + 4);

    if (pg_strncasecmp(p, "ONLY", 4) == 0 && isspace((unsigned char) p[4]))
        p = skip_ws(p + 4);

    if (*p == '\0')
        return false;

    if (*p == '"')
    {
        char first[NAMEDATALEN];

        if (!parse_quoted_ident(&p, first, sizeof(first)))
            return false;

        p = skip_ws(p);
        if (*p == '.')
        {
            p = skip_ws(p + 1);

            if (*p == '"')
            {
                char second[NAMEDATALEN];

                if (!parse_quoted_ident(&p, second, sizeof(second)))
                    return false;
                strlcpy(relname_out, second, relname_out_sz);
                return true;
            }
            else
            {
                const char *start = p;
                size_t      len;

                while (*p &&
                       !isspace((unsigned char) *p) &&
                       *p != ';' && *p != ',' && *p != '(')
                    p++;
                len = (size_t) (p - start);
                if (len == 0 || len >= relname_out_sz)
                    return false;
                memcpy(relname_out, start, len);
                relname_out[len] = '\0';
                return true;
            }
        }

        strlcpy(relname_out, first, relname_out_sz);
        return true;
    }
    else
    {
        const char *start = p;
        size_t      len;
        char        token[NAMEDATALEN * 2];
        char       *last_dot;
        const char *rel;

        while (*p &&
               !isspace((unsigned char) *p) &&
               *p != ';' && *p != ',' && *p != '(')
            p++;
        len = (size_t) (p - start);
        if (len == 0 || len >= sizeof(token))
            return false;

        memcpy(token, start, len);
        token[len] = '\0';

        last_dot = strrchr(token, '.');
        rel = last_dot ? last_dot + 1 : token;
        if (*rel == '\0')
            return false;

        strlcpy(relname_out, rel, relname_out_sz);
        return true;
    }
}

static void
bcdb_maybe_enqueue_deferred_delete0_by_key(BCDBShmXact *tx)
{
    const char *sql;
    int32 keyval;
    Oid relOid;
    char relname[NAMEDATALEN];
    uint32 h;
    PREDICATELOCKTARGETTAG tag;

    if (tx == NULL)
        return;

    sql = skip_ws(tx->sql);
    if (pg_strncasecmp(sql, "DELETE", 6) != 0)
        return;

    if (!completiontag_is_delete_0(completionTag))
        return;

    if (!parse_ycsb_key_eq_int32(sql, &keyval))
        return;

    if (!parse_delete_from_relname(sql, relname, sizeof(relname)))
        return;

    relOid = RelnameGetRelid(relname);
    if (!OidIsValid(relOid))
        return;

    h = hash_any((unsigned char *) &keyval, sizeof(int32));
    SET_PREDICATELOCKTARGETTAG_TUPLE(tag, 0, relOid,
                                     (BlockNumber)(h >> 16),
                                     (OffsetNumber)((h & 0xFFFF) | 1));
    ws_table_reserveDT(&tag);
    store_optim_delete_by_key(relOid, keyval, GetCurrentCommandId(true));
}

BCBlockID 
GetCurrentTxBlockId(void)
{
    if (activeTx == NULL)
        return 0;
    return activeTx->block_id_committed;
}

BCBlockID
GetCurrentTxBlockIdSnapshot(void)
{
    if (activeTx == NULL)
        return 0;
    return activeTx->block_id_snapshot;
}

bool
bcdb_worker_init(void)
{
    MemoryContext old_context;
    if (bcdb_worker_context != NULL && bcdb_tx_context != NULL)
        return true;
    ereport(DEBUG3,
        (errmsg("[ZL] worker(%d) initializing", getpid())));
    bcdb_worker_context = 
        AllocSetContextCreate(TopMemoryContext, 
                              "bcdb worker memory context", 
                              ALLOCSET_DEFAULT_SIZES);
    bcdb_tx_context = 
        AllocSetContextCreate(TopMemoryContext, 
                              "bcdb transaction memory context", 
                              ALLOCSET_DEFAULT_SIZES);
    old_context = MemoryContextSwitchTo(bcdb_worker_context);
    MemoryContextSwitchTo(old_context);
    pid = getpid();
    srand(pid);
    if (OEP_mode)
        DEBUGNOCHECK("[ZL] worker runing is OEP mode");
    return true;
}


void
get_write_set(BCDBShmXact *tx, Snapshot snapshot) {
    const char *query_string = tx->sql;

    MemoryContext oldcontext;
    List *parsetree_list;
    ListCell *parsetree_item;
    bool save_log_statement_stats = log_statement_stats;
    bool use_implicit_block;
    const char *commandTag;
    MemoryContext per_parsetree_context = NULL;
    List *querytree_list,
         *plantree_list;
    Portal portal;
    DestReceiver *receiver;
    int16 format;
    RawStmt *parsetree;
    bool snapshot_set = false;
    dest = whereToSendOutput;

    /*
     * Report query to various monitoring facilities.
     */
    //printf("safeDbg pid %d %s : %s: %d \n",
            //getpid(), __FILE__, __FUNCTION__, __LINE__ );
    debug_query_string = query_string;
    activeTx->start_parsing_time = bcdb_get_time();
    pgstat_report_activity(STATE_RUNNING, query_string);

    TRACE_POSTGRESQL_QUERY_START(query_string);

    /*
     * We use save_log_statement_stats so ShowUsage doesn't report incorrect
     * results because ResetUsage wasn't called.
     */
    if (save_log_statement_stats)
        ResetUsage();


    /*
     * Start up a transaction command.  All queries generated by the
     * query_string will be in this same command block, *unless* we find a
     * BEGIN/COMMIT/ABORT statement; we have to force a new xact command after
     * one of those, else bad things will happen in xact.c. (Note that this
     * will normally change current memory context.)
     */
//    start_new_tx_state();
//    start_xact_command();
//    if (snapshot == NULL) {
//        snapshot = GetTransactionSnapshot();
//    }
//    PushActiveSnapshot(snapshot);

    /*
     * Zap any pre-existing unnamed statement.  (While not strictly necessary,
     * it seems best to define simple-Query mode as if it used the unnamed
     * statement and portal; this ensures we recover any storage used by prior
     * unnamed operations.)
     */
    drop_unnamed_stmt();

    /*
     * Switch to appropriate context for constructing parsetrees.
     */
    oldcontext = MemoryContextSwitchTo(MessageContext);

    /*
     * Do basic parsing of the query or queries (this should be safe even if
     * we are in aborted transaction state!)
     */
    parsetree_list = pg_parse_query(query_string);

    /* Log immediately if dictated by log_statement */
    if (check_log_statement(parsetree_list)) {
        ereport(LOG,
                (errmsg("statement: %s", query_string),
                        errhidestmt(true),
                        errdetail_execute(parsetree_list)));
    }

    /*
     * Switch back to transaction context to enter the loop.
     */
    MemoryContextSwitchTo(oldcontext);

    /*
     * For historical reasons, if multiple SQL statements are given in a
     * single "simple Query" message, we execute them as a single transaction,
     * unless explicit transaction control commands are included to make
     * portions of the list be separate transactions.  To represent this
     * behavior properly in the transaction machinery, we use an "implicit"
     * transaction block.
     */
    use_implicit_block = (list_length(parsetree_list) > 1);

    /*
     * Run through the raw parsetree(s) and process each one.
     */
    assert(parsetree_list->length==1);
    parsetree_item = parsetree_list->elements;
    parsetree = lfirst_node(RawStmt, parsetree_item);

    //chris: snapshot is fixed @ the beginning
    

    /*
     * Get the command name for use in status display (it also becomes the
     * default completion tag, down inside PortalRun).  Set ps_status and
     * do any special start-of-SQL-command processing needed by the
     * destination.
     */
    commandTag = CreateCommandTag(parsetree->stmt);

    set_ps_display(commandTag, false);

    BeginCommand(commandTag, dest);

    /*
     * If we are in an aborted transaction, reject all commands except
     * COMMIT/ABORT.  It is important that this test occur before we try
     * to do parse analysis, rewrite, or planning, since all those phases
     * try to do database accesses, which may fail in abort state. (It
     * might be safe to allow some additional utility commands in this
     * state, but not many...)
     */
//        if (IsAbortedTransactionBlockState() &&
//            !IsTransactionExitStmt(parsetree->stmt))
//            ereport(ERROR,
//                    (errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
//                            errmsg("current transaction is aborted, "
//                                   "commands ignored until end of transaction block"),
//                            errdetail_abort()));

    /* Make sure we are in a transaction command */
//        start_xact_command();

    /*
     * If using an implicit transaction block, and we're not already in a
     * transaction block, start an implicit block to force this statement
     * to be grouped together with any following ones.  (We must do this
     * each time through the loop; otherwise, a COMMIT/ROLLBACK in the
     * list would cause later statements to not be grouped.)
     */
    if (use_implicit_block)
        BeginImplicitTransactionBlock();

    /* If we got a cancel signal in parsing or prior command, quit */
    CHECK_FOR_INTERRUPTS();

//        /*
//         * Set up a snapshot if parse analysis/planning will need one.
//         */
    if (analyze_requires_snapshot(parsetree))
    {
        PushActiveSnapshot(snapshot);
        snapshot_set = true;
    }

    /*
     * OK to analyze, rewrite, and plan this query.
     *
     * Switch to appropriate context for constructing query and plan trees
     * (these can't be in the transaction context, as that will get reset
     * when the command is COMMIT/ROLLBACK).  If we have multiple
     * parsetrees, we use a separate context for each one, so that we can
     * free that memory before moving on to the next one.  But for the
     * last (or only) parsetree, just use MessageContext, which will be
     * reset shortly after completion anyway.  In event of an error, the
     * per_parsetree_context will be deleted when MessageContext is reset.
     */
    if (lnext(parsetree_list, parsetree_item) != NULL) {
        per_parsetree_context =
                AllocSetContextCreate(MessageContext,
                                      "per-parsetree message context",
                                      ALLOCSET_DEFAULT_SIZES);
        oldcontext = MemoryContextSwitchTo(per_parsetree_context);
    } else
        oldcontext = MemoryContextSwitchTo(MessageContext);

    querytree_list = pg_analyze_and_rewrite(parsetree, query_string,
                                            NULL, 0, NULL); 
 

    plantree_list = pg_plan_queries(querytree_list,
                                    CURSOR_OPT_PARALLEL_OK, NULL);

    /* Done with the snapshot used for parsing/planning */
    if (snapshot_set)
        PopActiveSnapshot();

    /* If we got a cancel signal in analysis or planning, quit */
    CHECK_FOR_INTERRUPTS();

    /*
     * Create unnamed portal to run the query or queries in. If there
     * already is one, silently drop it.
     */
    portal = CreatePortal("", true, true);
    /* Don't display the portal in pg_cursors */
    portal->visible = false;

    /*
     * We don't have to copy anything into the portal, because everything
     * we are passing here is in MessageContext or the
     * per_parsetree_context, and so will outlive the portal anyway.
     */
    PortalDefineQuery(portal,
                      NULL,
                      query_string,
                      commandTag,
                      plantree_list,
                      NULL);
    activeTx->end_parsing_time = bcdb_get_time();

    /*
     * Start the portal.  No parameters here.
     * chris: I fix the snapshot here
     */
    PortalStart(portal, NULL, 0, snapshot);

    /*
     * Select the appropriate output format: text unless we are doing a
     * FETCH from a binary cursor.  (Pretty grotty to have to do this here
     * --- but it avoids grottiness in other places.  Ah, the joys of
     * backward compatibility...)
     */
    format = 0;                /* TEXT is default */
    if (IsA(parsetree->stmt, FetchStmt)) {
        FetchStmt *stmt = (FetchStmt *) parsetree->stmt;

        if (!stmt->ismove) {
            Portal fportal = GetPortalByName(stmt->portalname);

            if (PortalIsValid(fportal) &&
                (fportal->cursorOptions & CURSOR_OPT_BINARY))
                format = 1; /* BINARY */
        }
    }
    PortalSetResultFormat(portal, 1, &format);

#if SAFEDBG2
    printf("safeDbg pid %d %s : %s: %d  tx= %d dest=%d\n",
           getpid(), __FILE__, __FUNCTION__, __LINE__ ,activeTx->tx_id, whereToSendOutput );
#endif

    /*
     * Now we can create the destination receiver object.
     */
    receiver = CreateDestReceiver(dest);
    if (dest == DestRemote)
        SetRemoteDestReceiverParams(receiver, portal);

    /*
     * Switch back to transaction context for execution.
     */
    MemoryContextSwitchTo(oldcontext);

    activeTx->portal = portal;
    (void) PortalRun(portal,
                     FETCH_ALL,
                     true,    /* always top level */
                     true,
                     receiver,
                     receiver,
                     completionTag);

#if SAFEDBG1
    printf("safeDbg pid %d %s : %s: %d \n",
           getpid(), __FILE__, __FUNCTION__, __LINE__ );
#endif

    MemoryContextSwitchTo(oldcontext);
}

int chk_query_type(const char *query, const char* fmt1_str, const char *fmt2_str) {

  int cmd_type = 1;
  int cmd_len = 6; // all of INSERT, UPDATE, DELETE, SELECT len = 6 !!
  int ofst = 0; 
  for (ofst = 0; ofst < cmd_len; ofst++) {
    if((query[ofst] != fmt1_str[ofst]) && (query[ofst] != fmt2_str[ofst]))
              { cmd_type = 0; break; }
  }
  return cmd_type;
}

    /*
    */
void safedb_txdt(BCDBShmXact *tx, bool dualTab) {
   bool saved_is_bcdb_worker = is_bcdb_worker;
   PG_TRY();
   {
       bcdb_worker_process_tx_dt(tx,  dualTab);
   }
   PG_CATCH();
   {
       /*
        * Restore is_bcdb_worker flag even on error to avoid leaking
        * snapshots/relcache references in subsequent queries.
        */
       is_bcdb_worker = saved_is_bcdb_worker;
       PG_RE_THROW();
   }
   PG_END_TRY();
   /*
    * Restore is_bcdb_worker flag after BCDB transaction processing.
    * bcdb_worker_process_tx_dt sets is_bcdb_worker = true, but when called
    * from a regular psql session (not a dedicated worker), this flag must
    * be restored to prevent subsequent queries from using BCDB-specific
    * code paths (PORTAL_MULTI_QUERY strategy, skipped ExecutorFinish/End,
    * modified snapshot/heap visibility, etc.) which cause snapshot reference
    * leaks and other resource leaks.
    */
   is_bcdb_worker = saved_is_bcdb_worker;
}

void
bcdb_worker_process_tx_dt(BCDBShmXact *tx, bool dualTab)
{
    BCBlock     *block = NULL;
    Snapshot     snapshot;
    ResourceOwner old_owner;

    int latest_tx_id = 0;
    int rw_conflicts = 1;
    bool init = true;
    bool hold_portal_snapshot = false;
    int num_restarts = -1;
    OptimWriteEntry *optim_write_entry;
    int t_delta = 0;
    char saveState[]="saveState";
    char freeState[]="releaseState";
    char snapId[32]="";
    int saveLen = 9; 
    int releaseLen = 12; 
    int matchLen = 0; 
    int sqlOffset = 0;
    int sqlCmdOffset = 0;
    int j = 0;
    char tx_result[1024];
    struct timeval tv1, tv2;
    int condSig = 0;

    is_bcdb_worker = true;

    Assert(tx != NULL);
    tx->worker_pid = pid;
    activeTx = tx;
    tx->block_id_snapshot = tx->block_id_committed - 1;

    LIST_INIT(&ws_table_record);
    LIST_INIT(&rs_table_record);

    tv1.tv_sec = 0; tv2.tv_sec = 0;
    tv1.tv_usec = 0; tv2.tv_usec = 0;
    gettimeofday(&tv1, NULL);
#if SAFEDBG3
    printf(" id= %d  time= %ld.%ld\n",  tx->tx_id, tv1.tv_sec, tv1.tv_usec);
#endif

    if(tx->tx_id == 0) {
    	tx_start_time.tv_sec = 0;
    	tx_start_time.tv_usec = 0;
    	gettimeofday(&tx_start_time, NULL);
    }

#if SAFEDBG2
    printf("safeDbg start pid %d %s : %s: %d "\
           "latest-vs-myId= %d %d dualTab %d \n",
           getpid(), __FILE__, __FUNCTION__, __LINE__ ,
           get_last_committed_txid(tx), tx->tx_id, dualTab);
#endif
    PG_TRY();
    {
      for (;;)
      {
        latest_tx_id = get_last_committed_txid(tx);
#if SAFEDBG3
          printf("safeDbg init %d own-id %d : latest-id %d \n",
                  init, tx->tx_id, latest_tx_id );
#endif
        if(rw_conflicts == 1) {
	  // retry: 
         num_restarts++;
         LIST_INIT(&ws_table_record);
         LIST_INIT(&rs_table_record);

    	  if(init) {
	      activeTx->tx_id_committed =  get_last_committed_txid(tx);
#if SAFEDBG3
              printf("safeDbg %s : %s: %d init tx-id= %d\n",
                     getpid(), __FILE__, __FUNCTION__, __LINE__ ,
                     activeTx->tx_id_committed);
#endif
		  } else {
	      /* Fix: Clear stale optim_write_list before retry.
	       * AbortCurrentTransaction will undo heap/index changes,
	       * but the SIMPLEQ entries allocated in bcdb_tx_context
	       * need to be freed. MemoryContextReset frees the memory,
	       * then SIMPLEQ_INIT resets the head pointer. */
	      while ((optim_write_entry = SIMPLEQ_FIRST(&activeTx->optim_write_list)))
	      {
	          if (optim_write_entry->slot)
	              ExecDropSingleTupleTableSlot(optim_write_entry->slot);
	          SIMPLEQ_REMOVE_HEAD(&activeTx->optim_write_list, link);
	      }
	      MemoryContextReset(bcdb_tx_context);
		      /* Fix: AbortCurrentTransaction properly releases all resources
		       * (buffer pins, TupleDesc refs, locks) via ResourceOwnerRelease
		       * with isCommit=false (silent, no leak warnings).
		       * Then reset_xact_command sets xact_started=false so
		       * start_xact_command will begin a fresh transaction. */
		      AbortCurrentTransaction();
		      reset_xact_command();
		      /*
		       * AbortCurrentTransaction() already runs AtAbort/AtCleanup logic,
		       * which releases and drops portals/resource owners created in the
		       * failed transaction. Any pointers cached in activeTx may now be
		       * stale; never dereference them here.
		       */
		      activeTx->portal = NULL;
		      tx->queryDesc = NULL;
		      tx->sxact = NULL;
		      hold_portal_snapshot = false;
#if SAFEDBG2
	              printf("\n\n safeDbg tx %d rerun due to conflict pid %d %s : %s: %d \n",
	                     tx->tx_id, getpid(), __FILE__, __FUNCTION__, __LINE__ );
#endif
			  }
		  /*
		   * tx_id_committed defines the "baseline" of what was committed when we
		   * take our snapshot/simulate the transaction. On retries we must
		   * refresh this baseline, otherwise conflict_checkDT will repeatedly
		   * flag already-committed txs as conflicts and can livelock.
		   */
		  activeTx->tx_id_committed = get_last_committed_txid(tx);
          init = false;
	  XactIsoLevel = tx->isolation;
      start_xact_command();
	  tx->status = TX_EXECUTING;

#if SAFEDBG2
          printf("safeDbg pid %d %s : %s: %d \n",
                  getpid(), __FILE__, __FUNCTION__, __LINE__ );
#endif
		  old_owner = CurrentResourceOwner;
		  snapshot = GetTransactionSnapshot();	 // get snapshot
		  tx->sxact = ShareSerializableXact();
		  tx->sxact->bcdb_tx = tx;
		  get_write_set(tx, snapshot); 	//  does CreatePortal
		  bcdb_maybe_enqueue_deferred_delete0_by_key(tx);
#if SAFEDBG1
		  printf("safeDbg pid %d %s : %s: %d \n",
			  getpid(), __FILE__, __FUNCTION__, __LINE__ );
#endif
		  CurrentResourceOwner = activeTx->portal->resowner;
		  if (tx->queryDesc != NULL)
		  {
			  ExecutorFinish(tx->queryDesc);
			  ExecutorEnd(tx->queryDesc);
			  FreeQueryDesc(tx->queryDesc);
			  tx->queryDesc = NULL;
		  }
		  CurrentResourceOwner = old_owner;
		  hold_portal_snapshot = true;
#if SAFEDBG1
		  printf("safeDbg pid %d %s : %s: %d \n",
			  getpid(), __FILE__, __FUNCTION__, __LINE__ );
#endif
	  tx->status = TX_WAIT_FOR_COMMIT;

	}
        init = false;

#if SAFEDBG2
        gettimeofday(&tv2, NULL);
        printf("safeDbg waiting pid %d %s : %s: %d latest-vs-myId= %d %d tx-bid %d\n",
               getpid(), __FILE__, __FUNCTION__, __LINE__ ,
               latest_tx_id, tx->tx_id, tx->block_id_committed);
        printf("safeDbg id= %d  wait-time= %ld.%ld\n",   tx->tx_id,
		tx_start_time.tv_sec , tx_start_time.tv_usec) ;
#endif
        block = get_block_by_id(1, false);
        Assert(block != NULL);
        latest_tx_id = get_last_committed_txid(tx) ;
        if(dualTab) {
#if SAFEDBG1
           printf("\n *** safeDbg pid %d waiting %s : %s: %d latest-vs-myId= %d %d  blksz= %d *** \n",
               getpid(), __FILE__, __FUNCTION__, __LINE__ ,
               latest_tx_id, tx->tx_id, 2*block->blksize);
#endif
#if SAFEDBG2
           WaitConditionPidDbg(&block->cond, getpid(), 
#else
           //WaitConditionPidTimeout(&block->cond, getpid(), 1500,
           WaitConditionPid(&block->cond, getpid(), 
#endif
                        ( ( get_last_committed_txid(tx)+1)  == tx->tx_id ));
           strcpy(tx_result, block->result[(tx->tx_id)%(2*block->blksize)]);

#if SAFEDBG2
           printf("safeDbg blk read result = %s\n", tx_result);
           printf("safeDbg worker read result at %d= %s\n", ((tx->tx_id)%(2*block->blksize)), block->result[(tx->tx_id)%(2*block->blksize)]);
#endif
           rw_conflicts = conflict_checkDT();
                    // conflict_check();
        } else {
       	    printf("\n\n safeDbg **ERROR not dualtab ** pid %d %s : %s: %d \n",
                 getpid(), __FILE__, __FUNCTION__, __LINE__ );
        } 
        if (rw_conflicts == 1)
            continue;

        tx->status = TX_COMMITTING;
#if SAFEDBG2
      printf(" safeDbg pid %d %s : %s: %d tx %d \n",
                 getpid(), __FILE__, __FUNCTION__, __LINE__, tx->tx_id );
#endif
      publish_ws_tableDT(tx->tx_id) ; // HASHTAB_SWITCH_THRESHOLD 
					  
#if SAFEDBG2
      printf(" safeDbg pid %d %s : %s: %d tx %d \n",
                 getpid(), __FILE__, __FUNCTION__, __LINE__, tx->tx_id );
#endif
      /* CRITICAL FIX: Do NOT advance counter before apply_optim_writes.
       * Old code had set_last_committed_txid(tx) HERE, which woke the
       * next transaction before our writes were applied/committed,
       * causing concurrent serial execution and Merkle corruption.
       * Counter is now advanced AFTER finish_xact_command() below. */
      if (!apply_optim_writes())
      {
          /*
           * An INSERT failed (e.g. duplicate key because tx_id ordering
           * didn't match workload line ordering).  Abort this attempt
           * and retry the whole transaction with a fresh snapshot.
           *
           * It's safe to retry even after publish_ws_tableDT because:
           * 1) No other tx has entered serial yet (counter not advanced)
           * 2) Our stale published tags won't false-match ourselves
           *    (table_checkDT checks entry->tx_id < activeTx->tx_id)
           * 3) AbortCurrentTransaction rolls back partial apply_optim
           *    writes done before the failure
           */
          rw_conflicts = 1;
          continue;
      }
      tx->sxact->flags |= SXACT_FLAG_PREPARED;

      DEBUGMSG("[ZL] worker(%d) commiting tx(%s)", getpid(), tx->hash);
      tx->status = TX_COMMITED;

#if SAFEDBG2
      printf("safeDbg %s : %s: %d latest-vs-myId= %d %d \n",
		__FILE__, __FUNCTION__, __LINE__ , latest_tx_id, tx->tx_id);
#endif

      SpinLockAcquire(restart_counter_lock);
      total_num_restarts += num_restarts;
      SpinLockRelease(restart_counter_lock);

      gettimeofday(&tv2, NULL);
      t_delta = (tv2.tv_usec - tv1.tv_usec) ;
      if(t_delta < 0) t_delta += 1000000;

#if SAFEDBG1
      printf("\nsafeDbg id= %d start= %ld.%ld finish= %ld.%ld restarts= %d exec= %d tx= %s\n",
        tx->tx_id, tv1.tv_sec , tv1.tv_usec, tv2.tv_sec, tv2.tv_usec, total_num_restarts, t_delta, tx->sql);
#endif
      t_sinceTx1 += t_delta; 

    if((tx->tx_id)%1000 < 3*NUM_WORKERS) {
        fp = fopen("/tmp/timestamps.txt","a");
        fprintf(fp, "id= %d, pid= %d restarts=%d total_num_restarts= %d "
#if SAFEDBG1
			"start-time= %ld.%ld finish time= %ld.%ld " 
#endif
			"exectime(us)= %d totalTime(us)= %d\n", 
		tx->tx_id, getpid(), num_restarts, total_num_restarts,
#if SAFEDBG1
		tv1.tv_sec , tv1.tv_usec, tv2.tv_sec, tv2.tv_usec,
#endif
		t_delta, t_sinceTx1);
    fclose(fp);
    }

      /* CRITICAL FIX: Broadcast moved to after finish_xact_command
       * to prevent next tx from starting serial phase before our
       * writes are committed. condSig set after broadcast below. */

      // For tx "select saveState()" char 't' should be seen by offset 9
      for(sqlOffset = 0; sqlOffset < saveLen ; sqlOffset++) {
         if(tx->sql[sqlOffset] == 't') break;
         if(tx->sql[sqlOffset] == 'T') break;
      }
      matchLen = 0;
      sqlOffset += 2; // chars t and space
      sqlCmdOffset = sqlOffset; // chars t and space
      for(; sqlOffset < 2*saveLen ; sqlOffset++) {
         if((tx->sql[sqlOffset] == '\0') || ('\0' == saveState[matchLen]))
            break;
         if(tx->sql[sqlOffset] != saveState[matchLen]) break;
         matchLen++;
      }
#if SAFEDBG3
      printf("safeDbg tx matchLen = %d \n", matchLen);
#endif
      if(matchLen == saveLen) {
#if SAFEDBG3
          printf("\n safeDbg ** saveState tx detected !! ** pid %d %s : %s: %d \n",
             getpid(), __FILE__, __FUNCTION__, __LINE__ );
#endif
          sqlOffset += 2; // chars ('
          sqlCmdOffset = sqlOffset; // chars ('
          for( j = 0 ; j < 32 ; j++, sqlOffset++) {
             if(tx->sql[sqlOffset] == '\'') break;
          }
          strncpy(snapId, &(tx->sql[sqlCmdOffset]), j); // why exception...
          block->snapTid = tx->tx_id;
#if SAFEDBG3
          printf("safeDbg tx save snapId = %s blk snapTid= %d myPid= %d \n", snapId, block->snapTid, getpid());
#endif
          WaitConditionTimeoutPid(&block->condRecovery, ( block->snapTid != tx->tx_id), 500000, getpid() );
#if SAFEDBG3
          printf("\nsafeDbg  ** saveState tx releasing!! ** pid %d %s : %s: %d \n",
             getpid(), __FILE__, __FUNCTION__, __LINE__ );
#endif
          matchLen = 0;
      }
      else for( sqlOffset = sqlCmdOffset, matchLen = 0; sqlOffset < 2*releaseLen ; sqlOffset++) {
         if((tx->sql[sqlOffset] == '\0') || ('\0' == freeState[matchLen]))
            break;
         if(tx->sql[sqlOffset] != freeState[matchLen]) break;
         matchLen++;
      }
      if(matchLen == releaseLen) {
#if SAFEDBG3
          printf("\n safeDbg ** releaseState tx detected !! ** pid %d %s : %s: %d \n",
             getpid(), __FILE__, __FUNCTION__, __LINE__ );
#endif
          sqlOffset += 2; // chars ('
          sqlCmdOffset = sqlOffset; // chars ('
          for( j = 0 ; j < 32 ; j++, sqlOffset++) {
             if(tx->sql[sqlOffset] == '\'') break;
          }
          strncpy(snapId, &(tx->sql[sqlCmdOffset]), j); // is it exception...
#if SAFEDBG3
          printf("safeDbg tx release snapId = %s blk snapTid= %d myPid= %d \n", snapId, block->snapTid, getpid());
#endif
          block->snapTid = getpid();
          ConditionVariableBroadcast(&block->condRecovery);
      }
#if SAFEDBG3
      printf("safeDbg tx matchLen = %d \n", matchLen);
#endif
      int mem_txid = ( (tx->tx_id) % (block->blksize));

      /* Fix: Silently release portal resources before PortalDrop to avoid
       * leak warnings. ResourceOwnerRelease with isCommit=false releases
       * remaining buffer pins and TupleDesc refs. */
      if(hold_portal_snapshot) {
	          if (activeTx->portal->resowner) {
	              ResourceOwnerRelease(activeTx->portal->resowner,
	                                   RESOURCE_RELEASE_BEFORE_LOCKS, false, false);
	              ResourceOwnerRelease(activeTx->portal->resowner,
	                                   RESOURCE_RELEASE_LOCKS, false, false);
	              ResourceOwnerRelease(activeTx->portal->resowner,
	                                   RESOURCE_RELEASE_AFTER_LOCKS, false, false);
	              ResourceOwnerDelete(activeTx->portal->resowner);
	              activeTx->portal->resowner = NULL;
	          }
		  PortalDrop(activeTx->portal, false);
		  activeTx->portal = NULL;
	          hold_portal_snapshot = false;
	      }
      /* Release TopTransactionResourceOwner's direct resources.
       * CRITICAL: isTopLevel (4th param) MUST be true since this is a
       * top-level transaction. Using false triggers Assert(owner->parent != NULL)
       * at resowner.c:582 during RESOURCE_RELEASE_LOCKS, because it tries
       * retail lock release on child owners expecting subtransaction semantics.
       * Standard PG always uses isTopLevel=true for TopTransactionResourceOwner
       * (see CommitTransaction/AbortTransaction in xact.c). */
      ResourceOwnerRelease(TopTransactionResourceOwner,
                           RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
      ResourceOwnerRelease(TopTransactionResourceOwner,
                           RESOURCE_RELEASE_LOCKS, false, true);
      ResourceOwnerRelease(TopTransactionResourceOwner,
                           RESOURCE_RELEASE_AFTER_LOCKS, false, true);
      finish_xact_command();

      /* CRITICAL FIX: Advance counter and broadcast ONLY AFTER commit.
       * This ensures the next transaction cannot enter its serial phase
       * until our writes are fully committed and visible. */
      set_last_committed_txid(tx);
      ConditionVariableBroadcast(&block->cond);
      condSig = 1;

      memset(&block->result[mem_txid], 0, 1024);
#if SAFEDBG3
      printf("safeDbg txid= %d mem-txid = %d \n", tx->tx_id, mem_txid);
       
      printf("\n\n ** safeDbg pid %d %s : %s: %d tx sql %s \n",
             getpid(), __FILE__, __FUNCTION__, __LINE__ , tx->sql);
#endif
      if((tx->tx_id)%2000 < 2)
      printf("\n *** safeDbg pid %d signaling %s : %s: %d "
	"latest-vs-myId= %d %d myquery= %s\n", getpid(), __FILE__, __FUNCTION__,
		__LINE__ , get_last_committed_txid(tx), tx->tx_id, tx->sql);

      int read_cmd = chk_query_type( tx->sql, "select", "SELECT");
      int del_cmd = chk_query_type( tx->sql, "delete", "DELETE");
      int upd_cmd = chk_query_type( tx->sql, "update", "UPDATE");
      int ins_cmd = chk_query_type( tx->sql, "insert", "INSERT");

      if(read_cmd == 1)
        sprintf(&block->result[mem_txid], tx_result);
      else if(ins_cmd == 1)
        sprintf(&block->result[mem_txid],"\n\t*%s* INSERT 0 1\n", tx->hash);
      else if(upd_cmd == 1)
        sprintf(&block->result[mem_txid],"\n\t*%s* UPDATE 0 1\n", tx->hash);
      else if(del_cmd == 1)
        sprintf(&block->result[mem_txid],"\n\t*%s* DELETE 0 1\n", tx->hash);

#if SAFEDBG3
      //printf("blkmid read result at %d= %s\n", ((tx->tx_id)%(2*block->blksize)), block->result[(tx->tx_id)%(2*block->blksize)]); // does it get overwritten
      printf("safeDbg blk read result = %s\n", tx_result);
      //sprintf(&block->result[tx->tx_id],"\n\t*%s* user | field0 | field1 |\n", tx->hash);
#endif

#if SAFEDBG1
      printf("\n *** safeDbg pid %d endcmd %s : %s: %d "
            "latest-vs-myId= %d %d \n\n", getpid(), __FILE__, __FUNCTION__,
		__LINE__ , get_last_committed_txid(tx), tx->tx_id);
#endif
      EndCommand(completionTag, dest);
      delete_tx(tx);
      MemoryContextReset(bcdb_tx_context);
      break;
      }
    }
    PG_CATCH();
    {
      ConditionVariableCancelSleep();
        printf("safeDbg pg-catch() pid %d %s : %s: %d  tx %d %s \n",
			    getpid(), __FILE__, __FUNCTION__, __LINE__, tx->tx_id, tx->sql );
      //goto retry;

      //print_trace();
      /* Fix: Do NOT call finish_xact_command() here — it tries to commit
       * a transaction in error state, causing cascading warnings.
       * After PG_RE_THROW(), PostgresMain's sigsetjmp handler will call
       * AbortCurrentTransaction() which properly cleans up all resources. */
      if(condSig == 0) {
              BCBlock     *block2 = get_block_by_id(1, false);
	      ConditionVariableBroadcast(&block2->cond);
      }
		      if (hold_portal_snapshot && activeTx && activeTx->portal)
		      {
		          if (activeTx->portal->resowner)
		          {
	              ResourceOwnerRelease(activeTx->portal->resowner,
	                                   RESOURCE_RELEASE_BEFORE_LOCKS, false, false);
	              ResourceOwnerRelease(activeTx->portal->resowner,
	                                   RESOURCE_RELEASE_LOCKS, false, false);
	              ResourceOwnerRelease(activeTx->portal->resowner,
	                                   RESOURCE_RELEASE_AFTER_LOCKS, false, false);
	              ResourceOwnerDelete(activeTx->portal->resowner);
	              activeTx->portal->resowner = NULL;
	          }
	          PortalDrop(activeTx->portal, false);
		          activeTx->portal = NULL;
		          hold_portal_snapshot = false;
		      }

	      while (activeTx &&
	             (optim_write_entry = SIMPLEQ_FIRST(&activeTx->optim_write_list)))
	      {
	          if (optim_write_entry->slot)
	              ExecDropSingleTupleTableSlot(optim_write_entry->slot);
	          SIMPLEQ_REMOVE_HEAD(&activeTx->optim_write_list, link);
	      }

	      MemoryContextReset(bcdb_tx_context);
	      delete_tx(tx);
	      activeTx = NULL;
      PG_RE_THROW();
    }
    PG_END_TRY();

}

void
bcdb_worker_process_tx(BCDBShmXact *tx)
{
    BCBlock     *block = NULL;
    Snapshot     snapshot;
    ResourceOwner old_owner;
    int32        num_ready;
    int32        num_finished;
    bool         timing = rand() % 10 == 0;

    Assert(tx != NULL);
    tx->worker_pid = pid;
    activeTx = tx;
    DEBUGMSG("[ZL] tx %s scheduled on worker: %d", tx->hash, tx->worker_pid);

    WaitGlobalBmin(tx->block_id_committed);
    tx->block_id_snapshot = tx->block_id_committed - 1;

    DEBUGMSG("[ZL] tx %s snapshot %d", tx->hash, tx->block_id_snapshot);
    LIST_INIT(&ws_table_record);
    LIST_INIT(&rs_table_record);

    PG_TRY();
    {
        XactIsoLevel = tx->isolation;
        tx->status = TX_EXECUTING;
        if (timing)
            tx->start_simulation_time = bcdb_get_time();
        start_xact_command();
        snapshot = GetTransactionSnapshot();
		tx->sxact = ShareSerializableXact();
		tx->sxact->bcdb_tx = tx;
		get_write_set(tx, snapshot);
		old_owner = CurrentResourceOwner;
		CurrentResourceOwner = activeTx->portal->resowner;
		if (tx->queryDesc != NULL)
		{
			ExecutorFinish(tx->queryDesc);
			ExecutorEnd(tx->queryDesc);
			FreeQueryDesc(tx->queryDesc);
			tx->queryDesc = NULL;
		}
		CurrentResourceOwner = old_owner;
		PortalDrop(activeTx->portal, false);

        if (block == NULL)
        {
            block = get_block_by_id(tx->block_id_committed, false);
            Assert(block != NULL);
        }
        tx->status = TX_WAIT_FOR_COMMIT;

    	num_ready = __sync_add_and_fetch(&block->num_ready, 1);
        DEBUGMSG("[ZL] tx %s waiting with num_ready: %d", tx->hash, num_ready);
        if (num_ready == block->num_tx)
            ConditionVariableBroadcast(&block->cond);

        if (timing)
            tx->end_simulation_time = bcdb_get_time();

        WaitCondition(&block->cond, block->num_tx == block->num_ready);

        if (timing)
            tx->start_checking_time = bcdb_get_time();

        tx->status = TX_COMMITTING;

        if (SxactIsDoomed(activeTx->sxact))
			ereport(ERROR,
				(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
				 errmsg("doomed (tx: %s) after conflict check", activeTx->hash),
				 errdetail_internal("probably failed during conflict checking.")));

        conflict_check();
        if (timing)
            tx->end_checking_time = bcdb_get_time();
        /* Note: ignoring apply_optim_writes return value in non-DT path;
         * the non-DT path uses conflict_check() which is less strict. */
        apply_optim_writes();
        if (timing)
            tx->end_local_copy_time = bcdb_get_time();
        tx->sxact->flags |= SXACT_FLAG_PREPARED;
        finish_xact_command();

        DEBUGMSG("[ZL] worker(%d) commiting tx(%s)", getpid(), tx->hash);
        tx->status = TX_COMMITED;
        num_finished = __sync_add_and_fetch(&block->num_finished, 1);
        DEBUGMSG("[ZL] tx %s finishing with num_finish: %d", tx->hash, num_finished);

        if (num_finished == block->num_tx)
        {
            uint32 global_bmin;
            int32  block_committed = 0;
            BCDBShmXact *tx_iter;
            unsigned char tx_state_hash[SHA256_DIGEST_LENGTH];
            SHA256_CTX block_state_hash;
            unsigned char final_block_state_hash[SHA256_DIGEST_LENGTH];
            char *block_hash_b64;

            clean_rs_ws_table();
            global_bmin = __sync_add_and_fetch(&block_meta->global_bmin, 1);
            
            ConditionVariableBroadcast(&block_meta->conds[global_bmin % NUM_BMIN_COND]);
            DEBUGMSG("[ZL] tx %s incrementing bmin to: %d", activeTx->hash, global_bmin);
            SHA256_Init(&block_state_hash);

            for (int i=0; i < block->num_tx; i++)
            {
                tx_iter = block->txs[i];
                if (tx_iter->status == TX_COMMITED)
                {
                    block_committed += 1;
                    SHA256_Final(tx_state_hash, &tx_iter->state_hash);
                    SHA256_Update(&block_state_hash, tx_state_hash, SHA256_DIGEST_LENGTH);
                }
            }
            SHA256_Final(final_block_state_hash, &block_state_hash);
            Base64Encode(final_block_state_hash, SHA256_DIGEST_LENGTH, &block_hash_b64);
            ereport(LOG, (errmsg("[ZL] block %d hash: %s", block->id, block_hash_b64)));
    	    __sync_fetch_and_add(&block_meta->num_committed, block_committed);
    	    __sync_fetch_and_add(&block_meta->num_aborted, block->num_tx - block_committed);
            block_cleaning(block->id);
        }
        if (tx->create_time != 0)
            tx->commit_time = bcdb_get_time();

        if (timing)
            tx->commit_time = bcdb_get_time();
        MemoryContextReset(bcdb_tx_context);
        if (timing)
            ereport(LOG, (errmsg("[ZL] tx (%s) runtime: %lu, parsing: %lu, simulation: %lu, wait: %lu, check: %lu, apply: %lu, commit %lu", tx->hash, 
                                tx->commit_time - tx->start_simulation_time,
                                tx->end_parsing_time - tx->start_parsing_time,
                                tx->end_simulation_time - tx->start_simulation_time,
                                tx->start_checking_time - tx->end_simulation_time,
                                tx->end_checking_time - tx->start_checking_time,
                                tx->end_local_copy_time - tx->end_checking_time,
                                tx->commit_time - tx->end_local_copy_time)));  
        if (tx->create_time != 0)
            ereport(LOG, (errmsg("[ZL] tx (%s) latency: %lu", tx->hash, tx->commit_time - tx->create_time)));  
    }
    PG_CATCH();
    {
		OptimWriteEntry *optim_write_entry;

        while ((optim_write_entry = SIMPLEQ_FIRST(&activeTx->optim_write_list)))
        {
            ExecDropSingleTupleTableSlot(optim_write_entry->slot);
            SIMPLEQ_REMOVE_HEAD(&activeTx->optim_write_list, link);
        }
        PG_RE_THROW();
    }
    PG_END_TRY();
}


void
bcdb_on_worker_exit(int code, Datum arg)
{
    /* clean up here */
}
