/*-------------------------------------------------------------------------
 *
 * merkle.c
 *    Merkle tree integrity index access method - main handler
 *
 * This file implements the IndexAmRoutine handler function that returns
 * the callback function pointers for the merkle access method.
 *
 * IDENTIFICATION
 *    src/backend/access/merkle/merkle.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/amapi.h"
#include "access/merkle.h"
#include "access/reloptions.h"
#include "optimizer/cost.h"
#include "utils/builtins.h"
#include "utils/index_selfuncs.h"

/* GUC: Enable/disable Merkle index updates */
bool enable_merkle_index = true;
/* GUC: Emit NOTICE lines for touched Merkle nodes on commit */
bool merkle_update_detection = false;
/*
 * GUC: Suppress Merkle update-detection output during Merkle index builds
 * (CREATE INDEX / REINDEX).
 *
 * When enabled, Merkle index builds will not emit the touched-node report even
 * if merkle_update_detection is on. Default is enabled to avoid noisy output.
 */
bool merkle_update_detection_suppress = true;
/* Internal: suppress undo tracking in non-DML contexts (e.g. index build) */
bool merkle_undo_suppress = false;

/*
 * Merkle index reloption definitions using standard framework
 */
static relopt_kind merkle_relopt_kind;
static bool merkle_relopts_registered = false;

static bool
merkle_is_power_of(int value, int base)
{
    if (value < 1 || base < 2)
        return false;

    while ((value % base) == 0)
        value /= base;

    return (value == 1);
}

/*
 * merkle_register_relopts() - Register merkle reloptions with PostgreSQL
 *
 * This should be called once to register our options.
 */
static void
merkle_register_relopts(void)
{
    if (merkle_relopts_registered) /* already registered */
        return;
    
    merkle_relopt_kind = add_reloption_kind();
    
    add_int_reloption(merkle_relopt_kind, "partitions",
                      "Number of partitions in the merkle index",
                      MERKLE_NUM_PARTITIONS, 1, 10000, AccessExclusiveLock);
    
    add_int_reloption(merkle_relopt_kind, "leaves_per_partition",
                      "Number of leaves per partition (must be power of fanout)",
                      MERKLE_LEAVES_PER_PARTITION, 2, 1024, AccessExclusiveLock);

    add_int_reloption(merkle_relopt_kind, "fanout",
                      "Branching factor (children per internal node)",
                      MERKLE_DEFAULT_FANOUT, 2, 1024, AccessExclusiveLock);
    
    merkle_relopts_registered = true;
}

/* Reloption parsing table */
static relopt_parse_elt merkle_relopt_tab[] = {
    {"partitions", RELOPT_TYPE_INT, offsetof(MerkleOptions, partitions)},
    {"leaves_per_partition", RELOPT_TYPE_INT, offsetof(MerkleOptions, leaves_per_partition)},
    {"fanout", RELOPT_TYPE_INT, offsetof(MerkleOptions, fanout)}
};

/*
 * merkle_options() - Parse reloptions for merkle index
 *
 * This is called during CREATE INDEX to parse WITH clause options.
 */
bytea *
merkle_options(Datum reloptions, bool validate)
{
    MerkleOptions *opts;
    
    /* Ensure our reloptions are registered */
    merkle_register_relopts();
    
    opts = (MerkleOptions *) build_reloptions(reloptions, validate,
                                               merkle_relopt_kind,
                                               sizeof(MerkleOptions),
                                               merkle_relopt_tab,
                                               lengthof(merkle_relopt_tab));
    
    if (validate && opts != NULL)
    {
        if (opts->fanout < 2 || opts->fanout > 1024)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("fanout must be between 2 and 1024")));
        }

        /* Check if leaves_per_partition is a power of fanout */
        if (!merkle_is_power_of(opts->leaves_per_partition, opts->fanout))
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("leaves_per_partition must be a power of fanout"),
                     errhint("For fanout=%d, suggested values: %d, %d, %d, ...",
                             opts->fanout,
                             opts->fanout,
                             opts->fanout * opts->fanout,
                             opts->fanout * opts->fanout * opts->fanout)));
        }
    }
    
    return (bytea *) opts;
}

/*
 * merkle_get_options() - Extract options from index relation
 *
 * Returns MerkleOptions with user settings or defaults if not set.
 */
MerkleOptions *
merkle_get_options(Relation indexRel)
{
    MerkleOptions *opts;
    bytea *relopts;
    
    relopts = indexRel->rd_options;
    if (relopts == NULL)
    {
        /* No options specified, return defaults */
        opts = (MerkleOptions *) palloc0(sizeof(MerkleOptions));
        SET_VARSIZE(opts, sizeof(MerkleOptions));
        opts->partitions = MERKLE_NUM_PARTITIONS;
        opts->leaves_per_partition = MERKLE_LEAVES_PER_PARTITION;
        opts->fanout = MERKLE_DEFAULT_FANOUT;
        return opts;
    }
    
    /*
     * Options were stored - copy and validate.
     * The options are stored with local_reloptions format which includes
     * a varlena header followed by the option values at their defined offsets.
     */
    opts = (MerkleOptions *) palloc0(sizeof(MerkleOptions));
    memcpy(opts, relopts, Min(VARSIZE(relopts), sizeof(MerkleOptions)));
    SET_VARSIZE(opts, sizeof(MerkleOptions));

    /* Backward compatibility: older rd_options blobs won't have fanout */
    if (VARSIZE(relopts) < (offsetof(MerkleOptions, fanout) + sizeof(int)))
        opts->fanout = MERKLE_DEFAULT_FANOUT;
    
    /* Validate options - if values look corrupt, use defaults */
    if (opts->partitions <= 0 || opts->partitions > 10000 ||
        opts->leaves_per_partition <= 0 || opts->leaves_per_partition > 1024 ||
        opts->fanout < 2 || opts->fanout > 1024 ||
        !merkle_is_power_of(opts->leaves_per_partition, opts->fanout))
    {
        opts->partitions = MERKLE_NUM_PARTITIONS;
        opts->leaves_per_partition = MERKLE_LEAVES_PER_PARTITION;
        opts->fanout = MERKLE_DEFAULT_FANOUT;
    }
    
    return opts;
}

PG_FUNCTION_INFO_V1(merklehandler);

/*
 * merklehandler() - Return IndexAmRoutine for merkle access method
 *
 * This is the entry point that PostgreSQL calls when loading the access method.
 * We return a structure containing pointers to all the callback functions
 * that implement the Merkle index operations.
 */
Datum
merklehandler(PG_FUNCTION_ARGS)
{
    IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);
    
    /* Ensure reloptions are registered when AM is loaded */
    merkle_register_relopts();

    /*
     * Index properties
     * 
     * The Merkle index is NOT a traditional search index - it's for
     * integrity verification. So most search-related properties are false.
     */
    amroutine->amstrategies = 0;            /* no operator strategies */
    amroutine->amsupport = 0;               /* no support functions (partition logic is inline) */
    amroutine->amcanorder = false;          /* cannot order results */
    amroutine->amcanorderbyop = false;      /* no ordering operators */
    amroutine->amcanbackward = false;       /* no backward scans */
    amroutine->amcanunique = false;         /* not for uniqueness */
    amroutine->amcanmulticol = true;        /* multi-column keys supported */
    amroutine->amoptionalkey = true;        /* key is optional for scan */
    amroutine->amsearcharray = false;       /* no array searches */
    amroutine->amsearchnulls = false;       /* no null searches */
    amroutine->amstorage = false;           /* no special storage */
    amroutine->amclusterable = false;       /* cannot cluster on */
    amroutine->ampredlocks = false;         /* no predicate locks */
    amroutine->amcanparallel = false;       /* no parallel scans */
    amroutine->amcaninclude = false;        /* no included columns */
    amroutine->amkeytype = InvalidOid;      /* no specific key type */

    /*
     * Callback functions
     */
    /* Build functions */
    amroutine->ambuild = merkleBuild;
    amroutine->ambuildempty = merkleBuildempty;
    
    /* Insert/delete functions */
    amroutine->aminsert = merkleInsert;
    amroutine->ambulkdelete = merkleBulkdelete;
    amroutine->amvacuumcleanup = merkleVacuumcleanup;
    
    /* Scan functions - NOT SUPPORTED for Merkle index */
    /* 
     * The Merkle index does not support traditional index scans.
     * Verification is done through explicit SQL functions (merkle_verify, etc.)
     * which read the index pages directly via ReadBuffer().
     */
    amroutine->amcanreturn = NULL;          /* no index-only scans */
    amroutine->amcostestimate = merkleCostEstimate;
    amroutine->amoptions = merkle_options;  /* parse partitions, leaves_per_partition */
    amroutine->amproperty = NULL;           /* no special properties */
    amroutine->ambuildphasename = NULL;     /* no build phases */
    amroutine->amvalidate = NULL;           /* no opclass validation needed */
    amroutine->ambeginscan = NULL;          /* no scan support */
    amroutine->amrescan = NULL;             /* no scan support */
    amroutine->amgettuple = NULL;           /* no scan support */
    amroutine->amgetbitmap = NULL;          /* no bitmap scans */
    amroutine->amendscan = NULL;            /* no scan support */
    amroutine->ammarkpos = NULL;            /* no mark/restore */
    amroutine->amrestrpos = NULL;
    
    /* Parallel scan functions */
    amroutine->amestimateparallelscan = NULL;
    amroutine->aminitparallelscan = NULL;
    amroutine->amparallelrescan = NULL;

    PG_RETURN_POINTER(amroutine);
}

/*
 * merkleCostEstimate() - Estimate cost of scanning merkle index
 *
 * Since the merkle index is not used for searching but for verification,
 * we return minimal costs. The optimizer should never choose this index
 * for actual query processing.
 */
void
merkleCostEstimate(struct PlannerInfo *root,
                   struct IndexPath *path,
                   double loop_count,
                   Cost *indexStartupCost,
                   Cost *indexTotalCost,
                   Selectivity *indexSelectivity,
                   double *indexCorrelation,
                   double *indexPages)
{
    /*
     * Return very high costs so the optimizer never chooses this
     * for normal query processing. The merkle index is only for
     * integrity verification through explicit function calls.
     */
    *indexStartupCost = 1.0e10;
    *indexTotalCost = 1.0e10;
    *indexSelectivity = 0.0;
    *indexCorrelation = 0.0;
    *indexPages = 1;
}
