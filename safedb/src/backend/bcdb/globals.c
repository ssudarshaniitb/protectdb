/*-------------------------------------------------------------------------
 *
 * bcdb_global.c
 *    global variables for blockchainDB
 *
 *
 * IDENTIFICATION
 *	  src/backend/bcdb/globals.c
 *
 *-------------------------------------------------------------------------
 */
#include "bcdb/globals.h"
#include <time.h>
#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/buffer.h>
#include <stdint.h>

bool    is_bcdb_master = false;
bool    is_bcdb_worker = false;
bool    skip_conflict_checking = false;
int     gdb_pause_sig = 0;
char*   bcdb_host;
char*   bcdb_port;
bool    OEP_mode = false;
int32   bcdb_worker_count = BCDB_DEFAULT_WORKER_COUNT;
bool    bcdb_dt_conflict_tracking = false;
bool    bcdb_dt_completion_only_skip_reads = false;
int32   bcdb_serial_gate_mode = BCDB_SERIAL_GATE_MODE_POLL;
int32   bcdb_dt_hashtab_switch_threshold = BCDB_DEFAULT_HASHTAB_SWITCH_THRESHOLD;
int32   bcdb_result_ring_slots = BCDB_DEFAULT_RESULT_RING_SLOTS;
bool    bcdb_advance_commit_watermark = true;
int32   bcdb_serial_gate_source = BCDB_GATE_SRC_PUBLISHED_MAX;
char*   bcdb_client_public_key = NULL;
bool    bcdb_enforce_signatures = false;
bool    bcdb_gate_snapshot_each_block = false;
/*
 * bcdb_gate_telemetry_enabled — default off.
 *
 * Can be overridden at server startup via the GUC
 *   bcdb_gate_telemetry = on
 * which serves as the sole control plane inside PostgreSQL.
 * It is intentionally NOT reloadable at runtime (shared-memory stat shards
 * are sized once at startup, and toggling mid-run would produce incoherent
 * partial totals).
 */
bool    bcdb_gate_telemetry_enabled = false;
pid_t   pid;
BcdbIsolationLevel BcdbCurrentIsolationLevel = BCDB_SERIALIZABLE;
int32         worker_id;

uint64
bcdb_get_time()
{
    struct timespec time_spec;
    uint64 ret;
    clock_gettime(CLOCK_REALTIME, &time_spec);
    ret = (uint64)(time_spec.tv_sec) * (uint64)1e6; 
    ret += (uint64)time_spec.tv_nsec / (uint64)1e3;
    return ret;
}

int Base64Encode(const unsigned char* buffer, size_t length, char** b64text) 
{ 
	BIO *bio, *b64;
	BUF_MEM *bufferPtr;

	b64 = BIO_new(BIO_f_base64());
	bio = BIO_new(BIO_s_mem());
	bio = BIO_push(b64, bio);

	BIO_set_flags(bio, BIO_FLAGS_BASE64_NO_NL);
	BIO_write(bio, buffer, length);
	BIO_flush(bio);
	BIO_get_mem_ptr(bio, &bufferPtr);
    *b64text = (char*) palloc((bufferPtr->length + 1) * sizeof(char));
    memcpy(*b64text, bufferPtr->data, bufferPtr->length);
    (*b64text)[bufferPtr->length] = '\0';
	BIO_set_close(bio, BIO_CLOSE);
	BIO_free_all(bio);

	return (0); //success
}
