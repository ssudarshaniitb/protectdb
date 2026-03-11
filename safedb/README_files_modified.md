
# Our modifications (on top of ARIABC changes) for Deterministic execution :

/src/backend/access/heap/heapam.c
[apply merkle index ](https://github.com/ssudarshaniitb/protectdb/blob/12f9095e88fd4f60767ed3e44ebc55666f4ddcf1/safedb/src/backend/access/heapam.c#L1851)

minor /src/backend/access/heap/heapam_visibility.c

Reset transaction state /src/backend/access/transam/xact.c

/src/backend/bcdb/shm_block.c
[Initialize shared memory structures, track last committed tx-ID](https://github.com/ssudarshaniitb/protectdb/blob/12f9095e88fd4f60767ed3e44ebc55666f4ddcf1/safedb/src/backend/bcdb/shm_block.c#L10)

/src/backend/bcdb/shm_transaction.c

1. [Conflict check with other transaction](https://github.com/ssudarshaniitb/protectdb/blob/12f9095e88fd4f60767ed3e44ebc55666f4ddcf1/safedb/src/backend/bcdb/shm_transaction.c#L1678)

2. [Publish write-set ](https://github.com/ssudarshaniitb/protectdb/blob/12f9095e88fd4f60767ed3e44ebc55666f4ddcf1/safedb/src/backend/bcdb/shm_transaction.c#L1794)

/src/backend/bcdb/worker.c

1. [Initialize worker](https://github.com/ssudarshaniitb/protectdb/blob/12f9095e88fd4f60767ed3e44ebc55666f4ddcf1/safedb/src/backend/bcdb/worker.c#L345)

2. [Core worker logic](https://github.com/ssudarshaniitb/protectdb/blob/12f9095e88fd4f60767ed3e44ebc55666f4ddcf1/safedb/src/backend/bcdb/worker.c#L687)

3. [Get write-set of transaction](https://github.com/ssudarshaniitb/protectdb/blob/12f9095e88fd4f60767ed3e44ebc55666f4ddcf1/safedb/src/backend/bcdb/worker.c#L371)


/src/backend/executor/execMain.c

/src/backend/executor/nodeModifyTable.c

/src/backend/libpq/pqmq.c

/src/backend/main/main.c

/src/backend/nodes/tidbitmap.c

/src/backend/postmaster/postmaster.c

/src/backend/storage/lmgr/predicate.c

/src/backend/tcop/postgres.c

/src/backend/tcop/pquery.c

/src/backend/utils/init/globals.c

/src/backend/utils/misc/postgresql.conf.sample

/src/backend/utils/resowner/resowner.c

/src/backend/utils/time/snapmgr.c

/src/bin/initdb/initdb.c

/src/include/bcdb/globals.h


/src/include/bcdb/shm_block.h


/src/include/bcdb/worker.h

/src/backend/access/common/printtup.c

# ARIABC modifications (on top of postgres) for Deterministic execution:

/src/backend/bcdb/shm_transaction.c
/src/include/bcdb/shm_transaction.h [Each worker picks up one transaction from queue](https://github.com/ssudarshaniitb/protectdb/blob/12f9095e88fd4f60767ed3e44ebc55666f4ddcf1/safedb/src/backend/bcdb/shm_transaction.c)

/src/backend/bcdb/worker_controller.c [Spawn workers as per block size](https://github.com/ssudarshaniitb/protectdb/blob/12f9095e88fd4f60767ed3e44ebc55666f4ddcf1/safedb/src/backend/bcdb/worker_controller.c)

/src/include/bcdb/middleware.h
/src/backend/bcdb/middleware.c [Receive requests from block as JSON and unpack for workers](https://github.com/ssudarshaniitb/protectdb/blob/12f9095e88fd4f60767ed3e44ebc55666f4ddcf1/safedb/src/backend/bcdb/middleware.c)

/src/backend/bcdb/func.c


# New files for merkle tree creation as index and updating it :

/src/include/access/merkle.h

/src/backend/access/merkle/merkleinsert.c

/src/backend/access/merkle/merkleutil.c

/src/backend/access/merkle/merkleverify.c

# New files for Blake 3 hash computation :

/src/include/common/blake3.h

/src/common/Makefile

/src/common/blake3.c

/src/common/blake3.h

/src/common/blake3_dispatch.c

/src/common/blake3_impl.h

/src/common/blake3_portable.c
