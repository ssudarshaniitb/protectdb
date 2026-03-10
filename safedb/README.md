# ProtectDB 

This repository contains the source code of database, i.e., the deterministic concurrency control implemented on top of AriaBC implementation of PostgreSQL provided by authors of HarmonyBC, available at github.com/zllai/AriaBC .

Please check details of changes in README_files_modified.txt

## Compile and install

```sh
./configure --prefix=${POSTGRES_INSTALLDIR} --enable-debug --without-icu
make   
make install
```

## Setup database on a single machine


```sh
initdb -D /tmp/safedir
```

2. Add following lines to postgresql.conf:

```sh
   enable_merkle_index = on
   merkle_update_detection = on
```

3. Launch the database engine

```sh
postgres -D /tmp/safedir
```

4. Create the database, initialize YCSB and create merkle index

```sh
createdb safedb
psql -d safedb < ycsb-bb-pgdump-12k.sql
echo 'create index ycsb_merk_index ON usertable USING merkle(ycsb_key) WITH (partitions = 200, leaves_per_partition = 16);' | psql -d safedb
```
