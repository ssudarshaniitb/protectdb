# ProtectDB 

This repository contains the source code of ProtectDB, i.e., the deterministic concurrency control implemented on top of AriaBC implementation of PostgreSQL provided by authors of HarmonyBC, available at github.com/zllai/AriaBC .

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

2. Launch the database engine

```sh
postgres -D /tmp/safedir
```

3. Create the database and initialize with YCSB

```sh
createdb safedb
psql -d safedb < ycsb-bb-pgdump-12k.sql
```

