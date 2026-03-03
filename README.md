# ProtectDB 

This repository contains the source code of ProtectDB, i.e., the deterministic concurrency control implemented on top of AriaBC implementation of PostgreSQL provided by authors of HarmonyBC, available at github.com/zllai/AriaBC .

## Compile and install

```sh
```

## Run a sample workload (single machine)


```sh
initdb -D /tmp/safedir
```

2. Launch the database engine

```sh
postgres -D /tmp/safedir
```

3. Create the database and initialize the state

```sh
createdb safedb
psql -d safedb < 
```

4. Run the workload

```sh
psql -d safedb < 
```

