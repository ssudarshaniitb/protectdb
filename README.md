

<img width="563" height="505" alt="raft-kafka-trim5" src="https://github.com/user-attachments/assets/86d3a9d3-804e-48e7-95e8-2ed9e7d3d9b3" />

There are 2 directories in the project:

1) safedb: contains Deterministic transaction execution modification of Postgres database  (See safedb/README_protectdb.md )

2) ratis : contains changes to Apache Ratis package, used to establish Raft consensus (See ratis/README.md for details)

Steps to setup end-to-end system :

(1) Install database on each replica & start database (refer README inside safedb directory)

(2) Start Kafka on a node with 'topic2' for results and 'errtopic' for errors

(3) Install Raft on each replica and the node receiving tx from client (refer README inside ratis directory)
    
    (a) Start ratis server on each replica with PEERS configured

(4) Run wrapper on each replica

(5) Start ratis client on the receiver node, also with PEERS

    (a) Client can read requests from file or receive over network
