
Steps to compile and run Ratis :

1) ./mvnw clean package

2) export env variable PEERS on all replicas as (using localhost ip for own ID)

    export PEERS=n2:127.0.0.1:7000,n0:10.129.148.129:7000,n3:10.129.148.137:7000


3) Run the server on each replica

    bash$ ID=n2; cd ~/git/ratis; ${BIN}/server.sh filestore server --id ${ID} -kafkaAddr 10.129.148.129:9092 --storage /tmp/ratis2/${ID} --peers ${PEERS} -bkndport 8585 --pyTpCpPort 5000

4) Run the client

    bash$ ${BIN}/client.sh filestore loadgen --txInterval 10000 --storage /tmp/ratis1/loadgen2 --numClients 1 --peers ${PEERS} --queryFrom <filename>  --pollInterval 2000 --kafkaAddr <ip+port> --pollCount 100000  --numTerminals 1 --querySign 0 --clientId c2 






Ratis Source code Changes:

(1) Receive requests from external client (or input file)

(2) (After consensus) When committing requests to Raft log, send requests to local database wrapper

(3) Subscribe to Kafka result Topic and poll for transaction results from each replica

(4) (optional) add digital signature to responses

(5) Modified 'filestore' client to not save each message to separate file

For details on files changed, please refer README_files_modified.txt


