



# Steps to compile and run consensus :

1) ./mvnw clean package

2) export shell env variable PEERS on all replicas (using localhost ip for own ID)

    export PEERS=n2:127.0.0.1:7000,n0:10.129.148.129:7000,n3:10.129.148.137:7000


3) Run the server on each replica

    bash$ ID=n2; cd ~/git/ratis; ${BIN}/server.sh filestore server --id ${ID} -kafkaAddr <ip+port>  --storage /tmp/ratis2/${ID} --peers ${PEERS} -bkndport 8585 --pyTpCpPort < db-wrapper-port >

4) Run the client

    bash$ ${BIN}/client.sh filestore loadgen --txInterval 10000 --storage /tmp/ratis1/loadgen2 --numClients 1 --peers ${PEERS} --queryFrom <filename>  --pollInterval 2000 --kafkaAddr <ip+port> --pollCount 100000  --numTerminals 1 --querySign 0 --clientId c2 




