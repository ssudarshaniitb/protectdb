


# Ratis Source code Changes (based on ratis.apache.org):

(1) Receive requests from external client (or input file) [Receive transaction message from user ](https://github.com/ssudarshaniitb/protectdb/blob/376c2553c2e5fc9a83a468a36162384e78974421/ratis/ratis-examples/src/main/java/org/apache/ratis/examples/filestore/cli/LoadGen.java#L278)


(2) (After consensus) When committing requests to Raft log, send requests to local database wrapper [Send transaction to DB](https://github.com/ssudarshaniitb/protectdb/blob/376c2553c2e5fc9a83a468a36162384e78974421/ratis/ratis-examples/src/main/java/org/apache/ratis/examples/filestore/FileStoreStateMachine.java#L468)

(3) Subscribe to Kafka result Topic and poll for transaction results from each replica [Poll Kafka for transaction results ](https://github.com/ssudarshaniitb/protectdb/blob/376c2553c2e5fc9a83a468a36162384e78974421/ratis/ratis-examples/src/main/java/org/apache/ratis/examples/filestore/cli/LoadGen.java#L403)

(4) (optional) verify digital signature to responses
[Verify](https://github.com/ssudarshaniitb/protectdb/blob/869e0d5be7b4f49b2e9cc514ee2380423d948627/ratis/ratis-examples/src/main/java/org/apache/ratis/examples/filestore/cli/LoadGen.java#L205)

(5) Modified 'filestore' client to not save each message to separate file

[FileInfo](https://github.com/ssudarshaniitb/protectdb/blob/376c2553c2e5fc9a83a468a36162384e78974421/ratis/ratis-examples/src/main/java/org/apache/ratis/examples/filestore/FileInfo.java)

[FileStore](https://github.com/ssudarshaniitb/protectdb/blob/376c2553c2e5fc9a83a468a36162384e78974421/ratis/ratis-examples/src/main/java/org/apache/ratis/examples/filestore/FileStore.java)

[FileStoreClient](https://github.com/ssudarshaniitb/protectdb/blob/376c2553c2e5fc9a83a468a36162384e78974421/ratis/ratis-examples/src/main/java/org/apache/ratis/examples/filestore/FileStoreClient.java) 

[FileStoreCommon](https://github.com/ssudarshaniitb/protectdb/blob/376c2553c2e5fc9a83a468a36162384e78974421/ratis/ratis-examples/src/main/java/org/apache/ratis/examples/filestore/FileStoreCommon.java)

[Client](https://github.com/ssudarshaniitb/protectdb/blob/376c2553c2e5fc9a83a468a36162384e78974421/ratis/ratis-examples/src/main/java/org/apache/ratis/examples/filestore/cli/Client.java)

[Server](https://github.com/ssudarshaniitb/protectdb/blob/376c2553c2e5fc9a83a468a36162384e78974421/ratis/ratis-examples/src/main/java/org/apache/ratis/examples/filestore/cli/Server.java)



(6) Misc. tweaks - Files modified:

[OrderedAsync](https://github.com/ssudarshaniitb/protectdb/blob/376c2553c2e5fc9a83a468a36162384e78974421/ratis/ratis-client/src/main/java/org/apache/ratis/client/impl/OrderedAsync.java)

[OrderedStreamAsync](https://github.com/ssudarshaniitb/protectdb/blob/376c2553c2e5fc9a83a468a36162384e78974421/ratis/ratis-client/src/main/java/org/apache/ratis/client/impl/OrderedStreamAsync.java)

[RaftClientImpl](https://github.com/ssudarshaniitb/protectdb/blob/376c2553c2e5fc9a83a468a36162384e78974421/ratis/ratis-client/src/main/java/org/apache/ratis/client/impl/RaftClientImpl.java)

[UnorderedAsync](https://github.com/ssudarshaniitb/protectdb/blob/376c2553c2e5fc9a83a468a36162384e78974421/ratis/ratis-client/src/main/java/org/apache/ratis/client/impl/UnorderedAsync.java)

[RaftClientRequest](https://github.com/ssudarshaniitb/protectdb/blob/376c2553c2e5fc9a83a468a36162384e78974421/ratis/ratis-common/src/main/java/org/apache/ratis/protocol/RaftClientRequest.java)

[POM](https://github.com/ssudarshaniitb/protectdb/blob/376c2553c2e5fc9a83a468a36162384e78974421/ratis/ratis-examples/pom.xml)

[GrpcConfigKeys](https://github.com/ssudarshaniitb/protectdb/blob/376c2553c2e5fc9a83a468a36162384e78974421/ratis/ratis-grpc/src/main/java/org/apache/ratis/grpc/GrpcConfigKeys.java)

[RaftServer](https://github.com/ssudarshaniitb/protectdb/blob/376c2553c2e5fc9a83a468a36162384e78974421/ratis/ratis-server-api/src/main/java/org/apache/ratis/server/RaftServer.java)

[RaftServerConfigKeys](https://github.com/ssudarshaniitb/protectdb/blob/376c2553c2e5fc9a83a468a36162384e78974421/ratis/ratis-server-api/src/main/java/org/apache/ratis/server/RaftServerConfigKeys.java)

[RaftServerProxy](https://github.com/ssudarshaniitb/protectdb/blob/376c2553c2e5fc9a83a468a36162384e78974421/ratis/ratis-server/src/main/java/org/apache/ratis/server/impl/RaftServerProxy.java)





