
Changes:

(1) Receive requests from external client

(2) Modified 'filestore' client to not save each message to separate file

(3) Save requests to Raft log

(4) Send requests to local database wrapper

(5) (optional) add digital signature to responses

Filed modified:

ratis-client/src/main/java/org/apache/ratis/client/impl/OrderedAsync.java 

ratis-client/src/main/java/org/apache/ratis/client/impl/OrderedStreamAsync.java 

ratis-client/src/main/java/org/apache/ratis/client/impl/RaftClientImpl.java 

ratis-client/src/main/java/org/apache/ratis/client/impl/UnorderedAsync.java 

ratis-common/src/main/java/org/apache/ratis/protocol/RaftClientRequest.java 

ratis-examples/pom.xml 

ratis-examples/src/main/java/org/apache/ratis/examples/filestore/FileInfo.java 

ratis-examples/src/main/java/org/apache/ratis/examples/filestore/FileStore.java 

ratis-examples/src/main/java/org/apache/ratis/examples/filestore/FileStoreClient.java 

ratis-examples/src/main/java/org/apache/ratis/examples/filestore/FileStoreCommon.java 

ratis-examples/src/main/java/org/apache/ratis/examples/filestore/FileStoreStateMachine.java 

ratis-examples/src/main/java/org/apache/ratis/examples/filestore/cli/Client.java 

ratis-examples/src/main/java/org/apache/ratis/examples/filestore/cli/LoadGen.java 

ratis-examples/src/main/java/org/apache/ratis/examples/filestore/cli/Server.java 

ratis-grpc/src/main/java/org/apache/ratis/grpc/GrpcConfigKeys.java 

ratis-server-api/src/main/java/org/apache/ratis/server/RaftServer.java 

ratis-server-api/src/main/java/org/apache/ratis/server/RaftServerConfigKeys.java 
ratis-server/src/main/java/org/apache/ratis/server/impl/RaftServerProxy.java 
