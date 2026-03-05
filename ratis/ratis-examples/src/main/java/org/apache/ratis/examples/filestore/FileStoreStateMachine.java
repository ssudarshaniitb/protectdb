/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.examples.filestore;

import java.io.*;
import java.net.*;
import java.nio.file.Path;
import java.sql.*;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.ExamplesProtos;
import org.apache.ratis.proto.ExamplesProtos.*;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.util.*;


public class FileStoreStateMachine extends BaseStateMachine {
  private static final boolean dbg = false;
  private static final boolean dbg1 = false;
  private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();

  private final FileStore files;
  private BasicDataSource connectionPool;
  private Producer<String, String> producer;
  private ExecutorService executor;
  private ExecutorService recoveryIndexService;
  private final HashMap <String, String> nameToQuery = new HashMap<>();
  private final int dbPoolSize;
  private final String dbName;
  private final String dbPort;
  private final String bkPort;
  private final int pyTpCpPort;
  private Socket pySock = null;
  OutputStream pyStream = null;
  PrintWriter pyWriter = null;
  BufferedWriter bufWriter = null;
  private Socket rsock = null;
  ObjectOutputStream ros;
  ObjectInputStream ris ;
  private final String sharedLogFile;
  private FileWriter fw;
  private PrintWriter pw;
  private final int safedb;
  private int bcdb_init = 0;
  private int reqNum = 0; // need to set it when reading back after restart
  private Object dbinit_lock = new Object();
  private final String kafkaIpPort;
  final int MAX_POSTGRES_WORKERS = 20;
  private List<Pair<String, String>> idInputLog = new ArrayList(); // optimiz

  private String id;
  public FileStoreStateMachine(RaftProperties properties, int poolSize, int safedb, String name, String port, String kafka, String bkport, int pyTpCpPort) {
    this.files = new FileStore(this::getId, properties);
    this.dbPoolSize = (poolSize == -1) ? MAX_POSTGRES_WORKERS:poolSize;
    this.kafkaIpPort = kafka;
    this.safedb = safedb;
    this.dbName = name;
    this.dbPort = port;
    this.bkPort = bkport;
    this.pyTpCpPort = pyTpCpPort;
    this.sharedLogFile = properties.get("raft.server.storage.dir") + "/inputLog"; 
    System.err.println( "\n\n\n safedb clazz=" + this.dbName + "  " + this.dbPort + "  " + kafkaIpPort + "\n\n\n"); 
  }

  @Override
  public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage raftStorage)
      throws IOException {
    super.initialize(server, groupId, raftStorage);
    this.storage.init(raftStorage);
    for (Path path : files.getRoots()) {
      FileUtils.createDirectories(path);
    }

  
    try {
      fw = new FileWriter(this.sharedLogFile, true);
      pw = new PrintWriter(fw) ;
    } catch(IOException e) {

    }

    recoveryIndexService = Executors.newSingleThreadExecutor(); // newFixedThreadPool(1); // 
    executor = Executors.newFixedThreadPool(dbPoolSize);
    System.out.println(" initialize threadID: " + Thread.currentThread().getId() + "  ");

    System.err.println( "\n\t** calling service ");
    System.err.println( "\n\t** called service ");

    id = super.getId().toString().substring(1); // n0, n1, nNNN

    String dbUrl = "jdbc:postgresql://localhost:" + this.dbPort + "/" + this.dbName;
    String dbUser = "anant2040";
    String dbPassword = "anant2040";

    connectionPool = new BasicDataSource();
    connectionPool.setUsername(dbUser);
    connectionPool.setPassword(dbPassword);
    connectionPool.setUrl(dbUrl);
    connectionPool.setDriverClassName("org.postgresql.Driver");
    if(this.safedb == 1) {
    connectionPool.setInitialSize(1);
    } else {
      connectionPool.setInitialSize(dbPoolSize);
    }

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaIpPort);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    
      // Create the producer
    producer = new KafkaProducer<>(props);

    pySock = new Socket("localhost", pyTpCpPort);
    pyStream = pySock.getOutputStream();
    pyWriter = new PrintWriter(pyStream, true);
    bufWriter = new BufferedWriter(new OutputStreamWriter(pySock.getOutputStream()));

    System.err.println( "\n\t** done initialize ");
  }

  protected class RecoveryIndexGet implements Runnable {

    public void run() {
      int servPort = 9090;
      ServerSocket servSock = null;
      while (true) {
      try {
        if(servSock == null) {
         servSock = new ServerSocket(servPort);
        }
        //while(true){
            System.out.println("threadID: " + Thread.currentThread().getId() + " Waiting for the client req, ");
            //creating socket and waiting for client connection
            Socket socket = servSock.accept();
            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
            System.err.println("\n safedb threadID " + Thread.currentThread().getId() );
      
            String line = (String) ois.readObject();
            System.err.println("\n\n\t ** recvd on sock, port+logoffset=" + line);
            String[] tok = line.split(" ");
            oos.writeObject(" ok will resend from "+ tok[tok.length - 1]);

            if((tok[tok.length - 1]).length() != 0) {
              FileStoreStateMachine.this.resetBkndConn(tok[tok.length - 1]);
            }
        
      
      } catch (Exception e) {
        System.err.println(e.toString());
        // TODO: handle exception
      }
    }
  }
}

public void resetBkndConn(String offset) {

  int i = 0; //offset;

  try {
  this.rsock = new Socket(InetAddress.getLocalHost(), Integer.valueOf(bkPort)); // ratis client
  this.ros = new ObjectOutputStream(this.rsock.getOutputStream());
  this.ris = new ObjectInputStream(this.rsock.getInputStream());

} catch (Exception e) {
  System.err.println(e.toString());
  // TODO: handle exception
}

System.err.println("\n\n\t ** next id in idInputLog=" + offset + " len= " + idInputLog.size());
String idReq = (idInputLog.size() == 0)? "dummy": idInputLog.get(i).getLeft();
int found = 0;
int logSize = idInputLog.size();
  while(i != logSize) {
    if(idReq.equals(offset)) { found = 1;}
    idReq = idInputLog.get(++i).getLeft();
    if(found == 0) continue;
    execQuery(idInputLog.get(i).getRight()); // TODO anant2040 dec25 , thru pyTpCpPort
    System.err.println("\n\n\t ** next in idInputLog=" + idReq);
  }
  return ;
}

  @Override
  public StateMachineStorage getStateMachineStorage() {
    return storage;
  }

  @Override
  public void close() {
    producer.close();
    files.close();
    setLastAppliedTermIndex(null);
  }

  @Override
  public CompletableFuture<Message> query(Message request) {
    final ReadRequestProto proto;
    LOG.debug( "safedb clazz=" + this.getClass().getName() +"  func=" + Thread.currentThread().getStackTrace()[1].getMethodName());
    //System.err.println( "safedb syserr  clazz=" + this.getClass().getName() +"  func="  + Thread.currentThread().getStackTrace()[1].getMethodName());
    LOG.debug( "safedb thread-id =" + Thread.currentThread().getId());
    //System.err.println( "safedb syserr  thread-id ="  + Thread.currentThread().getId());
    LOG.debug( "safedb req-content=" + request.toString());
    //System.err.println( "safedb req-content=" + request.toString());

    for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
      System.out.println("safedb " + ste + "\n");
    }       
    
    try {
      proto = ReadRequestProto.parseFrom(request.getContent());
    } catch (InvalidProtocolBufferException e) {
      return FileStoreCommon.completeExceptionally("Failed to parse " + request, e);
    }

    final String path = proto.getPath().toStringUtf8();
    return (proto.getIsWatch()? files.watch(path)
        : files.read(path, proto.getOffset(), proto.getLength(), true))
        .thenApply(reply -> Message.valueOf(reply.toByteString()));
  }

  @Override
  public TransactionContext startTransaction(RaftClientRequest request) throws IOException {
    final ByteString content = request.getMessage().getContent();
    final FileStoreRequestProto proto = FileStoreRequestProto.parseFrom(content);
    final TransactionContext.Builder b = TransactionContext.newBuilder()
        .setStateMachine(this)
        .setClientRequest(request);
    LOG.debug( "safedb clazz=" + this.getClass().getName() +"  func=" + Thread.currentThread().getStackTrace()[1].getMethodName());
    if(dbg) System.err.println( "safedb syserr  clazz=" + this.getClass().getName() +"  func="  + Thread.currentThread().getStackTrace()[1].getMethodName());
        /* 
        LOG.debug( "safedb req-content=" + content.toString());
        System.err.println( "safedb req-content=" + content.toString());
        */
    
    if (proto.getRequestCase() == FileStoreRequestProto.RequestCase.WRITE) {
      final WriteRequestProto write = proto.getWrite();
      final FileStoreRequestProto newProto = FileStoreRequestProto.newBuilder()
          .setWriteHeader(write.getHeader()).build();
      
      b.setLogData(newProto.toByteString()).setStateMachineData(write.getData())
       .setStateMachineContext(newProto);
    } else {
      b.setLogData(content)
       .setStateMachineContext(proto);
    }
    return b.build();
  }

  @Override
  public TransactionContext startTransaction(LogEntryProto entry, RaftProtos.RaftPeerRole role) {
    ByteString copied = ByteString.copyFrom(entry.getStateMachineLogEntry().getLogData().asReadOnlyByteBuffer());
    if(dbg) LOG.debug( "safedb clazz=" + this.getClass().getName() +"  func=" + Thread.currentThread().getStackTrace()[1].getMethodName());
    if(dbg) System.err.println( "safedb syserr  clazz=" + this.getClass().getName() +"  func="  + Thread.currentThread().getStackTrace()[1].getMethodName());
    String copiedStr = entry.getStateMachineLogEntry().getStateMachineEntry().getStateMachineData().toStringUtf8();
    
    for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
      //System.out.println("safedb " + ste + "\n");
    }

    LOG.debug( "safedb req-content=" + copiedStr);
    System.err.println( "safedb req-content=" + copiedStr);
    return TransactionContext.newBuilder()
        .setStateMachine(this)
        .setLogEntry(entry)
        .setServerRole(role)
        .setStateMachineContext(getProto(copied))
        .build();
  }

  @Override
  public CompletableFuture<Integer> write(ReferenceCountedObject<LogEntryProto> entryRef, TransactionContext context) {
    
    LogEntryProto entry = entryRef.retain();
    final FileStoreRequestProto proto = getProto(context, entry);
    if (proto.getRequestCase() != FileStoreRequestProto.RequestCase.WRITEHEADER) {
      return null;
    }

    final WriteRequestHeaderProto h = proto.getWriteHeader();
    LOG.debug( "safedb clazz=" + this.getClass().getName() +"  func=" + Thread.currentThread().getStackTrace()[1].getMethodName());
    if(dbg) System.err.println( "safedb syserr  clazz=" + this.getClass().getName() +"  func="  + Thread.currentThread().getStackTrace()[1].getMethodName());
    LOG.debug( "safedb write-entry=" +  entry.getStateMachineLogEntry().getStateMachineEntry().getStateMachineData().toString());
    if(dbg) System.err.println( "\n safedb write-entry=" +  entry.getStateMachineLogEntry().getStateMachineEntry().getStateMachineData().toStringUtf8());
    if(dbg) System.err.println( "safedb write-logdata=" + h.getPath().toStringUtf8());
    //System.err.println("safedb  \t** \n\t **\n");
    for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
//      System.err.println("safedb* " +ste );
    }
    //System.err.println("safedb  \t** \n\t **\n");
    nameToQuery.put( h.getPath().toStringUtf8(), entry.getStateMachineLogEntry().getStateMachineEntry().getStateMachineData().toStringUtf8());
    //System.err.println("safedb  \t** \n\t **\n");

//    LOG.debug( "safedb req-hdr=" + h.toString() + " entry-str = " + entry.toString());
    
    final CompletableFuture<Integer> f =  files.write(entry.getIndex(),
        h.getPath().toStringUtf8(), h.getClose(),  h.getSync(), h.getOffset(),
        entry.getStateMachineLogEntry().getStateMachineEntry().getStateMachineData()
    ).whenComplete((r, e) -> entryRef.release());
    // */

//    System.err.println( "safedb write-entry=" +  entry.getStateMachineLogEntry().getStateMachineEntry().getStateMachineData().toString());
// sync only if closing the file
    return h.getClose() ? f: null;
  }

  static FileStoreRequestProto getProto(TransactionContext context, LogEntryProto entry) {
    if (context != null) {
      final FileStoreRequestProto proto = (FileStoreRequestProto) context.getStateMachineContext();
      if (proto != null) {
        return proto;
      }
    }
    /* 
    LOG.debug( "safedb "  +"  func=" + Thread.currentThread().getStackTrace()[1].getMethodName());
    System.err.println( "safedb syserr " + "  func="  + Thread.currentThread().getStackTrace()[1].getMethodName());
    LOG.debug( "safedb req-content=" + entry.getStateMachineLogEntry().getLogData().toString());
    System.err.println( "safedb req-content=" + entry.getStateMachineLogEntry().getLogData().toString());
    */
    return getProto(entry.getStateMachineLogEntry().getLogData());
  }

  static FileStoreRequestProto getProto(ByteString bytes) {
    try {
      return FileStoreRequestProto.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException("Failed to parse data", e);
    }
  }

  @Override
  public CompletableFuture<ByteString> read(LogEntryProto entry, TransactionContext context) {
    final FileStoreRequestProto proto = getProto(context, entry);
    if (proto.getRequestCase() != FileStoreRequestProto.RequestCase.WRITEHEADER) {
      return null;
    }

    final WriteRequestHeaderProto h = proto.getWriteHeader();
    CompletableFuture<ExamplesProtos.ReadReplyProto> reply =
        files.read(h.getPath().toStringUtf8(), h.getOffset(), h.getLength(), false);

    return reply.thenApply(ExamplesProtos.ReadReplyProto::getData);
  }

  static class LocalStream implements DataStream {
    private final String name;
    private final DataChannel dataChannel;

    LocalStream(String name, DataChannel dataChannel) {
      this.name = JavaUtils.getClassSimpleName(getClass()) + "[" + name + "]";
      this.dataChannel = dataChannel;
    }

    @Override
    public DataChannel getDataChannel() {
      return dataChannel;
    }

    @Override
    public CompletableFuture<?> cleanUp() {
      return CompletableFuture.supplyAsync(() -> {
        try {
          dataChannel.close();
          return true;
        } catch (IOException e) {
          return FileStoreCommon.completeExceptionally("Failed to close data channel", e);
        }
      });
    }

    @Override
    public String toString() {
      return name;
    }
  }

  @Override
  public CompletableFuture<DataStream> stream(RaftClientRequest request) {
    final ByteString reqByteString = request.getMessage().getContent();
    final FileStoreRequestProto proto;
    try {
      proto = FileStoreRequestProto.parseFrom(reqByteString);
    } catch (InvalidProtocolBufferException e) {
      return FileStoreCommon.completeExceptionally(
          "Failed to parse stream header", e);
    }
    final String file = proto.getStream().getPath().toStringUtf8();
    return files.createDataChannel(file)
        .thenApply(channel -> new LocalStream(file, channel));
  }

  @Override
  public CompletableFuture<?> link(DataStream stream, LogEntryProto entry) {
    LOG.info("linking {} to {}", stream, LogProtoUtils.toLogEntryString(entry));
    return files.streamLink(stream);
  }

  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    final LogEntryProto entry = trx.getLogEntryUnsafe();

    final long index = entry.getIndex();
    updateLastAppliedTermIndex(entry.getTerm(), index);

    final FileStoreRequestProto request = getProto(trx, entry);

    switch(request.getRequestCase()) {
      case DELETE:
        return delete(index, request.getDelete());
      case WRITEHEADER:
        return writeCommit(index, request.getWriteHeader(),
            entry.getStateMachineLogEntry().getStateMachineEntry().getStateMachineData().size());
      case STREAM:
        return streamCommit(request.getStream());
      case WRITE:
      /* 
      LOG.debug( "safedb clazz=" + this.getClass().getName() +"  func=" + Thread.currentThread().getStackTrace()[1].getMethodName());
      System.err.println( "safedb syserr  clazz=" + this.getClass().getName() +"  func="  + Thread.currentThread().getStackTrace()[1].getMethodName());
      LOG.debug( "safedb req-content=" + request.toString());
      System.err.println( "safedb req-content=" + request.toString());
      */
        // WRITE should not happen here since
        // startTransaction converts WRITE requests to WRITEHEADER requests.
      default:
        LOG.error(getId() + ": Unexpected request case " + request.getRequestCase());
        return FileStoreCommon.completeExceptionally(index,
            "Unexpected request case " + request.getRequestCase());
    }
  }

  private CompletableFuture<Message> writeCommit(
      long index, WriteRequestHeaderProto header, int size) {
    final String reqId = header.getPath().toStringUtf8();
    LOG.debug( "safedb clazz=" + this.getClass().getName() +"  func=" + Thread.currentThread().getStackTrace()[1].getMethodName());
    if(dbg) System.err.println( "safedb syserr  clazz=" + this.getClass().getName() +"  func="  + Thread.currentThread().getStackTrace()[1].getMethodName());
    LOG.debug( "log safedb writeCommit-reqId=" + reqId);
    if(dbg1) System.err.println( "safedb writeCommit-reqId=" + reqId + "  safedb threadID " + Thread.currentThread().getId() + 
                        " nameToQuery= "+ nameToQuery.get(reqId));

    //System.err.println("safedb  \t** \n\t\t **\n");
    for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
      // System.err.println("safedb* " +ste );
    }

    //System.err.println("safedb  \t** \n\t\t **\n");
    
    idInputLog.add( ImmutablePair.of(reqId, (reqId+ " " + id + ":" + nameToQuery.get(reqId))));
    if(dbg || (reqNum % 2000 < 2)) System.err.println(" reread reqNum " + reqNum + " query= " + nameToQuery.get(reqId));
    reqNum++;
    executor.submit(() -> { execQueryAndPublish1(reqNum, reqId);});
    return files.submitCommit(index, reqId, header.getClose(), header.getOffset(), size)
      .thenApply(reply -> Message.valueOf(reply.toByteString()));
  }
  
  void execQuery(String queryString) {
    try {
      ros.writeObject(queryString);
      String message = (String) ris.readObject();
      System.out.println("Received from DB: " + message);
    } catch(Exception e) {
      System.err.println(e.toString());
    }
}

void execQueryAndPublish1(int reqnum, String reqId) {
  try {
    bufWriter.write(reqId+ " " + nameToQuery.get(reqId));    
  } catch (Exception e) {
    e.printStackTrace();
  }
  
  if((reqnum >8500)&&(reqnum %10 == 0)) System.out.println(reqId+ " " + nameToQuery.get(reqId));
  if( pyWriter.checkError()) {
    System.out.println("pyWriter err");
  }
}

void execQueryAndPublish(String reqId) {
   
  final int NO_DB = 0;
  StringBuffer resp = new StringBuffer();
  StringBuilder uid = new StringBuilder(10 - reqId.length());
  for (int i = 0; i < (10- reqId.length()); i++) { uid.append('0'); }
  uid.append(reqId);  
  String reqUid = uid.toString();
  String queryString = nameToQuery.get(reqId);
  System.err.println("\n \t ** safedb  nameToQuery clientTxID-token= \t** "+ reqId);
  // let user form blocks

  // pass reqId just for debug purposes on pg side...
  
    System.err.println("\n\t **** saving to file *** \n" + sharedLogFile + "::" + Integer.toString(reqNum) + "=" +reqId);
    pw.println(Integer.toString(reqNum) + "=" +reqId);
    pw.flush();
    // System.err.println("\n \t ** safedb  = \t** "+ safedb+ "  sQ= " + safeQuery + '\n');
    if(NO_DB == 1) {
      resp.append(queryString).append(queryString); // test
    } else if (this.safedb == 0) { // remote backend 16-sep-25
      // TODO safedb make this connection pool or async ???

      execQuery(reqId+ " " + id + ":" + queryString);

    } 
    else {
    try {
    Connection connection = connectionPool.getConnection();
    Statement statement = connection.createStatement();
    ResultSet resultSet = null;
    if(this.safedb == 1) { // bcdb
      if(this.bcdb_init == 0) {
        String bcdbInitQ = "select bcdb_init(True, " + Integer.toString(dbPoolSize) + ");";
        this.bcdb_init = 1;
        System.err.println("\n \t ** safedb  = \t** "+ safedb+ "  SinitQ= " + bcdbInitQ + '\n');
        resultSet = statement.executeQuery(bcdbInitQ); 
      }
    } else {
      System.err.println("\n \t ** postgres/safedb Query= " + queryString + '\n');
    }
    statement.close();
    connection.close();
  } catch (SQLException e) {
    System.err.println("For req: " + reqId + "  " + queryString + " --SQLErr: " + e.toString() );
  }
  
    try {
  
    Connection connection = connectionPool.getConnection();
    System.err.println("safedb connStr-hope=Id " +  connection.toString() );

    Statement statement = connection.createStatement();
    ResultSet resultSet = null;

    int offset = 0;
    if(this.safedb == 1) offset = 9; // seq# in safedb
    int selectQ = ((queryString.charAt(0) == 's') || (queryString.charAt(0) == 'S')) ? 1: 0;
    int result = -1;
    boolean querySuccess = false; 

    // Perform desired database operations
    while( querySuccess == false ) {
      try {
        try {
          Thread.sleep(1000);
        } catch (Exception e) {
          System.out.println( "timeout e="+ e.getStackTrace());
        }

        if(this.safedb == 1) {
          if(this.bcdb_init == 0) {
            System.err.println("\n \t ** bcdb should have been initialized already... Abort... = \t** ");
          }
          resultSet = statement.executeQuery(queryString); 
          System.out.println("bcdb result == "+ resultSet.toString());
        } else if(this.safedb == 2) { 
          // safedb can just exec same query without json double quote formatting !! 
          // no - if pg should support both safedb and plainpg...
          resultSet = statement.executeQuery(queryString); 
          System.out.println("safedb result == "+ resultSet.toString());
        } else {
          // TODO executeUpdate for upd commands
          if(selectQ == 1)  { resultSet = statement.executeQuery(queryString); 
            System.err.println("pg select case --> " + queryString );
          }
          else {  // TODO expand the query as transaction to update merkle tree
            System.err.println("pg ins/upd/del case --> " + queryString );
            result = statement.executeUpdate(queryString); 
          }
          querySuccess = true;
        }
      } catch(SQLException e) {
        System.err.println("For req: " + reqId + "  " + queryString + " --SQLErr: " + e.toString() );
        if(!e.toString().contains("Serialization")) break;
      }
    }

    // ResultSet resultSet = statement.executeQuery("SELECT pg_sleep(3);");
    // ResultSet resultSet = statement.executeQuery("SELECT * FROM usertable limit 3");
    if(this.safedb == 0) {
      StringTokenizer stQ = new StringTokenizer(queryString);
      if(selectQ == 1) {
        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        for (int i = 1; i <= columnsNumber; i++)
          System.err.print( rsmd.getColumnName(i) + "\t");
      
        System.err.print("rsmd toStr == " + rsmd.toString() + " columnsNumber == " + columnsNumber);
        while (resultSet.next()) {
          for (int i = 1; i <= columnsNumber; i++) {
            resp.append(resultSet.getString(i) + " ");
          }
          //System.out.println("");
            //String columnValue = resultSet.getString("hval");
            //System.err.println("Column Value: " + columnValue);
            //System.err.println("row=\t" + resultSet.getString(1)+"\t" + resultSet.getString(2));
          System.err.println("resp Value: " + resp);
        }
        resultSet.close();
      } else {
        resp.append(stQ.nextToken() + " " + result + "  " + reqId);
      }
    } else {
    int insertQ = ((queryString.charAt(offset) == 'i') || (queryString.charAt(offset) == 'I')) ? 1: 0;
    int deleteQ = ((queryString.charAt(offset) == 'd') || (queryString.charAt(offset) == 'D')) ? 1: 0;
    int updateQ = ((queryString.charAt(offset) == 'u') || (queryString.charAt(offset) == 'U')) ? 1: 0;
    
    if(selectQ == 1) {
      resp.append("SELECT 1");
    } else if (insertQ == 1) {
      resp.append("INSERT 0 1");
    } else if (deleteQ == 1) {
      resp.append("DELETE 1");
    } else if (updateQ == 1) {
      resp.append("UPDATE 1");
    }
  }
      statement.close();
            // Close the connection
      connection.close();
      // https://stackoverflow.com/questions/28209955/does-dbcp-connection-pool-connection-close-return-connection-to-pool

  } catch(SQLException e) {
        System.err.println("For req: " + reqId + " --SQLErr: " + e.toString() );
    }//Logger.debug(" SQLErr: " + e.toString());

    StringBuffer respString = new StringBuffer(reqId);
    respString.append("  " + id + "  " + Instant.now().truncatedTo(ChronoUnit.MICROS) + "  " + resp.toString());
    ProducerRecord<String, String> record = new ProducerRecord<>("firstTopic", respString.toString());
        // Send the record
    producer.send(record);
    System.err.println("safedb sent to kafka: " + respString);
  }
  System.err.println("Server-ID== n+" +  id ); // n0, n1 ,...
  System.err.println("processed: " + reqId);


        // Close the producer
        //producer.close();

   }

  private CompletableFuture<Message> streamCommit(StreamWriteRequestProto stream) {
    final String path = stream.getPath().toStringUtf8();
    final long size = stream.getLength();
    return files.streamCommit(path, size).thenApply(reply -> Message.valueOf(reply.toByteString()));
  }

  private CompletableFuture<Message> delete(long index, DeleteRequestProto request) {
    final String path = request.getPath().toStringUtf8();
    return files.delete(index, path).thenApply(resolved ->
        Message.valueOf(DeleteReplyProto.newBuilder().setResolvedPath(
            FileStoreCommon.toByteString(resolved)).build().toByteString(),
            () -> "Message:" + resolved));
  }
}
