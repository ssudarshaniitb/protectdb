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
package org.apache.ratis.examples.filestore.cli;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.text.NumberFormat;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.ratis.examples.filestore.FileStoreClient;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.buffer.PooledByteBufAllocator;
import org.apache.ratis.util.FileUtils;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

/**
 * Subcommand to generate load in filestore state machine.
 */
@Parameters(commandDescription = "Load Generator for FileStore")
public class LoadGen extends Client {
  private static final boolean dbg = false;
  @Parameter(names = { "--sync" }, description = "Whether sync every bufferSize", required = false)
  private int sync = 0;

  private static final int MAX_TX_SIZE = 1024;
  protected int reqNum = (rid == 1) ? 1:rid;
  private static int spawnPollThread = 0;
  private  int numQueryThreads = getNumThread();
  Map<String, Map<Integer, List<String>>> txReplies = new HashMap<>();
  Map<Integer, String> hashMsg = new HashMap<>(); // optimiz
  private ReentrantLock lockObj = new ReentrantLock();
  private Condition queryStatus = lockObj.newCondition();
  private static ServerSocket servSock;
  ExecutorService fixedExecutorService; //socket server port on which it will listen
  
  //final int respThreshold = 0;
  int num_tx_complete = 0;
  long time_last_1k = 0;
    
  long startTime = 0; //System.nanoTime(); // currentTimeMillis();
  long startTimeMs = 0; //System.nanoTime(); // currentTimeMillis();
  Map<String, Long> txStartTime = new HashMap<>();
  Map<String, Long> txEndTimes = new HashMap<>();
  Map<String, Integer> txProcTime = new HashMap<>();
  PublicKey pub;
  String warmupQ = "dummy";
  String warmupSign = "RX2RCQrM9T2ynrp8VPNxKDgyvpV58Wn69VWGZ15RCr97HGWfABhIxAte1g7Z52fLe70euWIL5CnDKB00trMP6QehLs2I/1WvwWbTGODtl0qAzBtHokB2gremCzVLzvMPBydo8UA2CpEjodbpYi9A7wpzQXJrkkmNFgTxIi+xmD8VnFs=";
  int warmupWait = 0;

  @Override
  protected void operation(List<FileStoreClient> clients) throws IOException, ParseException, ExecutionException, InterruptedException {
    dropCache();
    System.out.println("Starting Async write now ");
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    System.err.println("\n safedb num threads " + getNumThread());
    fixedExecutorService = Executors.newFixedThreadPool( getNumThread() );
    System.err.println("\n safedb main threadID " + Thread.currentThread().getId() );
    boolean ignore = false;
    try {
        pub = readPublicKeyFromFile(keyFilePath);
        ignore = verifySignature(warmupQ, warmupSign, pub);
        System.err.println("\n safedb dummy= " + ignore + " main threadID " + Thread.currentThread().getId() );
    } catch (Exception e) {
            // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    if(spawnPollThread == 0) {
      //InnerLoadGen 
      spawnPollThread = 1;
      executorService.execute(new InnerLoadGen());
    }
    long sumExecTime = 0;
    int countQ = 0;
    // while (warmupWait == 0) {       Thread.sleep(500);    }

    startTime = System.nanoTime(); 
    startTimeMs = System.currentTimeMillis();
    long totalWrittenBytes = 0;

    // rsaSignVerifyRef();
      
    if(!StringUtils.isNumeric(getQSrc())) {
      totalWrittenBytes = writeDbTx(clients);
      if(dbg) System.out.println("\n txReplies non-numeric num keys sz= " + txReplies.size());
      if(dbg) for ( Map.Entry  entry : txStartTime.entrySet()) {
          System.out.println(" req= " + entry.getKey() + " startTime = " + entry.getValue());
      }
            System.out.println("\n txReplies num keys sz= " + txReplies.size());
    } else {
      int portNum = ((Number)NumberFormat.getInstance().parse(getQSrc())).intValue();
      if(portNum < 1024) return ; // TODO error
      totalWrittenBytes = writeDbTx(clients, portNum);
    }

    //System.out.println("Total files written: " + getNumFiles());
    //System.out.println("Each files size: " + getFileSizeInBytes());
    //System.out.println("Total data written: " + totalWrittenBytes + " bytes");

    System.err.println("awaiting term...        " + System.nanoTime()/1000 + " microsec&& msec=" + System.currentTimeMillis());
    int delta = 101;
    countQ = txProcTime.size();
    while (delta > 20) {
      executorService.awaitTermination(100, TimeUnit.MILLISECONDS);
      delta = txProcTime.size() - countQ;
      countQ = txProcTime.size();
    }
    System.err.println(" kfk resp recvd so far = " + countQ);   
    long endTime = System.nanoTime(); // currentTimeMillis();
    System.err.println("callinggg shutdown...  " + System.nanoTime()/1000 + " microsec && msec=" + System.currentTimeMillis());
    executorService.shutdown();
    System.err.println(" term endTime...        " + System.nanoTime()/1000 + " microsec && msec=" + System.currentTimeMillis());
    close(clients);
    fixedExecutorService.shutdown();
    fixedExecutorService.awaitTermination(1, TimeUnit.SECONDS);

    Thread.sleep(5000);
    countQ = txProcTime.size();
    for ( Map.Entry  entry : txStartTime.entrySet()) {
          System.out.println(" req= " + entry.getKey() + " startTime = " + entry.getValue() + " endTime= " + txEndTimes.get(entry.getKey()));
        //  sumExecTime += ((Long) (entry.getValue())).intValue() / 1000;
    }
    for ( Map.Entry  entry : txEndTimes.entrySet()) {
          System.out.println(" req= " + entry.getKey() + " endTime = " + entry.getValue());
        //  sumExecTime += ((Long) (entry.getValue())).intValue() / 1000;
    }
    for(reqNum = 1; txProcTime.containsKey(this.getCid()+"-"+reqNum); reqNum++) {
      final String reqId2 = new String(this.getCid() + "-" + (reqNum));
          System.out.println(" req= " + reqId2 + " execTime = " + txProcTime.get(reqId2));
          sumExecTime += (txProcTime.get(reqId2)).longValue()/1000;
    }
    if(countQ != 0) System.out.println("Avggggg time taken: " + (sumExecTime)/countQ + " microsec; countQ= " + countQ);
    System.out.println("Total time taken: " + (endTime - startTime)/1000 + " microsec");
    System.err.println("Total time taken: " + (endTime - startTime)/1000 + " microsec");
    //if(dbg) 
      System.out.println("start time: " + (startTime) + " nanosec  && msec=" + startTimeMs);
    if(dbg) 
      System.out.println("end time : " + (endTime) + " nanosec");
    
  }

  void rsaSignVerifyRef() {

    try {
      byte[] bytes = Files.readAllBytes(Paths.get("/home/anant2040/tpcc-setup/rsa/msg1"));
      String message1 = new String(bytes, StandardCharsets.UTF_8); // Specify encoding
      bytes = Files.readAllBytes(Paths.get("/home/anant2040/tpcc-setup/rsa/1048privk"));
      String privStr = new String(bytes, StandardCharsets.UTF_8); // Specify encoding
      String message = message1.replace("\n", "");
      System.out.println(message);
      byte[] privBytes = Base64.getDecoder().decode(privStr);
      PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(privBytes);
      KeyFactory keyFactory = KeyFactory.getInstance("RSA");
      PrivateKey privk = keyFactory.generatePrivate(keySpec);
      String sign = signMessage(message, privk);
      String text = "{\n\t\"msg\":\""+message+"\",\n\t\"sig\":\""+sign + "\"\n}\n";
      Files.write(Paths.get("/home/anant2040/tpcc-setup/rsa/sig2"), text.getBytes(StandardCharsets.UTF_8));
    } catch (Exception e) {
            // TODO Auto-generated catch block
      e.printStackTrace();
    }
  
     // VERIFY TODO -- MOVE TO FUNC WHICH RCVS SIGNED-Q
    String msgSig = "";
    String msgSigFail = "";
    try {
      byte[] bytes = Files.readAllBytes(Paths.get("/home/anant2040/tpcc-setup/rsa/sig1"));
      msgSig = new String(bytes, StandardCharsets.UTF_8); // Specify encoding
        // Process the content
      byte[] bytesFail = Files.readAllBytes(Paths.get("/home/anant2040/tpcc-setup/rsa/sig2fail"));
      msgSigFail = new String(bytesFail, StandardCharsets.UTF_8); // Specify encoding

      int msgLast = msgSig.indexOf('"',11);
      int sigLast = msgSig.indexOf('"',msgLast+11+5);
      String msg = msgSig.substring(10,msgLast);
      String sig = msgSig.substring(msgLast+11, sigLast);
      System.out.println("msg: " + msg);
      System.out.println("Signature : " + sig);

      int msgLast2 = msgSigFail.indexOf('"',11);
      int sigLast2 = msgSigFail.indexOf('"',msgLast2+11+5);
      String msg2 = msgSigFail.substring(10,msgLast2);
      String sig2 = msgSigFail.substring(msgLast2+11, sigLast2);

      boolean isValid;
      PublicKey pub = readPublicKeyFromFile("/home/anant2040/tpcc-setup/rsa/1048pubk");
      isValid = verifySignature(msg, sig, pub);
      System.out.println("Signature in file sig1 : valid: " + isValid);
      isValid = verifySignature(msg2, sig2, pub);
      System.out.println("Signature in file sig2fail : valid: " + isValid);

      List<String> queries = Files.readAllLines(Paths.get("/home/anant2040/tpcc-setup/ycsb-tx-bchain/ycsb-pttx-bcdb-intkey-signed-10"));
      System.out.println("q0= " + queries.get(0));
      for (int k = 0; k < 6; k++) { 
        msgSig = queries.get(k);
        msgLast = msgSig.indexOf(';');
        msg = msgSig.substring(0,msgLast+1);
        sig = msgSig.substring(msgLast+2);
        System.out.println("msg: " + msg);
        System.out.println("Signature : " + sig);
        isValid = verifySignature(msg, sig, pub);
        System.out.println("Signature of q-" + k + ": valid: " + isValid);
        }

        //        } catch (IOException e) {
        // Handle exception
        //        e.printStackTrace();
      } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        
  }

  protected class InnerRcvReq implements Runnable {

    private FileStoreClient fsClient;
    private Socket socket;
    private String reqString;

    public InnerRcvReq(FileStoreClient client, Socket sock, String requestId) {
      fsClient = client;
      socket = sock;
      reqString = requestId;
    }

    public void run() {
      String reqId = reqString+"-0";

      try {
      ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
      ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
      // final String txString = line;
      System.err.println("\n safedb threadID " + Thread.currentThread().getId() );
      int rCount = 0;
      int queryNeedSign = getQSign();

      String line = (String) ois.readObject();
      if(line.contains("demoSaveState")) {
        reqId = "a"+reqString.substring(1);
        reqString = reqString.replaceFirst("c", "a");
        System.err.println("safedb reqstr== "+ reqString);
      }

      while(! line.startsWith("bye")) {
        System.out.println("safedb rcvd input= " + line);
        // TODO rsa sign verify
        boolean isValid = false;
        String sig ;
        try {
          PublicKey pub = readPublicKeyFromFile("/home/anant2040/tpcc-setup/rsa/1048pubk");
          String msg = line.substring(0,line.indexOf(';')+1);
          if(msg.length() == line.length()) {
            System.err.println("safedb msg NOT signed:" + msg);
            isValid = true;
            if(queryNeedSign == 1) isValid = false; // continue;
          } else {
            sig = line.substring(2+line.indexOf(';'));
            isValid = verifySignature(msg, sig, pub);
          }
          if(isValid) {
            System.err.println("safedb NOT signed msg :" + msg);
            reqId = reqString + "-" + Integer.toString(rCount); rCount++;
            final ByteBuf buf = PooledByteBufAllocator.DEFAULT.heapBuffer(MAX_TX_SIZE);
            buf.writeBytes(msg.getBytes(), 0, msg.length());
            System.err.println("safedb buf NOT signed msg :" + msg);
            try {
              final CompletableFuture<Long> f = fsClient.writeAsync(
                      reqId, 0, true, buf.nioBuffer(), sync == 1);
                      System.err.println("safedb writing async... NOT signed msg :" + msg);
                      f.join();
            } catch (Throwable e) {
              System.err.println(e.toString());
            }
          }
        }catch(Exception e) {
        }

        synchronized (this) {
          lockObj.lock();
          System.err.println("waiting...locking threadID " + Thread.currentThread().getId() );
          while (txReplies.get(reqId)== null) { queryStatus.await(2, TimeUnit.MILLISECONDS); }
            System.err.println("done wait...unlocking threadID " + Thread.currentThread().getId() );
          lockObj.unlock();
        }
        //write object to Socket
        System.err.println("safedb query result get txReplies-val:  "+ txReplies.get(reqId));
        Set<Integer> hashNodeList = txReplies.get(reqId).keySet();
        int majorityHash = -1; int majorityCount = 0; 
        for(Integer i: hashNodeList) {
          int count = txReplies.get(reqId).get(i).size();
          if(count > majorityCount) {
            majorityHash = i;
            majorityCount = count;
          }
        }
        System.err.println("safedb query result:  "+ hashMsg.get(majorityHash));
        //  sign the result RSA
        String resp = hashMsg.get(majorityHash);
        byte[]bytes = Files.readAllBytes(Paths.get("/home/anant2040/tpcc-setup/rsa/1048privk"));
        String privStr = new String(bytes, StandardCharsets.UTF_8); // Specify encoding
        byte[] privBytes = Base64.getDecoder().decode(privStr);
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(privBytes);
        try {
          KeyFactory keyFactory = KeyFactory.getInstance("RSA");
          PrivateKey privk = keyFactory.generatePrivate(keySpec);
          String output = "{\n\t\"req\":\""+reqId + "  " + resp+"\",\n\t\"sig\":\""+signMessage(resp, privk) + "\"\n}\n";
          if(reqId.startsWith("a")) {
            output = "{\n\t\"req\":\""+reqId + "  " + resp +"\",\n\t\"sig\":\""+signMessage(resp, privk) + "\"\n}\n";
          } else { System.err.println("safedb reqid:  "+ reqId);}
          oos.writeObject(output);
        } catch (Exception e) {

        }
        line = (String) ois.readObject();
    }

    try {
        TimeUnit.SECONDS.sleep(5);
        System.err.println("safedb query result sleep over... quitting... threadID " + Thread.currentThread().getId() );
  
    } catch(InterruptedException e) { }
       
      oos.close();
      ois.close();
      socket.close();
    } catch(EOFException e) { 
      System.err.println("safedb client data EOF...... threadID " + Thread.currentThread().getId() );
    }
      catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } 
    }
  }

  protected class InnerLoadGen implements Runnable {

    int logCount = 0;
    protected Producer<String, String> errProducer;

    public void run() {
      if(!kafkaIpPort.isEmpty())
        pollResults();
    }
  
    public void pollResults() {

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaIpPort);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    
      // Create the producer
    errProducer = new KafkaProducer<>(props);
            
        // This is the crucial property to start reading from the latest offset
        // propCons.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getConsumerProperties(kafkaIpPort));
    consumer.subscribe(Collections.singletonList("topic2"));

    int count = pollCount;
    System.err.println("\n pt... starting poll " + (System.nanoTime()/1000));
    warmupWait = 1;
    while (count > 0 || pollCount == 0) {

      //int numMsg = 0;
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofNanos(pollTime*1000));
      int numMsg = records.count();
      if(dbg) System.err.println("\n pt... Consumer Record will be shown here" + numMsg);
      if(numMsg == 0 ) { logCount++;
        if(logCount == (100000/pollTime)) { 
          logCount = 0; System.out.println(" Waiting for request " + System.currentTimeMillis()/1000 + '\n');
          //no result in record poll " + System.currentTimeMillis()/1000 + '\n');
          //System.out.println(" pt... pollResult count= " + count + " t= " );
        }
        continue; 
      }
      else  { logCount = 0;     count--; }
      // server result
      // which can notify clients -- cond var type ??

  long txEndTime = (System.nanoTime()/1000); 
  for (ConsumerRecord<String, String> record : records) {
    if(dbg) System.err.println("\n pt... Kafka Consumer Record in braces:("+ record.value()+")\n");
    
          List<String> tokens = Arrays.asList(record.value().split("\\s+"));
                      // Calculate msgStart position
          final int CREQ = 0; final int SERV = 1; final int tstamp = 3; final int threshold = 0;
          if(dbg) System.out.printf(" pt... num tokens = " + tokens.size() + " tok1=" + tokens.get(CREQ) + " tok2= " + tokens.get(SERV)); 
          final int spaces = 2; final int total_nodes = 3;
          int msgStart = tokens.get(CREQ).length() + 2* spaces + tokens.get(SERV).length() ;
          if(dbg) System.out.printf(" pt... msg offset= " + msgStart + "\t msg== " + record.value().substring(msgStart, msgStart+2)); 
          int msgHash = record.value().substring(msgStart).hashCode(); // same
          hashMsg.put(msgHash,  record.value().substring(msgStart)); // if not already present ? TODO dec25
          if(dbg) System.out.printf("\n pt... msg= " + record.value().substring(msgStart)); 

          String requestToken = tokens.get(CREQ);
          Map<Integer, List<String>> hashNodes = new HashMap<>();
          long tStart = 0; 
          if(!txStartTime.containsKey(requestToken))  System.out.println("\n pt... tx start chk " + requestToken + " currT= " + System.nanoTime());
          else tStart = txStartTime.get(requestToken);
          txEndTimes.put(requestToken, txEndTime);
          int e2etime = (int) (txEndTime - tStart);// txStartTime.get(requestToken));
          txProcTime.put(requestToken, e2etime); 
          if(dbg) System.out.println("\n pt... " + requestToken + " execTime (ns)= " + e2etime);

          if (txReplies.containsKey(requestToken)) {
            if(dbg) System.out.printf("\n pt... found existing entry for client-req= " + tokens.get(CREQ));
            hashNodes = txReplies.get(requestToken);
            List<String> nodeList = hashNodes.getOrDefault(msgHash, new ArrayList<>());
            System.out.println("\n pt... resp seen earlier ... from " + ((nodeList==null ) ? 0: nodeList.size()) + " nodes= " + String.join(", ", nodeList));
            nodeList.add(tokens.get(SERV));
            hashNodes.put(msgHash, nodeList);
          } else {
            hashNodes = new HashMap<>();
            if(dbg) System.out.println("\n pt... txReplies num keys sz= " + txReplies.size());
            if(dbg) System.out.printf(" pt... creating entry for new client-req= " + tokens.get(CREQ));
            String nodeList = tokens.get(SERV);
            hashNodes.put(msgHash, new ArrayList<>(Arrays.asList(nodeList)));
            
          }
          txReplies.put(requestToken, hashNodes);
          if(dbg) System.err.println(" pt... safedb threadID " + Thread.currentThread().getId() );
          if(dbg) System.err.println(" pt... safedb query result put txReplies-val:  "+ txReplies.get(requestToken));
          //System.err.println("safedb query result put txReplies-val--get(0):  "+ txReplies.get(requestToken));
  
          int total_tx_resp = 0;
          int majorityHash = 0;
          for(int j : hashNodes.keySet()) {
            total_tx_resp += hashNodes.get(j).size();
            if( hashNodes.get(j).size() > threshold) { majorityHash = j;}
          }

          if(total_tx_resp > respThreshold) {
              txEndTimes.put(requestToken, txEndTime); 
              txProcTime.put(requestToken, e2etime); 
          }

          int new_tx_complete = txProcTime.size();
          if(txProcTime.size() == 0) { time_last_1k = System.currentTimeMillis(); }
          if(new_tx_complete - num_tx_complete > 998) {
            num_tx_complete = new_tx_complete;
            int delta = (int) (System.currentTimeMillis() - time_last_1k);
            time_last_1k = System.currentTimeMillis();
              System.err.println("qSent= " + txStartTime.size() + " kfk-done=" + 
                    txEndTimes.size() + " delta= " + delta + " t= " + System.currentTimeMillis());

          }
          if(total_tx_resp == total_nodes) {

              List<String> nodeList = hashNodes.get(majorityHash);
              int numResp = ((nodeList == null) ? 0: nodeList.size());
              if (numResp > threshold) {
                if(dbg) System.err.println("\n pt... ** tx " + tokens.get(CREQ) + " result " + " complete at " + Instant                // Represent a moment in UTC. 
                .now()                 // Capture the current moment. Returns a `Instant` object. 
                .truncatedTo(          // Lop off the finer part of this moment. 
                    ChronoUnit.MICROS  // Granularity to which we are truncating. 
                ));                   //System.currentTimeMillis());
    
                  ProducerRecord<String, String> stepRecord ;
              if(numResp != total_nodes) {

          //for (Map<Integer,List<String>> nodes : hashNodes) {}
        List<String> corruptNodes = new ArrayList<>();
        String healthyRefNode ="", recoverySteps;
        for(Map.Entry<Integer, List<String>> entry : hashNodes.entrySet()) {
            if (entry.getValue().size() == threshold) 
            { 
              for (String node: entry.getValue()) { corruptNodes.add(node); }
            } else {
              healthyRefNode = entry.getValue().get(0);
            }

        }
          /* TODO - send list of err nodes to kafka.
            TODO -- auto pick all tables in database in the py-script */
        recoverySteps = requestToken + " Transaction successful at majority nodes; On corrupt nodes, run \"python3 ./demo-snapshot-isolate-recover-mtree.py  which gives instructions to replay transactions from where processing was stopped \n" ;// + healthyRefNode;
        stepRecord = new ProducerRecord<>("errTopic", recoverySteps);
            // Send the record
        System.err.println("safedb sending to kafka: " + recoverySteps);
        } else {
          stepRecord  = new ProducerRecord<>("errTopic", requestToken +" Tx successful at all nodes");
          // Send the record
        }
        errProducer.send(stepRecord);
      }

      }
    }

        synchronized(this) {
          if(numMsg != 0) 
            if(dbg) System.out.println(" pt... pollResult sync blk txReplies num keys sz= " + txReplies.size());
          if(dbg) System.out.println(" pt... pollResult count= " + count + " t= " + System.currentTimeMillis()/1000 + '\n');
          lockObj.lock();
          queryStatus.signalAll();
          lockObj.unlock();
        }
        consumer.commitAsync();     // Date.valueOf(null)
        }
        System.out.println(" pt... leaving while loop ... pollResult count = " + count);
      }
    }
    
  public Properties getConsumerProperties(String kafkaString) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaString);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    return props;
  }

  long writeDbTx(List<FileStoreClient> clients, int servPort) throws IOException {

    BufferedReader reader;
    servSock = new ServerSocket(servPort);

    final FileStoreClient client = clients.get(0 % clients.size());
    System.err.println("safedb syserr  clazz=" + this.getClass().getName() + "  func="
        + Thread.currentThread().getStackTrace()[1].getMethodName());

    try {
      if(!StringUtils.isNumeric(getQSrc())) {
        System.err.println("safedb syserr  clazz=" + this.getClass().getName() + "  func="
        + Thread.currentThread().getStackTrace()[1].getMethodName());
      return -1;
      }
      while(true){
        System.out.println("Waiting for the client req, threadID " + Thread.currentThread().getId());
        //creating socket and waiting for client connection
        Socket socket = servSock.accept();
        final String reqId = new String(getCid() + "-" + (reqNum++));
        InnerRcvReq irr = new InnerRcvReq(client, socket, reqId);
        fixedExecutorService.submit(irr);
        //read from socket to ObjectInputStream object

      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally { servSock.close(); }

    return 1;
  }

  long writeDbTx(List<FileStoreClient> clients) throws IOException {

    BufferedReader reader;
    int queryNeedSign = this.getQSign();

    final FileStoreClient client = clients.get(0 % clients.size());
    System.err.println("safedb syserr  clazz=" + this.getClass().getName() + "  func="
        + Thread.currentThread().getStackTrace()[1].getMethodName());

    try {
      if(StringUtils.isNumeric(getQSrc())) {
        System.err.println("safedb syserr  clazz=" + this.getClass().getName() + "  func="
        + Thread.currentThread().getStackTrace()[1].getMethodName());
        return -1;
      }
      reader = new BufferedReader(new FileReader(getQSrc()));
      List<String> txLines = new ArrayList<String>();
      String line = reader.readLine();
      startTime = System.nanoTime(); // currentTimeMillis();
      int max = 0;

      while (line != null) {
        txLines.add(line);
        line = reader.readLine();
        max++;
      }

      int count = 0;
      while( count < max ) {
        line = txLines.get(count);
        final ByteBuf buf = PooledByteBufAllocator.DEFAULT.heapBuffer(MAX_TX_SIZE);
        if(dbg|| ((reqNum%2000) < 3)) 
          System.err.println("q= " + reqNum + " kfk-done=" + txEndTimes.size() + " t= " + System.currentTimeMillis());
        // rsa sign check TODO
        boolean isValid = false;
        String sig ;
        final String reqId = new String(this.getCid() + "-" + (reqNum++));
        try {
        String msg = line.substring(0,line.indexOf(';')+1);
        if(msg.length() == line.length()) {
          isValid = true;
          if(queryNeedSign == 1) {
            isValid = false; // continue;
            System.err.println("\n\t** safedb msg NOT signed...: Skipping \n\n");
          } else {
              if(dbg) System.err.println("safedb msg NOT signed...");
	  }
        } else {
          sig = line.substring(2+line.indexOf(';'));
          isValid = verifySignature(msg, sig, pub);
        }
        if(isValid) {
          if(dbg) System.err.println("safedb sig verified..." );
          buf.writeBytes(msg.getBytes(), 0, msg.length());

          txStartTime.put(reqId, (System.nanoTime()/1000));
          if(reqNum % 2000 == 0) System.out.println("\n pt... tx start put " + reqId + " currT= " + System.nanoTime());
          try {
          final CompletableFuture<Long> f = client.writeAsync(
              reqId, 0, true, buf.nioBuffer(), sync == 1);
          f.join();

          } catch (Throwable e) {
            System.err.println(e.toString());
          }

          // read next line
        
          if (dbg) try {
          TimeUnit.SECONDS.sleep(1);
          } catch(InterruptedException e) { }
           
        } else {
          System.err.println("safedb sig NOT verified...");
        } 
        } catch (Exception e) {
          System.err.println(e.toString());
        }
        count++;
      }
      reader.close();
    } catch (IOException e) {
      e.printStackTrace();
    } finally { }

    return 1;
  }

  long write(FileChannel in, long offset, FileStoreClient fileStoreClient, String path,
      List<CompletableFuture<Long>> futures) throws IOException {
    final int bufferSize = getBufferSizeInBytes();
    final ByteBuf buf = PooledByteBufAllocator.DEFAULT.heapBuffer(bufferSize);
    final int bytesRead = buf.writeBytes(in, bufferSize);

    System.err.println("safedb syserr  clazz=" + this.getClass().getName() + "  func="
        + Thread.currentThread().getStackTrace()[1].getMethodName());

    if (bytesRead < 0) {
      throw new IllegalStateException("Failed to read " + bufferSize + " byte(s) from " + this
          + ". The channel has reached end-of-stream at " + offset);
    } else if (bytesRead > 0) {
      final CompletableFuture<Long> f = fileStoreClient.writeAsync(
          path, offset, offset + bytesRead == getFileSizeInBytes(), buf.nioBuffer(),
          sync == 1);
      f.thenRun(buf::release);
      if (futures != null) {
        futures.add(f);
      }
    }
    return bytesRead;
  }

  private Map<String, CompletableFuture<List<CompletableFuture<Long>>>> writeByHeapByteBuffer(
      List<String> paths, List<FileStoreClient> clients, ExecutorService executor) {
    Map<String, CompletableFuture<List<CompletableFuture<Long>>>> fileMap = new HashMap<>();

    System.err.println("safedb syserr  clazz=" + this.getClass().getName() + "  func="
        + Thread.currentThread().getStackTrace()[1].getMethodName());

    int clientIndex = 0;
    for (String path : paths) {
      final CompletableFuture<List<CompletableFuture<Long>>> future = new CompletableFuture<>();
      final FileStoreClient client = clients.get(clientIndex % clients.size());
      clientIndex++;
      CompletableFuture.supplyAsync(() -> {
        List<CompletableFuture<Long>> futures = new ArrayList<>();
        File file = new File(path);
        try (FileChannel in = FileUtils.newFileChannel(file, StandardOpenOption.READ)) {
          for (long offset = 0L; offset < getFileSizeInBytes();) {
            offset += write(in, offset, client, file.getName(), futures);
          }
        } catch (Throwable e) {
          future.completeExceptionally(e);
        }

        future.complete(futures);
        return future;
      }, executor);

      fileMap.put(path, future);
    }

    return fileMap;
  }

  private long waitWriteFinish(Map<String, CompletableFuture<List<CompletableFuture<Long>>>> fileMap)
      throws ExecutionException, InterruptedException {
    long totalBytes = 0;

    System.err.println("safedb syserr  clazz=" + this.getClass().getName() + "  func="
        + Thread.currentThread().getStackTrace()[1].getMethodName());

    for (CompletableFuture<List<CompletableFuture<Long>>> futures : fileMap.values()) {
      long writtenLen = 0;
      for (CompletableFuture<Long> future : futures.get()) {
        writtenLen += future.join();
        if (getTxInterval() != 0)
          Thread.sleep(getTxInterval());
      }

      if (writtenLen != getFileSizeInBytes()) {
        System.out.println("File written:" + writtenLen + " does not match expected:" + getFileSizeInBytes());
      }

      totalBytes += writtenLen;
    }
    return totalBytes;
  }
}
