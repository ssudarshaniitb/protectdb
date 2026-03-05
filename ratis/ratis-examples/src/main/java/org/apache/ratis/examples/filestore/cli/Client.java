/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.examples.filestore.cli;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.*;
import java.security.spec.X509EncodedKeySpec;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.*;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.SupportedDataStreamType;
import org.apache.ratis.examples.common.SubCommandBase;
import org.apache.ratis.examples.filestore.FileStoreClient;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.*;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.*;

import com.beust.jcommander.Parameter;

/**
 * Client to connect filestore example cluster.
 */
public abstract class Client extends SubCommandBase {

  @Parameter(names = {"--size"}, description = "Size of each file in bytes", required = false)
  private long fileSizeInBytes;

  @Parameter(names = {"--bufferSize"}, description = "Size of buffer in bytes, should less than 4MB, " +
      "i.e BUFFER_BYTE_LIMIT_DEFAULT", required = false)
  private int bufferSizeInBytes = 1024;

  @Parameter(names = {"--queryFrom"}, description = "Query source - port number or file", required = true)
  private String qSrc = "";
  
  @Parameter(names = {"--querySign"}, description = "Query sign required or not", required = true)
  private int qSign = 0;

  @Parameter(names = {"--txInterval"}, description = "Time to wait after each tx", required = true)
  private int txInterval;

  @Parameter(names = {"--numClients"}, description = "Number of clients to write", required = true)
  private int numClients;

  @Parameter(names = {"--numTerminals"}, description = "Number of bb terminals", required = false)
  protected int numTerminals;

  @Parameter(names = {"--storage", "-s"}, description = "Storage dir, eg. --storage dir1 --storage dir2",
      required = false)
  private List<File> storageDir = new ArrayList<>();

  @Parameter(names = {"--clientId", "-c"}, description = " client , eg. --clientId c1 ", required = true)
  protected String cid; 

  @Parameter(names = {"--reqIdOffset", "-rid"}, description = " reqId , eg. --reqIdOffset 2 ", required = false)
  protected int rid = 1; 

  @Parameter(names = {"--kafkaAddr", "-kafkaAddr"}, description = "Kafka IP port e.g. 4.4.4.4:9092", required = false)
  protected String kafkaIpPort;

  @Parameter(names = {"--keyFile", "-keyFile"}, description = "Key file path", required = true)
  protected String keyFilePath;

  @Parameter(names = {"--respThreshold"}, description = "how many responses to wait for ", required = false)
  protected int respThreshold = 0;

  @Parameter(names = {"--pollCount"}, description = "how many seconds to poll, 0 for forever ", required = false)
  protected int pollCount = 0;

  @Parameter(names = {"--pollInterval"}, description = "Time between polls (microsec) ", required = true)
  protected int pollTime = -1;

  public String getCid() {
    return cid;
  }

  public String getQSrc() {
    return qSrc;
  }

  public int getQSign() {
    return qSign;
  }
  
  //private static final int MAX_THREADS_NUM = 1000;
  private int reqId = 1;
  private int numFiles = 0;

  public int getTxInterval() {
    return txInterval;
  }

  public int getNumThread() {
    return numTerminals; //numFiles < MAX_THREADS_NUM ? numFiles : MAX_THREADS_NUM;
  }

  public long getFileSizeInBytes() {
    return fileSizeInBytes;
  }

  public int getBufferSizeInBytes() {
    return bufferSizeInBytes;
  }

  public int getNumFiles() {
    return numFiles;
  }

  @Override
  public void run() throws Exception {
    int raftSegmentPreallocatedSize = 1024 * 1024 * 1024;
    RaftProperties raftProperties = new RaftProperties();
    RaftConfigKeys.Rpc.setType(raftProperties, SupportedRpcType.GRPC);
    GrpcConfigKeys.setMessageSizeMax(raftProperties,
        SizeInBytes.valueOf(raftSegmentPreallocatedSize));
    RaftServerConfigKeys.Log.Appender.setBufferByteLimit(raftProperties,
        SizeInBytes.valueOf(raftSegmentPreallocatedSize));
    RaftServerConfigKeys.Log.setWriteBufferSize(raftProperties,
        SizeInBytes.valueOf(raftSegmentPreallocatedSize));
    RaftServerConfigKeys.Log.setPreallocatedSize(raftProperties,
        SizeInBytes.valueOf(raftSegmentPreallocatedSize));
    RaftServerConfigKeys.Log.setSegmentSizeMax(raftProperties,
        SizeInBytes.valueOf(1 * 1024 * 1024 * 1024L));
    RaftConfigKeys.DataStream.setType(raftProperties, SupportedDataStreamType.NETTY);

    RaftServerConfigKeys.Log.setSegmentCacheNumMax(raftProperties, 2);

    RaftClientConfigKeys.Rpc.setRequestTimeout(raftProperties,
        TimeDuration.valueOf(50000, TimeUnit.MILLISECONDS));
    RaftClientConfigKeys.Async.setOutstandingRequestsMax(raftProperties, 10);

    System.err.println("safedb  \t** \n\t client-run \t **\n");

    for (File dir : storageDir) {
      FileUtils.createDirectories(dir);
    }

    operation(getClients(raftProperties));
    System.err.println("exiting client run function");
  }

  public List<FileStoreClient> getClients(RaftProperties raftProperties) {
    List<FileStoreClient> fileStoreClients = new ArrayList<>();
    for (int i = 0; i < numClients; i ++) {
      final RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(ByteString.copyFromUtf8(getRaftGroupId())),
          getPeers());

      RaftClient.Builder builder =
          RaftClient.newBuilder().setProperties(raftProperties);
      builder.setRaftGroup(raftGroup);
      builder.setClientRpc(
          new GrpcFactory(new org.apache.ratis.conf.Parameters())
              .newRaftClientRpc(ClientId.randomId(), raftProperties));
      RaftPeer[] peers = getPeers();
      builder.setPrimaryDataStreamServer(peers[0]);
      // safedb - which peer req will be sent to
      RaftClient client = builder.build();
//      fileStoreClients.add(new FileStoreClient(client));
      fileStoreClients.add(new FileStoreClient(client));
    }
    return fileStoreClients;
  }


  protected void close(List<FileStoreClient> clients) throws IOException {
    for (FileStoreClient client : clients) {
      client.close();
    }
  }

  public String getPath(String fileName) {
    int hash = fileName.hashCode() % storageDir.size();
    System.err.println("safedb  \t** \n\t\t getpath **\n");

    return new File(storageDir.get(Math.abs(hash)), fileName).getAbsolutePath();
  }

  protected void dropCache() {
    String[] cmds = {"/bin/sh","-c","echo 3 > /proc/sys/vm/drop_caches"};
    try {
      Process pro = Runtime.getRuntime().exec(cmds);
      pro.waitFor();
    } catch (Throwable t) {
      if (t instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      System.err.println("Failed to run command:" + Arrays.toString(cmds) + ":" + t.getMessage());
    }
  }

  private CompletableFuture<Long> writeFileAsync(String path, ExecutorService executor) {
    System.err.println("inside write file async");
    final CompletableFuture<Long> future = new CompletableFuture<>();
    CompletableFuture.supplyAsync(() -> {
      try {
//        future.complete(writeFile(path, fileSizeInBytes, bufferSizeInBytes));
        future.complete(writeTx(path));
      } catch (IOException e) {
        future.completeExceptionally(e);
      }
      return future;
    }, executor);
    return future;
  }
  
      // Sign a message with a private key
      public static String signMessage(String message, PrivateKey privateKey) throws Exception {
        Signature sig = Signature.getInstance("SHA256withRSA");
        sig.initSign(privateKey);
        sig.update(message.getBytes());
        byte[] signatureBytes = sig.sign();
        return Base64.getEncoder().encodeToString(signatureBytes);
    }

   public static PublicKey readPublicKeyFromFile(String filename) throws Exception, IOException {
        // Step 1: Read Base64-encoded string from file
        //byte[] encodedBytes = Files.readAllBytes(Paths.get(filename));
        //String pubKeyBase64 = new String(encodedBytes).replaceAll("\\s", "");
      String content = "";
        byte[] bytes = Files.readAllBytes(Paths.get(filename));
        content = new String(bytes, StandardCharsets.UTF_8); // Specify encoding
    // Process the content
      String pubKeyBase64 = new String(content).replaceAll("\\s", "");

        //String pubKeyBase64 = new String(Files.readString(Paths.get(filename))).replaceAll("\\s", "");

        // Step 2: Decode Base64 to get X.509 encoded byte array
        byte[] decodedKey = Base64.getDecoder().decode(pubKeyBase64);

        // Step 3: Reconstruct public key using KeyFactory
        X509EncodedKeySpec spec = new X509EncodedKeySpec(decodedKey);
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        return keyFactory.generatePublic(spec);
    }


    //  Verify signature
    public static boolean verifySignature(String message, String base64Signature, PublicKey publicKey) throws Exception {
        Signature sig = Signature.getInstance("SHA256withRSA");
        sig.initVerify(publicKey);
        sig.update(message.getBytes());
        byte[] signatureBytes = Base64.getDecoder().decode(base64Signature);
        return sig.verify(signatureBytes);
    }
  protected List<String> generateReqIds(ExecutorService executor) {
    UUID uuid = UUID.randomUUID();
    List<String> reqs = new ArrayList<>();
    for (int i = 0; i < numFiles; i ++) {
      String req = ("req-" + uuid + "-" + i);
      reqs.add(req);
    }
    return reqs;
  }

  protected List<String> generateFiles(ExecutorService executor) {
    UUID uuid = UUID.randomUUID();
    List<String> paths = new ArrayList<>();
    List<CompletableFuture<Long>> futures = new ArrayList<>();
    for (int i = 0; i < numFiles; i ++) {
      String path = getPath("file-" + uuid + "-" + i);
      paths.add(path);
      futures.add(writeFileAsync(path, executor));
    }

    for (int i = 0; i < futures.size(); i ++) {
      long size = futures.get(i).join();
      if(fileSizeInBytes != 0) {
        if (size != fileSizeInBytes) {
        System.err.println("Error: path:" + paths.get(i) + " write:" + size +
            " mismatch expected size:" + fileSizeInBytes);
      }
    }
  }

    return paths;
  }

  protected long writeFile(String path, long fileSize, long bufferSize) throws IOException {
    final byte[] buffer = new byte[Math.toIntExact(bufferSize)];
    long offset = 0;
    final String txString = " select read_modify_write( 'user1', 'user2', 'user3', 'user4');";
    StringBuffer empString = new StringBuffer(" ");
    System.err.println("safedb inside write file ... filesz " +fileSize + " bufsz "+ bufferSize);
   System.err.println("safedb  \t** \n\t **\n");
   for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
    System.err.println("safedb* " +ste );
   }
   System.err.println("safedb  \t** \n\t **\n");

    try(RandomAccessFile raf = new RandomAccessFile(path, "rw")) {
      while (offset < fileSize) {
        final long remaining = fileSize - offset;
        final long chunkSize = Math.min(remaining, bufferSize);
        ThreadLocalRandom.current().nextBytes(buffer);
 //       raf.write(buffer, 0, Math.toIntExact(chunkSize));
        raf.writeBytes(txString);
//        offset += chunkSize;
        offset += txString.length();
        for (int j = 0 ; j < fileSize- offset-1; j++) empString.append(" ");
        offset += fileSize - offset;
        raf.writeBytes(empString.toString());
      }
    }
    return offset;
  }

  protected long writeTx(String path) throws IOException {
    final String txString = new String(this.getCid() + "-" + (reqId++) + " select read_modify_write( 'user1', 'user2', 'user3', 'user4');");
    System.err.println("safedb inside write - tx file ... ");

   System.err.println("safedb  \t** \n\t **\n");
   for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
    System.err.println("safedb* " +ste );
   }
   System.err.println("safedb  \t** \n\t **\n");

    try(RandomAccessFile raf = new RandomAccessFile(path, "rw")) {
        raf.writeBytes(txString);
    }
    return txString.length();
  }

  protected abstract void operation(List<FileStoreClient> clients)
      throws IOException, ExecutionException, ParseException, InterruptedException;
}
