#!/usr/bin/python3

import time
import sys
import socket
import threading
from threading import Thread, Lock
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
from psycopg_pool import ConnectionPool
from kafka import KafkaProducer

LOG_SKIP = 1000
KAFKA_PUB = 1
DBG2 = 0
kafkaip = '10.129.148.129:9092' #aws ip 172.31.44.145:9092'

if(KAFKA_PUB != 0):
    producer = KafkaProducer(bootstrap_servers=[kafkaip],
                         value_serializer=lambda v: v.encode('utf-8'),
                         max_in_flight_requests_per_connection= 200,
                         linger_ms= 20)
topic_name = 'topic2'

mutex = Lock()
mutex_seqnum = Lock()
execTime_dict = {}

DB_USER = "anant2040"
DB_PASS = "anant2040"
DB_HOST = "localhost"
DB_PORT = "5432"

DB_NAME = "pg13db2" # "ycsbdb2"
DB_NAME = "ycsbdb2"
DB_NAME = "pgsafedb" #

global tEnd, startTime, endTime, totalTime, totalExecPubTime
startTime = 0.0
endTime = 0.0
totalTime = 0.0
totalExecPubTime = 0.0
tEnd = time.time()

if(len(sys.argv) < 4):
  print("Err: missing arguments... ")
  print("Usage: python3 <scriptname> <dbName> <dbPg0Safe1> <num-thread> <server-ID> [BatchSize] [<queryDataFile>] ");
  print(": ... ")
  exit(-1)

print('\n Arguments: a1 a2 == ', sys.argv[1], sys.argv[2]);
print(" a3 a4 pause == ", sys.argv[3], sys.argv[3]);
DB_NAME = sys.argv[1]
DB_TYPE = 0
DB_TYPE = int(sys.argv[2])
NUM = 5  # Number of worker threads
NUM = int(sys.argv[3])
SRVID = int(sys.argv[4])
if(len(sys.argv) > 5):
    BATCHSZ = int(sys.argv[5])

connStr = "postgresql://"+DB_USER + ":" + DB_PASS + "@" + DB_HOST+":"+ DB_PORT + "/" + DB_NAME


ariaNum = NUM
if (DB_TYPE == 2):
  NUM = 1

_BCDB_MERKLE_TAG = "BCDB_MERKLE_ROOTS:"

def _install_bcdb_merkle_notice_handler(conn) -> None:
    try:
        conn._bcdb_merkle_roots = None
    except Exception:
        return

    def _notice_cb(diag):
        try:
            msg = diag.message_primary or ""
            if not msg:
                return
            p = msg.find(_BCDB_MERKLE_TAG)
            if p == -1:
                return
            payload = msg[p + len(_BCDB_MERKLE_TAG):].strip()
            if payload:
                conn._bcdb_merkle_roots = payload
        except Exception:
            return

    try:
        conn.add_notice_handler(_notice_cb)
    except Exception:
        pass

def configure_connection(conn):
    _install_bcdb_merkle_notice_handler(conn)

if(DB_NAME != 'null'):
    pool = ConnectionPool(connStr, min_size = NUM,  configure=configure_connection)

procSeqNum = 0
inputSeqNum = 0
message_dict = {}

def execWrapper():
    qCount, q  = getSeqNum(0)
    while (qCount == -1):
        time.sleep(0.005)
        qCount,q  = getSeqNum(0)
    if (q == ''):
        return
    execTx(qCount, q)

def execTx(qCount, query):
    """Process a single query """
    if((qCount % LOG_SKIP) < NUM):
      print(f"\nProcessing query: {query}")
    msgHash = getSeqHash(qCount)
    realQuery = query[query.find(' '):].strip()
    inpSeqQuery = query[:query.find(' ')]
    if(DBG2):
     if((qCount % LOG_SKIP) < NUM):
      print("qCount=" + str(qCount) + "  thread-id=" +str(threading.get_ident()) + " seq#=  " + msgHash + "  inputSeqNum=# " + inpSeqQuery)
    ts1 = time.time()
    global startTime
    if(qCount == 0):
        startTime = ts1
        print('startTime == ' + str(ts1))

    pfx = 's '

    ts1 = time.time()
    result_message = ''
    if(DB_NAME != 'null'):
      with pool.connection() as conn:
        cur1 = conn.cursor()
        if (DB_TYPE == 1):
          cur1.execute( 's ' + msgHash + ' ' + realQuery )
        else: 
            cur1.execute(  's ' + realQuery )
        conn.commit()
      result = f"{SRVID} " + str(cur1.statusmessage) 
      # Append BCDB merkle roots captured from NOTICEs (if any).
      line = realQuery
      if(line[0] !='s') and (line[0] !='S'):
          merkle_roots = getattr(conn, "_bcdb_merkle_roots", None)
          if merkle_roots:
              result_message = result + " , " + str(merkle_roots)
              conn._bcdb_merkle_roots = None

    sys.stdout.flush()

    ts2 = time.time()
    # print("\ncommit timestamp2 = " + str(ts2) + " line= " + inpSeqQuery)
    execTime = (ts2-ts1)

    #print('    \''+cur1.statusmessage + " conn_id= " + str(conn.info.backend_pid) + "\' tid= " + str(threading.get_ident()) )
    qIndex = realQuery.find('=##')
    offset = 0 if (qIndex == -1) else qIndex +3
    #print( 'sel cursor rowcount = ' + str(cur1.rowcount))

    if ((realQuery[offset] =='s') or (realQuery[offset] =='S')):
      resp = str(cur1.fetchall())
      # print( 'sel q resp = ' + resp)
      result = result + ' ' + resp

    if((qCount % LOG_SKIP) < NUM):
      print(" raw-result-msg== " + result_message)
      print("q = " + inpSeqQuery + " result = " + result)
    if(KAFKA_PUB != 0):
      producer.send(topic_name, value = (inpSeqQuery + ' ' + result))

    ts3 = time.time()
    waitTime = (ts3-ts1)

    global tEnd
    endTime = ts3
    mutex.acquire()
    tEnd = time.time()
    mutex.release()

    if((qCount % LOG_SKIP) < NUM):
      if(KAFKA_PUB != 0):
        producer.flush()
      print("qCount= " + str(qCount) + " endtime stamp2= " + str(ts3) + " qSz=" + str(len(message_dict)))
      #print("qCount= " + str(qCount) + " totalExec=" + str(totalTime*1000) + "(ms) totalExecPub=" + str(totalExecPubTime*1000) + "(ms) timeExecPub= " + str(waitTime*1000) + "(ms) timestamp2= " + str(ts3) )

    return result


def getSeqHash(seqNum):

    num = seqNum
    nd = 0
    for len in range(0, 8):
      num =  (int) (num/10)
      nd = nd + 1
      if(num == 0):
          break
    numstr = ""
    for len in range(0, 8-nd):
      numstr = numstr + '0'
    #print("    thread-id=" +str(threading.get_ident()) + " seq#==  " + numstr+str(currNum))
    return numstr+str(seqNum)

def getSeqNum(dummy):
    global procSeqNum
    global inputSeqNum

    mutex_seqnum.acquire()
    currNum = procSeqNum
    if(procSeqNum == inputSeqNum):
        mutex_seqnum.release()
        return -1, ''
    procSeqNum = procSeqNum + 1
    mutex_seqnum.release()
    if (DBG2):
     if((currNum % LOG_SKIP) < NUM):
      print(f"\nThread: {threading.current_thread().name} | ID: {threading.get_ident()} | seq: {procSeqNum}")

    return currNum, message_dict[currNum]

def chkPool(message):
    # Print both ID and Name to verify
    print(f"Thread: {threading.current_thread().name} | ID: {threading.get_ident()} | Msg: {message}")
    time.sleep(1) # Simulated work
    return f"Done: {message}"

tests = ["A", "B", "C", "D", "E"]

def start_server(host='localhost', port=5000):
    # Create a TCP/IP socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # Bind the socket to the address and port
    server_socket.bind((host, port))

    # Listen for incoming connections (max 5 queued connections)
    server_socket.listen(5)
    print(f"Server listening on {host}:{port}")

    global tEnd
    try:
        while True:
            # ExecPub for a connection
            global inputSeqNum
            print("Waiting for a connection...", file = sys.stderr)
            client_socket, client_address = server_socket.accept()
            inputSeqNum = 0

            try:
               print(f"\tConnection from {client_address}", file = sys.stderr)

                # Keep reading messages until EOF
               time.sleep(2)
               message_count = 0
               buffer = ""
               beginInit = 0

                # Create ThreadPoolExecutor for this client connection
               with concurrent.futures.ThreadPoolExecutor(max_workers=NUM) as execPool:
                 batFutures = []
                 while True:
                        # Receive data in chunks
                    data = client_socket.recv(8192)

                        # If no data, client has closed connection (EOF)
                    if not data:
                       message_dict[inputSeqNum] = ''
                       inputSeqNum += 1
                       ariaExecPub = 'c3-0 select bcdb_wait_to_finish();'
                       ariaClose = 'c4-0 select bcdb_num_committed();'
                       tEnd = time.time()
                       print("q-overall time taken (millisec) = " + str((tEnd - startTime)*1000))
                       if (DB_TYPE == 2):
                              execTx( ariaExecPub)
                              execTx( ariaClose)
                       print(f"Client disconnected. Total messages received: {message_count}")

                            # ExecPub for all submitted tasks to complete
                            #print(f"Waiting for {len(futures)} tasks to complete...")
                       break
                    ariaInit = 'c2-0 select bcdb_init(True, ' + str(ariaNum) + ');'
                    if ((DB_TYPE == 2) and (beginInit == 0)):
                        beginInit = 1
                        execTx( ariaInit)

                        # Decode and add to buffer
                    buffer += data.decode('utf-8')

                        # Process complete lines (messages ending with \n)
                    messages = []
                    while ';' in buffer:
                      if(len(buffer) <= 1):
                              break
                      line, buffer = buffer.split(';', 1)
                      message = line.strip()+';'

                      if message:
                        message_count += 1
                        if((message_count % LOG_SKIP) < NUM):
                            print(f"RxMessage {message_count}: {message} ts=" + str(time.time()))
                                # Send a response back to client
                        response = f"Server recvd msg {message_count}: {message}\n"
                        #client_socket.sendall(response.encode('utf-8'))
                        messages.append(message)
                        if((message_count % LOG_SKIP) < NUM):
                          print(f"Submitting message {message} to executor")

                           # Submit message immediately to executor
                    for message in messages:
                        message_dict[inputSeqNum] = message
                        inputSeqNum += 1
                        future = execPool.submit(execWrapper)
                        batFutures.append(future)

               fCount = 0
               print(f"\n\nLoop completed with fcount: {fCount}")
               for f in concurrent.futures.as_completed(batFutures):
                    try:
                            result = f.result()
                            fCount = LOG_SKIP - 1
                            if((fCount % LOG_SKIP) < 2*NUM):
                               print("\n msg-count= " + str(message_count) + " kafka= " + str(KAFKA_PUB) + " finalTimeDiff=" + str((endTime-startTime)*1000) + "(ms) endTime=" + str(endTime*1000) + "(ms) startTime= " + str(startTime*1000) )
                            if((fCount % LOG_SKIP) < NUM):
                              print(f"Task completed with result: {result}")
                            break
                    except Exception as e:
                            print(f"Task failed with error: {e}")


            except Exception as e:
                print(f"Error: {e}")
            finally:
                # Clean up the connection
                reqCount = 0
                eTotal = 0
                if(KAFKA_PUB != 0):
                  producer.flush()
                print("finally q-overall time taken (millisec) = " + str((tEnd - startTime)*1000))
                for reqId, etime in execTime_dict.items():
                    reqCount += 1
                    eTotal += etime
                    #print("py req=" + reqId + " execTime(ms)= " + str(etime*1000))
                #print("py req#=" + str(reqCount) + " avg execTime(ms)= " + str(1000*eTotal/reqCount))

                client_socket.close()

    except KeyboardInterrupt:
        print("\nServer shutting down...")
        server_socket.close()
    finally:
        server_socket.close()

if __name__ == "__main__":
    tStart = time.time()
    start_server()
    print('tstart = ' + str(startTime))
    sys.stdout.flush()


