#!/usr/bin/python3

import psycopg2
import sys
import time
import itertools
from threading import Thread, Lock
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
from psycopg_pool import ConnectionPool
from psycopg2.pool import ThreadedConnectionPool

mutex = Lock()
mutex_seqnum = Lock()
procSeqNum = 0
inputSeqNum = 0
message_dict = {}

DB_USER = "anant2040"
DB_PORT = "5432"
DB_NAME = "pg13db2" # "ycsbdb2"
DB_NAME = "safeMtreeDb" #
DB_NAME = "ycsbdb2"
DB_PASS = "anant2040"
#DB_HOST = "10.129.148.129"
DB_HOST = "localhost"
RATE = 800
NUM = 4
LOG_SKIP = 1000
 
#print("arglen== " , len(sys.argv))
if(len(sys.argv) < 4):
  print("Err: missing arguments... ")
  print("Usage: python3 <scriptname> <dbName> <queryDataFile> <pg0Safe1Aria2> <num-thread> [<<Qrate>]");
  exit(-1)

NUM = int(sys.argv[4])
DB_TYPE = int(sys.argv[3])
if(len(sys.argv) > 5):
  RATE = int(sys.argv[5])
reqPause = 1.0 #/RATE;
print(" arg1 a2 == ", sys.argv[1], sys.argv[2]);
print(" arg3 a4 pause == ", sys.argv[3], sys.argv[4], reqPause);
DB_NAME = sys.argv[1]
connStr = "postgresql://"+DB_USER + ":" + DB_PASS + "@" + DB_HOST+":"+ DB_PORT + "/" + DB_NAME

totalWaitTime = 0.0

def _install_bcdb_merkle_notice_handler(conn) -> None:
    """
    Capture NOTICE payloads of the form:
      "BCDB_MERKLE_ROOTS: (p, <hex>) ..."

    bcpsql/psql suppresses this NOTICE and appends the payload to the command
    status line (e.g. "DELETE 1 , (p, <hex>)"). We emulate that here.
    """
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
    # This runs ONLY when a new connection is added to the pool
    _install_bcdb_merkle_notice_handler(conn)

#pool = ThreadedConnectionPool(connStr, min_size = NUM, connection_factory=_install_bcdb_merkle_notice_handler )
pool = ConnectionPool(connStr, min_size = NUM, configure=configure_connection) #, connection_factory=_install_bcdb_merkle_notice_handler )
#, connection_class= <class 'psycopg.Connection'>) 

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
    if((currNum % LOG_SKIP) < NUM):
      print(f"Thread: {threading.current_thread().name} | ID: {threading.get_ident()} | seq: {procSeqNum}")

    return currNum, message_dict[currNum]

def execWrapper(dummy):
    #print(arg)
    #time.sleep(0.0012) # this fixes most dup-key errors - now only 1 in 1k !!
    qCount, q  = getSeqNum(0)
    while (qCount == -1):
        time.sleep(0.00050)
        qCount,q  = getSeqNum(0)
    if (q == ''):
        return
    execTx(qCount, q)

def ThreadWrapper(dummy):
    qCount = 0
    global inputSeqNum
    while (qCount < inputSeqNum):
        qCount, q  = getSeqNum(0)
        if (q == ''):
            return
        execTx(qCount, q)
    return 1

def _get_raw_statusmessage(cur) -> str:
    """
    Best-effort: psycopg may normalize/truncate `statusmessage` for some builds.
    Try to prefer the raw `pgresult.command_status` when available.
    """
    try:
        pgresult = getattr(cur, "pgresult", None)
        if pgresult is not None:
            raw = getattr(pgresult, "command_status", None)
            if raw:
                if isinstance(raw, bytes):
                    try:
                        return raw.decode("utf-8", "replace")
                    except Exception:
                        return str(raw)
                return str(raw)
    except Exception:
        pass
    try:
        return cur.statusmessage if cur.statusmessage else "NO STATUS"
    except Exception:
        return "NO STATUS"

_BCDB_MERKLE_TAG = "BCDB_MERKLE_ROOTS:"

def _format_safedb_meta_row(row) -> str:
    # Target format: (143, 607f...hash...) (no quotes around the hash).
    try:
        if isinstance(row, tuple):
            parts = []
            for v in row:
                if isinstance(v, str):
                    parts.append(v)
                else:
                    parts.append(str(v))
            return "(" + ", ".join(parts) + ")"
    except Exception:
        pass
    return str(row)

def _try_fetch_one_row(cur):
    # Only fetch if the backend returned a rowset.
    try:
        if getattr(cur, "description", None) is None:
            return None
    except Exception:
        return None
    try:
        return cur.fetchone()
    except Exception:
        return None

def execTx(qCount, line):

 #print("0timestamp0 = " + str(ts1) + " line= " + line)
 #sys.stdout.flush()
 try:
  ts1 = time.time()
  serialErr = 1
  pfx = 's '
  mHash = getSeqHash(qCount)
  if(line != message_dict[qCount]):
    print(" qCount = " + str(qCount) + ' ' + message_dict[qCount] + ' whereas line= ' + line)
  if((qCount % LOG_SKIP) < NUM):
    print("timestamp1 = " + str(ts1) + " line= " + line)
  with pool.connection() as conn:
    #    conn.execute(line)
    if((0) and (line[0] !='s') and (line[0] !='S')):
      _install_bcdb_merkle_notice_handler(conn)
      if hasattr(conn, "_bcdb_merkle_roots"):
        conn._bcdb_merkle_roots = None
    cur1 = conn.cursor()

    if (DB_TYPE == 1):
        #print(pfx + mHash + ' ' + line  )
        cur1.execute(pfx + mHash + ' ' + line  )
    else:
        cur1.execute(pfx + line  )

    #print(" raw-result-msg== " + result_message)
  conn.commit()
  result_message = cur1.statusmessage # _get_raw_statusmessage(cur1)
  #meta_row = _try_fetch_one_row(cur1)
  #if meta_row is not None:
  #  result_message = result_message + " , " + _format_safedb_meta_row(meta_row)
  #print(" raw-result-msg== " + result_message)

  # Append BCDB merkle roots captured from NOTICEs (if any).
  if(line[0] !='s') and (line[0] !='S'):
    merkle_roots = getattr(conn, "_bcdb_merkle_roots", None)
    if merkle_roots:
      result_message = result_message + " , " + str(merkle_roots)
      conn._bcdb_merkle_roots = None

  if((qCount % LOG_SKIP) < NUM):
    print(" raw-result-msg== " + result_message)
    print("  2timestamp1 = " + str(ts1) + " tid= " + str(threading.get_ident()))
  sys.stdout.flush()
  #while (serialErr == true):
      #print("Database connected successfully")
  #print("3timestamp1 = " + str(ts1) )
  serialErr = 0
      #break
  if((qCount % LOG_SKIP) < NUM):
    print(cur1.statusmessage)
    if(line[0] =='s') or (line[0] =='S'):
      print(str(cur1.fetchall()))
  ts2 = time.time()
  #print("    timestamp2 = " + str(ts2) )
  if((qCount % LOG_SKIP) < NUM):
    print("    time diff (ms) = " + str((ts2 - ts1)*1000) + " tid= " + str(threading.get_ident()) )

  waitTime = reqPause - (ts2-ts1)
  waitTime = 0
  #global totalWaitTime
  if(waitTime > 0) :
       time.sleep(waitTime);
       mutex.acquire()
       totalWaitTime += waitTime
       mutex.release()
       #print("     timeWait = " + str(waitTime) )
  #else:
       #print("     NO...timeWait = " + str(waitTime) )
 #except psycopg.errors.SerializationFailure:
 #except (Exception, psycopg2.DatabaseError) as error:
  #session.rollback()
 # print("pgcode: " + str(error.pgcode))

 # if isinstance(error, psycopg2.errors.SerializationFailure):
 #   print("got instance of the SerializationFailure subclass")

 # else:
 #     print("got instance of the other subclass")
 #     raise error
 #except psycopg2.Error as err:
 #     print("Database psycopg.errors.SerializationFailure... ")
 #     excpStr = traceback.format_exc()
 #     if excpStr.find("SerializationFailure"):
 #       print(" found it to recurse...")
 #     print(excpStr)
 except:
      pass
      print("Database not connected successfully... chk rerun" + " tid= " + str(threading.get_ident()))
      #pool.putconn(conn)
      print("   chk rerun stoPro= " + line)
      excpStr = traceback.format_exc()
      print("    str==  "+ excpStr)
      found = excpStr.find("SerializationFailure")
      if (found != -1):
        print("   rerun - found it to recurse...")
        #execStoPro(line)
 sys.stdout.flush()

    
#a = executor.submit(my_function)
allQueries = []
k = 0
with open(sys.argv[2], 'r') as fptr:
     count = 0
     for line in fptr:
        name = line.strip()
        count += 1
        if((count % LOG_SKIP) < NUM):
           print("next Tx = " + name + '  ')
        #allQueries.append( name)
        message_dict[inputSeqNum] = name
        inputSeqNum += 1
        #execStoPro(line)
        k = k + 1
        #rows1 = cur1.fetchall()
        #for data in rows1:
        #  print("result " + str(data) )
        #if(k%10 == 0): # full
        #  break;
            # print(str(k) + " " + cur1.statusmessage)
message_dict[inputSeqNum] = ''
inputSeqNum += 1
#print(allQueries)
tStart = time.time()
poolFutures = []

with ThreadPoolExecutor(max_workers= NUM) as execPool: 

  mapAll = 0

  if  mapAll == 2:
   #for i in range(NUM):
     #print(message)
     #future = execPool.submit(ThreadWrapper)
     results = execPool.map(ThreadWrapper, itertools.repeat(None, NUM))
     for result in results:
        count += 1

  elif  mapAll == 1:
    # allQ change to msg-dict ; fewer refleaks than submit...
    #execPool.map(execStoPro, allQueries)
      results = execPool.map(execWrapper, itertools.repeat(None, inputSeqNum))
      count = 0
      for result in results:
        count += 1
        #if((count % LOG_SKIP) < NUM):
        #    print(result)

  else:
   for i in range(inputSeqNum):
     #print(message)
     future = execPool.submit(execWrapper, 0)
     poolFutures.append(future)

   try:
      for future in concurrent.futures.as_completed(poolFutures):
         result = future.result()
   except Exception as e:
                print(f"Error: {e}")

tEnd = time.time()
sys.stdout.flush()
print("overall time taken (millisec) = " + str((tEnd - tStart)*1000))
print(" total wait time (ms) " + str(totalWaitTime))

