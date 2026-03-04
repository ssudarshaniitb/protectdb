#!/usr/bin/python3

import psycopg2
import sys, json
import decimal
import traceback
import hashlib
import time
 
DB_USER = "username"
DB_PORT2 = "5433"
DB_PORT = "5432"
DB_HOST = "localhost"

DB_NAME = "safedb"
DB_PASS = "username"
DB_HOST = "10.129.148.179"
 
DB_NAME2 = "safedb"
DB_PASS2 = "username"
DB_HOST2 = "10.129.148.215"

DBG = 0
num_partitions = 100
leaves_per_partition = 4

func_key = {} # index-func must be named as tablename_fi and take one col as arg
func_key['table']= 'key-col'
func_key['savings']= 'userid'
func_key['usertable']= 'ycsb_key'
func_key['usertable2']= 'ycsb_key'
func_key['wh']= 'key-col'

def getDiffDeleteQ(rowDiff, tablename):
    query = ""
    for row in rowDiff:
      #print('del key === ' + str(row))
      pkey = str(row)
      query += "delete from " + tablename + " where " + func_key[tablename] + "= " + pkey + "; ";
    return query


def convert(data):
        #noDecimaldata = [tuple(str(item) for item in t) for t in data]
        noDecimaldata = "("
        quote = "'"
        comma = ', '
        last = data[-1]
        for item in data:
          if isinstance(item, decimal.Decimal):
            noDecimaldata += str(float(item)) 
          elif type(item) is str:
            noDecimaldata += quote + item + quote 
          else:
            noDecimaldata += '' + str(item)
          if(item != last):
            noDecimaldata += ", "
        #print("final == " + str(noDecimaldata))
        return noDecimaldata

#print("arglen== " , len(sys.argv))
if(len(sys.argv) < 4):
  print("Err: missing arguments... ")
  print("Usage: python3 <scriptname> <healthy_ip> <tablename> <1 for commit_fetched_differences>\n \t .....  outFile pg_diff_fix_cmd.txt");
  exit(-1)

tablename = sys.argv[2]
DB_HOST2 = sys.argv[1]

if( tablename not in func_key): 
  print("Error: undefined key and functional index for  relation name... " + sys.argv[1])
  exit(-1)

def getkeys(rows):
        keylist = ''
     #for j in range(len(rows)):
        set1 = {r[0] for r in rows}
        #print('set1 = ' + str(set1))
        nraw = 0
        for raw_str in sorted(set1):
            clean_klist = raw_str.strip("{}").split(",")
            #print( 'clean = ' + str(clean_klist))

    #  Convert each string piece into an actual integer
            nkey = 0
            for key in clean_klist:
                keylist += str(key) 
                nkey += 1
                if (nkey < len(clean_klist)):
                    keylist += ','
            nraw += 1
            if(nkey >0) and (nraw != len(set1)):
                    keylist += ','

        if(DBG == 1):
          print( 'clean = ' + str(keylist))
        return str(keylist)

start_time = time.time_ns()
try:
    conn2 = psycopg2.connect(database=DB_NAME,
                            user=DB_USER,
                            password=DB_PASS,
                            host=DB_HOST2,
                            port=DB_PORT)
    print("Database2 connected successfully")
    conn = psycopg2.connect(database=DB_NAME,
                            user=DB_USER,
                            password=DB_PASS,
                            host=DB_HOST,
                            port=DB_PORT)
    print("Database connected successfully")

    cur1 = conn.cursor()
    cur1.execute("SELECT merkle_tree_stats('" + tablename + "');")

         # Retrieve query results
    for record in cur1.fetchall():
             print(f"key-ID: {record[0]}")
    try:
             # Parse the JSON string into a dictionary
             data = json.loads(record[0])

             # Extracting specific fields
             fanout = data.get("fanout")
             leaves_per_partition =  data.get("leaves_per_partition")
             nodes_per_partition =  data.get("nodes_per_partition")
             num_partitions = data.get("num_partitions")
             # index_keys is a list, so we take the first element

    except json.JSONDecodeError as e:
             print(f"Failed to parse JSON: {e}")

    print(f"nodes_per_partition: {nodes_per_partition}")
    print(f"leaves_per_partition: {leaves_per_partition}")
    print(f"Total Partitions: {num_partitions}")
    #num_partitions = 2
    fanout = 2

    phaseSep = "*************************************************"
    print('\n\n'+phaseSep)
    print("Part-1: Comparing merkle trees \n\t\t Config: database relation:  \"" + tablename + "\" \n\t\t local corrupt node : " + DB_HOST + " \n\t\t healthy reference: " + DB_HOST2 ) 
    print(phaseSep)
    nodes = ""
    outFilename = '/tmp/pg_diff_fix_cmd.txt'
    outFile = open(outFilename, 'w')
    for i in range(num_partitions):
        nodes += '\'' + str(i) + '_1\'' 
        if i < (num_partitions - 1):
            nodes += ','

    exit
    commitDiff = int(sys.argv[3])
    cur2 = conn2.cursor()

    is_leaf = False
    level = 0
    replica_match = 0
    while (is_leaf is False):
      level = level + 1
      if len(nodes) == 0:
        replica_match = 1
        break
      args = " in (" + nodes  + " ) order by nodeid "
      print("\nLevel-" + str(level) + ": node-id(s) = " + nodes + '\n')
      q = " SELECT * from merkle_node_hash('" + tablename + "') where nodeid " + args +";"
      print('q= ' + q)
      cur1.execute(q)
      cur2.execute(q)
      rows1 = cur1.fetchall()
      for data in rows1:
        is_leaf = data[3]
        if(DBG == 1):
          print(" local leaf=" + str(is_leaf) +" nodeID:" + str(data[0]) + " hash:" + data[5])
      rows2 = cur2.fetchall()
      if(DBG == 1):
        for data in rows2:
          print("  remote - nodeID:" + str(data[0]) + "  hash col :" + data[5])
      #print(rows1)
      sames = list(set(rows1).intersection(rows2))
      #print("\ncommon items : " + str(sames))

      diffs = list(set(rows1).difference(rows2))
      dnodes = ""
      ndiff = 0
      for diff in diffs:
          ndiff += 1
          dnodes += '\'' + str(diff[0]) + "\'  "
          if (ndiff < len(diffs)):
              dnodes += ','
      if(len(dnodes) == 0):
          dnodes = 'None'
      print("\n  Differing merkle node(s) : " + dnodes + '\n' + phaseSep)
      count = 0
      nodes = ""

      prev_level2_nodes = fanout ** (level-2)
      if(prev_level2_nodes < 0):
          prev_level2_nodes = 0

      prev_level_nodes = fanout ** (level-1)
      if(prev_level_nodes < 0):
          prev_level_nodes = 0

      ndiff = 0
      if (is_leaf is True):
          nodes = dnodes
          break

      for diff in diffs:
        ndiff += 1

        part, node_in_part = diff[0].split('_')
        current_n_id = int(node_in_part)
        #print(' split = ' + part+ node_in_part)
        # general formula firstNodeId = fanout * ( parentId -1 ) +2
        firstNodeId = 2* (current_n_id )
        firstNodeId = fanout * ( current_n_id -1 ) +2

        for i in range(fanout):
            nodeId = firstNodeId + i 
            #print( ' nid = ' + str(nodeId))
            nodes += '\'' + part +'_' + str(nodeId) + '\''
            if(i < fanout - 1):
                nodes += ','
        if (ndiff < len(diffs)):
            nodes += ','

      time.sleep(0.25)

    if(replica_match == 1):
        print('\n\n'+phaseSep)
        print("Replica database states match !! ")
        print(phaseSep+'\n')
        exit()
    #if(len(nodes) != 0):
    print('\n\n'+phaseSep)
    print("Part-2 : Find record from differing leaf ")
    print(phaseSep+'\n')
    print("Differing merkle node-ids = " + nodes)
    lookupQ = " SELECT leaf_id FROM merkle_node_hash('" + tablename + "') where " +  " nodeid in (" + nodes + ");"
    print("\n  q = " + lookupQ)
    cur1.execute(lookupQ)
    rows1 = cur1.fetchall()
    leafids = ''
    nrow = 0
    for row in rows1:
         leafids += '\''+str(row[0]) +'\''
         nrow += 1
         if(nrow < len(rows1)):
             leafids += ','
    print("\n  merkle leaf IDs= " + leafids)
      #print("Step (2a) Differing Merkle tree node " + node)
    lookupQ = " SELECT keys FROM merkle_leaf_tuples('" + tablename + "') where " +  " leaf_id in (" + leafids + ") order by leaf_id;"
    print("\nStep (2b) Reverse lookup : \n\n  " + lookupQ)
      #print("\n\t" + lookupQ)
    cur1.execute(lookupQ)
    rows1 = cur1.fetchall()
    cur2.execute(lookupQ)
    rows2 = cur2.fetchall()
    keylist = getkeys(rows1)
    if(DBG == 1):
        print('klist1 = ' + keylist)
        print('klist2 = ' + (getkeys(rows2)))
    if(len(keylist) > 0):
        keylist += ',' 
    keylist += (getkeys(rows2))
    keys = keylist.split(',')
    kset = (set(keys))
    print('\nNet user relation keys from differing merkle leaf node(s)= ' + str(kset))
    nkey = 0
    keylist = ''
    for key in kset:
                keylist += str(key) 
                nkey += 1
                if (nkey < len(kset)):
                    keylist += ','

    if(DBG == 1):
        print('keys = ' + str(keys))
        print('klist = ' + str(keylist))

    detect_time = time.time_ns()
      #print("del len = " + str(len(toDelete)))
    lookupQ = " SELECT * FROM " + tablename + " where " +  " ycsb_key in (" + str(keylist) + ");"
    if(DBG == 1):
        print("\n\tdelq == " + lookupQ)
    cur1.execute(lookupQ)
    rows1 = cur1.fetchall()
    cur2.execute(lookupQ)
    rows2 = cur2.fetchall()
    if(DBG == 1):
        for row in rows1:
          print('row=== ' + str(row[0]))
    if(len(keylist) != 0):
        delQ = getDiffDeleteQ(keylist.split(','), tablename)
        #print('commands = ' + delQ)
        print(delQ, file=outFile)
        if(commitDiff == 1):
          cur1.execute(delQ)
    else:
        print("del list null ")

    #print('klist = ' + str(keylist))

    lookupQ = " SELECT * FROM " + tablename + " where " +  " ycsb_key in (" + str(keylist) + ");"
    cur2.execute(lookupQ)
    rows2 = cur2.fetchall()
    if(DBG == 1):
        for item in rows2:
          print('\nStep2 addupd Differing record = ' + str(item) + '\n')
    if(len(rows2) == 0):
        print("add list null ")
    else:
       for data in rows2:
          query = convert(data)
          insertQ = " insert into " + tablename + " values " + str(query) + ");"
        #print(insertQ)
          print(insertQ, file=outFile)
          if(commitDiff == 1):
            cur1.execute(insertQ)
    conn.commit()
    conn2.commit()
    conn.close()
    conn2.close()
    outFile.close()
    end_time = time.time_ns()
    print('\ndetect time (msec) = ' + str((detect_time - start_time)/(1000*1000)))
    print('\noverall time (msec) = ' + str((end_time - start_time)/(1000*1000)))

except SystemExit:
    pass
except:
    print("Database not connected successfully")
    print(traceback.format_exc())

