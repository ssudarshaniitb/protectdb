import matplotlib.pyplot as plt
import numpy as np

xpoints=[1,2,3,4,5]

afullpoints =[13360, 12290, 12740, 11770, 11270]
kfkpoints =[9090, 8140, 7354, 7238, 7237]
#aws + safedb + signed + merkle
NUMTX = 10000

afull=[]
for yj in reversed(kfkpoints):
    afull.insert(0, NUMTX/yj)
    #print(yp)
   
aunit=[]
for yj in reversed(afullpoints):
    aunit.insert(0, 2* NUMTX/yj)

print(xpoints)
plt.xlim([0, 5.0])
plt.ylim([0, 5.0])
plt.rc('font', size=15)
plt.xlabel("Number of threads ", fontsize = 15)
plt.ylabel(" Throughput ( * 1000 tx/sec)",  fontsize = 15)
plt.tight_layout(pad=0.30)
plt.plot(xpoints, afull, marker='^')
plt.plot(xpoints, aunit, marker='o')
plt.legend([ 
    ' Cluster with only Kafka  (Local)', 
    ' Cluster with Raft + Kafka (Local)' ])
plt.savefig('thruput-threads+safe_with-vs-without_raft+kafka.eps', format='eps')
plt.show()

