import matplotlib.pyplot as plt
import numpy as np

xpoints=[1,2,3,4,5]

kfkpoints =[9090, 8140, 7354, 7238, 7237]
safeUnit = [9742, 6346, 4606, 4042, 3901]
afullpoints =[13360, 12290, 12740, 11770, 11270]

num = 19700 
ref=[]
for yj in reversed(safeUnit):
    yp = num/yj
    ref.insert(0, yp)
    #print(yp)
   
kfkdata=[]
for yj in reversed(kfkpoints):
    yp = num/yj
    kfkdata.insert(0, yp)
    #print(yp)

aunit=[]
for yj in reversed(afullpoints):
    aunit.insert(0, num/yj)
   
print(xpoints)
print(kfkpoints)
print(ref)
print(kfkdata)
plt.xlim([0, 5.0])
plt.ylim([0, 6.0])
plt.rc('font', size=14)
plt.xlabel("Number of threads ", fontsize = 15)
plt.ylabel(" Throughput ( * 1000 tx/sec)",  fontsize = 15)
plt.tight_layout(pad=0.30)
plt.plot(xpoints, ref, marker='^')
plt.plot(xpoints, kfkdata, marker='o')
plt.plot(xpoints, aunit, marker='o')
plt.legend([ 
    'Single node (local)', 
    'Cluster with kafka (local)',
    'Cluster with Raft + kafka (local)',
    ])
plt.savefig('thruput-full-unit-vs-cluster+kfk.eps', format='eps')
plt.show()

