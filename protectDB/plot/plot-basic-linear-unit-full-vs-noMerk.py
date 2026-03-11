import matplotlib.pyplot as plt
import numpy as np

xpoints=[1,2,3,4,5,6,7,8]

refpoints =[20090, 11978, 9163, 8111, 8024, 8200, 8315, 8346]
d2points =[19686, 11885, 9117, 7921, 7700, 7892, 7892, 7892]

num = 19700 
ref=[]
for yj in reversed(refpoints):
    yp = num/yj
    ref.insert(0, yp)
    #print(yp)
   
data2=[]
for yj in reversed(d2points):
    yp = num/yj
    data2.insert(0, yp)
    #print(yp)
   
print(xpoints)
print(d2points)
print(ref)
print(data2)
plt.xlim([0, 8.0])
plt.ylim([0, 4.0])
plt.rc('font', size=15)
plt.xlabel("Number of threads ", fontsize = 15)
plt.ylabel(" Throughput ( * 1000 tx/sec)",  fontsize = 15)
plt.tight_layout(pad=0.30)
plt.plot(xpoints, ref, marker='^')
plt.plot(xpoints, data2, marker='o')
plt.legend([
    'proposed model with merkle tree' ,
    'proposed model without merkle tree' ])
plt.savefig('thruput-unit-deterministic-vs-noMerk.eps', format='eps')
        #, format='eps')
plt.show()

