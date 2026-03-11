import matplotlib.pyplot as plt
import numpy as np

xpoints=[1,2,3,4,5,6,7,8]
#xpoints = np.array([1, 8])

safe129 = [9742, 6346, 4604, 4042, 3901, 4035, 3955, 4129]
safeUnsigned = [ 7828, 4723, 3838, 3589, 3653, 3707, 3954, 3991]

num = 19700 
ref=[]
for yj in reversed(safe129):
    yp = num/yj
    ref.insert(0, yp)
    #print(yp)
   
data2=[]
for yj in reversed(safeUnsigned):
    yp = num/yj
    data2.insert(0, yp)
    #print(yp)
   
print(xpoints)
print(d2points)
print(ref)
print(data2)
plt.xlim([0, 8.0])
plt.ylim([0, 6.0])
plt.rc('font', size=14)
plt.xlabel("Number of threads ", fontsize = 15)
plt.ylabel(" Throughput ( * 1000 tx/sec)",  fontsize = 15)
plt.plot(xpoints, ref, marker='^')
plt.plot(xpoints, data2, marker='o')
plt.legend([ 
    'proposed model (signed transactions)', 
    'proposed model (unsigned transactions)'])
plt.savefig('thruput-unit-full-vs-unsigned.eps', format='eps')
        #, format='eps')
plt.show()

