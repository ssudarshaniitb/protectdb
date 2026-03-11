import matplotlib.pyplot as plt
import numpy as np

xpoints=[1,2,3,4,5,6,7,8]
#xpoints = np.array([1, 8])

safe129 = [9742, 6346, 4604, 4042, 3901, 4035, 3955, 4129]
pg129 = [9520, 5637, 4330, 3778, 3473, 3637,  3780, 3879]

num = 19700 
safi=[]
for yj in reversed(safe129):
    yp = num/yj
    safi.insert(0, yp)
    #print(yp)
   
pgpt=[]
for yj in reversed(pg129):
    yp = num/yj
    pgpt.insert(0, yp)
    #print(yp)
   
print(xpoints)
print(pgpt)
plt.xlim([0, 8.0])
plt.ylim([0, 6.0])
plt.rc('font', size=14)
plt.xlabel("Number of threads ", fontsize = 15)
plt.ylabel(" Throughput ( * 1000 tx/sec)",  fontsize = 15)
plt.tight_layout(pad=0.30)
plt.plot(xpoints, pgpt, marker='o')
plt.plot(xpoints, safi, marker='*')
plt.legend([
    'Postgres with pkey btree index (AWS)', 
    'proposed model with pkey btree index (AWS) '])
plt.savefig('thruput-safe+index-unit-vs-pg.eps', format='eps')
plt.show()

