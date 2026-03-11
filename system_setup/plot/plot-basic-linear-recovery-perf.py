import matplotlib.pyplot as plt
import numpy as np

xpoints=[1,2,3,4,5,6,7,8,9,10]

recoverIndex =[1463, 1524, 1553, 1605, 1646, 1687, 1774, 1822, 1857, 1866]

print(xpoints)
print(recoverTs)
plt.xlim([0, 10.0])
plt.ylim([000, 2000])
plt.rc('font', size=13)
plt.xlabel("Number of leaf nodes with corrupt tuples " , fontsize = 15)
plt.ylabel(" Recovery Time (msec)" , fontsize = 15)
plt.rcParams['xtick.labelsize'] = 16   # Fontsize of the x tick labels
plt.rcParams['ytick.labelsize'] = 16   # Fontsize of the y tick labels
plt.tight_layout(pad=0.30)
plt.plot(xpoints, recoverIndex, marker='o')
plt.savefig('corruption-recovery2.eps', format='eps')
plt.show()

