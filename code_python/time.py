import time
t1 = time.clock()
 
# code 
k = 0
for i in range(10000000):
    k += 1
print(k)
# code
 
t2 = time.clock()

print("Temps d'execution: " +str(t2 - t1)+" secondes.")
