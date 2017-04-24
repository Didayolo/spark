import time
t1 = time.time()
 
# code 
k = 0
for i in range(10000000):
    k += 1
print(k)
# code
 
t2 = time.time()

print("Temps d'execution: " +str(t2 - t1)+" secondes.")
