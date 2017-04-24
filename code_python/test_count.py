import time

def quick_pow4(n):
    pow4 = [1, 4, 16, 64, 256, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216, 67108864]
    return pow4[n]


### simple_calc version python ###
for n in range(4,14):
    t1 = time.time()
    tmp = quick_pow4(n)
    arr = [i for i in range(tmp)]

    """
    #count
    cpt = 0
    for i in arr:
        cpt += 1
    """
    
    t2 = time.time()
    print("n = " + str(n))
    #print("count = "+str(cpt))
    print("count = " + str(len(arr)))
    print("Temps d'execution: " +str(t2 - t1)+" secondes.")   
