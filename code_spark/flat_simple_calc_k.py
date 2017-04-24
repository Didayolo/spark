import time
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("rdd")
sc = SparkContext(conf=conf)

for k in range(1, 15):

    ln = []
    lt = []

    for n in range(1, 15 - k):

        t1 = time.time()

        rdd = sc.parallelize(range(4**k))

        for i in range(n):    
            #rdd = rdd.union(rdd)
            rdd = rdd.flatMap(lambda x : [x,x,x,x])
        
        count = rdd.count()

        t2 = time.time()
        t = t2 - t1

        #print("k = "+str(k)) 
        #print("n = "+str(n+k))
        #print("count = "+str(count))
        #print("Temps d'execution: " +str(t)+" secondes.")
        
        ln.append(n + k)
        lt.append(t)

    print("plt.plot("+str(ln)+", "+str(lt) +", 'b', linewidth=0.8, marker='+', label= K = '"+str(k)+"')")
    

sc.stop()

