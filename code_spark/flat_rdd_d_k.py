import time
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("rdd")
sc = SparkContext(conf=conf)

ln = []
lt = []

for k in range(7, 17):

    for n in range(1, 15):

        t1 = time.time()

        rdd = sc.parallelize(range(4))

        for i in range(n):    
        
            rdd = rdd.flatMap(lambda x : [x,x,x,x])
        
#            if (i==k): # seuil
#                rdd = rdd.map(lambda x : x).cache() # flatten
#                rdd.first()
       
        count = rdd.count()

        t2 = time.time()

        t = t2 - t1
                
        print("k = "+str(k))
        print("n = "+str(n))
        print("count = "+str(count))
        print("Temps d'execution: " +str(t)+" secondes.")
        ln.append(n)
        lt.append(t)

    print(ln)
    print(lt)    
        
sc.stop()

