import time
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("rdd")
sc = SparkContext(conf=conf)

k = 5

for n in range(1, 17):

    t1 = time.time()

    rdd = sc.parallelize(range(4**k))

    for i in range(n):    
        rdd = rdd.flatMap(lambda x : [x,x,x,x])
        
    count = rdd.count()

    t2 = time.time()
    print("n = "+str(n))
    print("count = "+str(count))
    print("Temps d'execution: " +str(t2 - t1)+" secondes.")

sc.stop()

