import time
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("rdd")
sc = SparkContext(conf=conf)

for n in range(2, 9):

    t1 = time.time()

    distSeeds = sc.parallelize(range(4**n))
    rdd = distSeeds.cartesian(distSeeds)

    #print(rdd.count())
    count = rdd.count()

    #num partitions ?

    t2 = time.time()
    print("n = "+str(n*2))
    print("count = "+str(count))
    print("Temps d'execution: " +str(t2 - t1)+" secondes.")

sc.stop()

