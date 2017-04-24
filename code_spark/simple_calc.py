import time
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("rdd")
sc = SparkContext(conf=conf)

for n in range(4, 17):

    t1 = time.time()

    rdd = sc.parallelize(range(4**n))
    count = rdd.count()

    t2 = time.time()
    print("n = "+str(n))
    print("count = "+str(count))
    print("Temps d'execution: " +str(t2 - t1)+" secondes.")

sc.stop()

