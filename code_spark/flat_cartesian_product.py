import time
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("rdd")
sc = SparkContext(conf=conf)

t1 = time.clock()

n = 8

distSeeds = sc.parallelize(range(4**n))
rdd = distSeeds.cartesian(distSeeds)

#print(rdd.count())
rdd.count()

t2 = time.clock()
print("n = "+str(n))
print("Temps d'execution: " +str(t2 - t1)+" secondes.")

sc.stop()

