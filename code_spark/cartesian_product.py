import time
from pyspark import SparkContext, SparkConf
import operator

conf = SparkConf().setAppName("rdd")
sc = SparkContext(conf=conf)

t1 = time.clock()

distSeeds = sc.parallelize(['a', 'b', 'c', 'd'])


def distSucc(rdd):
    return rdd.cartesian(rdd)
    #return sc.union([rdd, rdd.cartesian(distSeeds)])

def allWords(n):
    ret = distSeeds
    for i in range(n-1):
        ret = distSucc(ret)
    return ret

n = 3

#print(allWords(n)).collect()

rdd = allWords(n)
count = rdd.count()

#counts = rdd.map(lambda word: 1).reduce(operator.add)
#print(counts)

t2 = time.clock()
print("n = "+str(n))
print("count = "+str(count))
print("Temps d'execution: " +str(t2 - t1)+" secondes.")

sc.stop()

