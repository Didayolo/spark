import time
from pyspark import SparkContext, SparkConf
import operator

conf = SparkConf().setAppName("rdd")
sc = SparkContext(conf=conf)

def distSucc(rdd):
    return rdd.cartesian(rdd)
    #return sc.union([rdd, rdd.cartesian(distSeeds)])
 
def allWords(n):
    ret = distSeeds
    for i in range(n-1):
        ret = distSucc(ret)
    return ret

for n in range(2, 5):

    t1 = time.time()

    distSeeds = sc.parallelize(['a', 'b', 'c', 'd'])

    rdd = allWords(n)
    #count = rdd.count()

    #print(allWords(n)).collect()
    count = rdd.map(lambda word: 1).reduce(operator.add)
    #print(counts)

    t2 = time.time()
    print("n = "+str(2**(n-1)))
    print("count = "+str(count))
    print("Temps d'execution: " +str(t2 - t1)+" secondes.")

sc.stop()

