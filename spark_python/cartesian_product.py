from pyspark import SparkContext, SparkConf
import operator

conf = SparkConf().setAppName("rdd")
sc = SparkContext(conf=conf)

distSeeds = sc.parallelize(['a', 'b', 'c', 'd'])


def distSucc(rdd):
    return rdd.cartesian(rdd)
    #return sc.union([rdd, rdd.cartesian(distSeeds)])

def allWords(n):
    ret = distSeeds
    for i in range(n-1):
        ret = distSucc(ret)
    return ret

print(allWords(3)).collect()

#rdd = allWords(4)
#print(rdd.count())

#counts = rdd.map(lambda word: 1).reduce(operator.add)
#print(counts)


sc.stop()

