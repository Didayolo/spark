import time
from pyspark import SparkContext, SparkConf
from gen import generator

conf = SparkConf().setAppName("rdd")
sc = SparkContext(conf=conf)

def distSucc(rdd):
    r = rdd.flatMap(lambda w: [w+'a', w+'b', w+'c', w+'d'])
    return r


for n in range(4, 17):

    t1 = time.time()

    # a et b pas naturel a implementer

    seeds = ['']
    distSeeds = sc.parallelize(seeds)

    it = generator(distSeeds, distSucc)

    for i in range(n+1):
        rdd = next(it)

    count = rdd.map(lambda word: 1) \
                .reduce(lambda a, b: a + b)


    t2 = time.time()

    print("n = "+str(n))
    print("count = "+str(count))
    print("Temps d'execution: " +str(t2 - t1)+" secondes.")

#print(counts == pow(4, 16)) #range(15)
#print(rdd.collect())
#print(rdd.collect())
"""
#test generateur recursif
it = generator(distSeeds, distSucc)
print(next(it).collect())
print(next(it).collect())
"""

sc.stop()
