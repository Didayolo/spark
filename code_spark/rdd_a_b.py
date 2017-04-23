import time
from pyspark import SparkContext, SparkConf
from gen import generator

conf = SparkConf().setAppName("rdd")
sc = SparkContext(conf=conf)

t1 = time.clock()

# a et b pas naturel a implementer

seeds = ['']
distSeeds = sc.parallelize(seeds)

def distSucc(rdd):
    r = rdd.flatMap(lambda w: [w+'a', w+'b', w+'c', w+'d'])
    return r


it = generator(distSeeds, distSucc)

n = 5

for i in range(n):
    rdd = next(it)

counts = rdd.map(lambda word: 1) \
            .reduce(lambda a, b: a + b)


#print(counts == pow(4, 16)) #range(15)
#print(rdd.collect())
print(counts)
print(rdd.collect())
"""
#test generateur recursif
it = generator(distSeeds, distSucc)
print(next(it).collect())
print(next(it).collect())
"""
t2 = time.clock()
print("n = "+str(n))
print("Temps d'execution: " +str(t2 - t1)+" secondes.")

sc.stop()
