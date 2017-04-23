import time
from pyspark import SparkContext, SparkConf
from gen import generator

conf = SparkConf().setAppName("rdd")
sc = SparkContext(conf=conf)

t1 = time.clock()

seeds = ['']
distSeeds = sc.parallelize(seeds)

def distSucc(rdd):
    r = rdd.flatMap(lambda w: [w+'a', w+'b', w+'c', w+'d'])
    return r


it = generator(distSeeds, distSucc)

n = 12

print("Creation du rdd...")
for i in range(n):
    rdd = next(it)
    print(rdd.getNumPartitions())
    
print("fait.")

print(rdd.getNumPartitions())

"""
print("map reduce...")
counts = rdd.map(lambda word: 1) \
            .reduce(lambda a, b: a + b)
print("fait.")
"""

#test separation map et reduce
print("map...")
rdd = rdd.map(lambda w: 1)
print("fait.")
print(rdd.getNumPartitions())
print("reduce...")
count = rdd.reduce(lambda a, b: a + b)
print("fait.")

#print(counts == pow(4, 16)) #range(15)
#print(rdd.collect())
#print(counts)
#print(rdd.collect())
"""
#test generateur recursif
it = generator(distSeeds, distSucc)
print(next(it).collect())
print(next(it).collect())
"""

t2 = time.clock()
print("n = "+str(n))
print("count = "+str(count))
print("Temps d'execution: " +str(t2 - t1)+" secondes.")

sc.stop()
