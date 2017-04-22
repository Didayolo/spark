from pyspark import SparkContext, SparkConf
from gen import generator

conf = SparkConf().setAppName("rdd")
sc = SparkContext(conf=conf)

seeds = ['']
distSeeds = sc.parallelize(seeds)

def distSucc(rdd):
    r = rdd.flatMap(lambda w: [w+'a', w+'b', w+'c', w+'d'])
    return r


it = generator(distSeeds, distSucc)

print("Creation du rdd...")
for i in range(15):
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
counts = rdd.reduce(lambda a, b: a + b)
print("fait.")

#print(counts == pow(4, 16)) #range(15)
#print(rdd.collect())
print(counts)
#print(rdd.collect())
"""
#test generateur recursif
it = generator(distSeeds, distSucc)
print(next(it).collect())
print(next(it).collect())
"""
sc.stop()
