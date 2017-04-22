from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("rdd")
sc = SparkContext(conf=conf)

seeds = ['']
distSeeds = sc.parallelize(seeds)

def distSucc(rdd):
    r = rdd.flatMap(lambda w: [w+'a', w+'b'])
    return r

print(distSucc(distSucc(distSeeds)).collect())


