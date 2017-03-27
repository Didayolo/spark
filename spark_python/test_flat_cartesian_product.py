from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("rdd")
sc = SparkContext(conf=conf)

distSeeds = sc.parallelize(range(4**8))
rdd = distSeeds.cartesian(distSeeds)

print(rdd.count())


