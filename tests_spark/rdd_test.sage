from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("rdd")
sc = SparkContext(conf=conf)

seeds = ['']
distSeeds = sc.parallelize(seeds)

succ = lambda w: [w+'a', w+'b']

#def distSucc(rdd):
    

C = RecursivelyEnumeratedSet(seeds, succ, structure='forest')




it = C.depth_first_search_iterator()
for i in range(10):
    print(next(it))



