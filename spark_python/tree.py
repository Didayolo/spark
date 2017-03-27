from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("rdd")
sc = SparkContext(conf=conf)

seeds = [1]
distSeeds = sc.parallelize(seeds)

def t(n):
    """ t(n) est l'ensemble des arbres binaires a n noeuds"""
    res = []
    if n == 0:
        return 1
    else:
        for i in range(n):
            res.append((t(i), 0, t(n-i-1))) # 0 -> node
    return res

def distSucc(rdd):
    r = rdd.flatMap(lambda t: t)
    return r

#print(distSucc(distSucc(distSeeds)).collect())

print(t(2))


