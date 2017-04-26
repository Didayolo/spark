import time
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("rdd")
sc = SparkContext(conf=conf)


for n in range(1, 30):

    t1 = time.time() #########

    rdd = sc.parallelize(range(1,5))

    for i in range(n):    
        rdd = rdd.flatMap(lambda x : range(1,4*x+1))
     
    count = rdd.count()
        
    t2 = time.time() #########
    t = t2 - t1
                
    print("n = "+str(n))
    print("count = "+str(count))
    print("Temps d'execution: " +str(t)+" secondes.")

sc.stop()

