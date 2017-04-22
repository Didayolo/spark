from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("wordCount")
sc = SparkContext(conf=conf)

#RDD
txtFile = "all_words.txt" #genere par perm.py
txtData = sc.textFile(txtFile)
txtData.cache() #lazy
#peut aussi etre cree a partir d'un ensemble fini e avec sc.parallelize(e)

wcData = txtData.flatMap(lambda line: line.split(" ")) \
            .map(lambda word: (1)) \
            .reduce(lambda a, b: a + b)

wcData2 = txtData.flatMap(lambda line: line.split(" ")).count()

print "number of words (1) : %i" %(wcData - 1) #un espace en trop en fin de fichier
print "number of words (2) %i" %(wcData2 - 1)

sc.stop()
