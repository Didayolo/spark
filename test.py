"""simpleApp.py"""
from pyspark import SparkContext

logFile = "/usr/lib/spark-2.1.0/README.md"  # Should be some file on your system
sc = SparkContext("local", "test")
logData = sc.textFile(logFile).cache()

numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

sc.stop()
