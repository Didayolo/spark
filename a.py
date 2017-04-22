from pyspark import SparkContext, SparkConf
from sage.all_cmdline import *   # import sage library

conf = SparkConf().setAppName("wordCount")
sc = SparkContext(conf=conf)

def allWords(alphabet, n):
      """ Engendre tous les mots de longueur n possible avec l'alphabet alphabet """
  
      #fichier = open("all_words.txt", "w")
      if n == 0:
          return []
  
      if n == 1:
          #fichier.write(str(alphabet))
          return alphabet
  
      result = []
      for l in alphabet:
          for p in allWords(alphabet, n-1):
              #fichier.write(str(l) + str(p) + " ")
              result.append(l + p)
      #fichier.close()
      return result

alphabet = ['a', 'b', 'c']
length = 4 #48 mots au total

#RDD
"""
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
"""

#RDD
x = sc.parallelize(alphabet)
words = allWords(alphabet, length)
seed = x
succ = lambda n: sc.parallelize([n + 'a', n + 'b']) if len(n) <= 16 else []
s = RecursivelyEnumeratedSet(seed, succ, structure = 'forest')

it = s.depth_first_search_iterator()
for _ in range(16): 
    print next(it)

sc.stop()
