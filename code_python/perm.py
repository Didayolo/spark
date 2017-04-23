def permutation(n):
	""" Engendre toutes les permutations de n """
	strn = str(n)

	if len(strn)==1:
		return [strn]
	
	result = []
	for i, v in enumerate(strn):
		for p in permutation(strn[:i] + strn[i+1:]):
			result.append(v + p)

	return result

#tests
#print(permutation(1))
#print(permutation(12))
#print(permutation(123))
#print(permutation(8787))
	

def allWords(alphabet, n):
    """ Engendre tous les mots de longueur n possible avec l'alphabet alphabet """
	
    fichier = open("../all_words.txt", "w")
    if n == 0:
        return []

    if n == 1:
        fichier.write(str(alphabet))
        return alphabet	

    result = []
    for l in alphabet:
        for p in allWords(alphabet, n-1):
            fichier.write(str(l) + str(p) + " ")
            result.append(l + p)
    fichier.close()
    return result

#tests
alphabet = ["a", "b", "c", "d", "e"]
allWords(alphabet, 9)
#print(allWords(alphabet, 0))
#print(allWords(alphabet, 1))
#print(allWords(alphabet, 2))
#print(allWords(alphabet, 4))
