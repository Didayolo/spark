# spark
Test TER individualisé

## TODO:

### Créer un compte github, réviser git
Compte github Adrien: https://github.com/Didayolo

### Implanter en Python des fonctions récursives pour:

- Engendrer toutes les permutations de n
- Engendrer tous les mots de longeur n sur un alphabet

### Essayer http://cloud.sagemath.com/

### Installer SageMath sur vos machines

download.sagemath.org

### Jouer avec RecursivelyEnumeratedSets

sage: RecursivelyEnumeratedSet?
sage: sage: f = lambda a: [a+3, a+5]
....:       sage: C = RecursivelyEnumeratedSet([0], f)
....:       sage: C
....: 
A recursively enumerated set (breadth first search)
sage: 
sage: C
A recursively enumerated set (breadth first search)
sage: RecursivelyEnumeratedSet?
sage:  f = lambda a: [2*a,2*a+1]
....:       sage: C = RecursivelyEnumeratedSet([1], f, structure='forest')
....:       sage: C
....: 
An enumerated set with a forest structure
sage: 
sage: C.map_reduce?

### Lire le rapport de #107 ci dessous

### Commencer à lire la documentation de Spark

### Facile a installer sur votre portable?

### Réunion Spark le 20

Le vendredi 20 janvier de 9h00 à 12h00 dans la salle 166 du LAL

# Références

- https://spark.apache.org/
- http://sagemath.org/
- http://opendreamkit.org
- Calcul Mathématique avec Sage, chapitre 15.4, algorithmes génériques  http://sagebook.gforge.inria.fr/
- Implantation de MapReduce dans Sage, cas multiprocesseur: voir le rapport dans https://github.com/OpenDreamKit/OpenDreamKit/issues/107
