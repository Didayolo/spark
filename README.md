# TER individualisé: Spark pour Map-Reduce distribués sur des ensembles définis récursivement

Étudiants: Adrien Pavao et Thomas Foltête
Encadrant: Nicolas Thiéry

## TODO:

### Github/git

- [X] Réviser git
- [X] Compte github Adrien: https://github.com/Didayolo
- [X] Compte github Thomas: https://github.com/tfoltete

### Python / arbres de génération

Implanter en Python des fonctions récursives pour:

- [x] Engendrer toutes les permutations de n
- [x] Engendrer tous les mots de longeur n sur un alphabet

### SageMath

- [x] Essayer http://cloud.sagemath.com/
- [x] Lire les [conventions de codage de Sage](http://doc.sagemath.org/html/en/developer/#writing-code-for-sage)
- [x] Installer SageMath sur vos machines (http://download.sagemath.org)

### Implantations existantes de RecursivelyEnumeratedSets + mapreduce

- [x] Jouer avec RecursivelyEnumeratedSets

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
        An enumerated set with a forest structure
        sage: 
        sage: C.map_reduce?

- [x] Lire le rapport de https://github.com/OpenDreamKit/OpenDreamKit/issues/107

### Exemples

- [ ] Monoides numériques
- [ ] Lire l'article ["Exploring the tree of Numerical Semigroups"](https://arxiv.org/find/all/1/all:+AND+Hivert+Fromentin/0/1/0/all/0/1)

### Spark

- [x] Commencer à lire la documentation de Spark
- [X] Facile a installer sur votre portable?
- [ ] Réunion Spark

      Vendredi 20 janvier de 9h00 à 12h00 dans la salle 166 du LAL

# Références

- https://spark.apache.org/
- http://sagemath.org/
- http://opendreamkit.org
- Calcul Mathématique avec Sage, chapitre 15.4, algorithmes génériques  http://sagebook.gforge.inria.fr/
- Implantation de MapReduce dans Sage, cas multiprocesseur: voir le rapport dans https://github.com/OpenDreamKit/OpenDreamKit/issues/107

- Sage developers guide: http://doc.sagemath.org/html/en/developer/
