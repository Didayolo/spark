
### Tests de RecursivelyEnumeratedSets ###


# A -> Tous les mots de taille <= 4 compose de la lettre a
seeds = ['']
succ = lambda m: [m+'a'] if len(m)<4 else []
A = RecursivelyEnumeratedSet(seeds, succ, structure='forest')

def contains(rec_set, element, max_search):
    """ 
    Verifie la presence d'un element dans un RecursivelyEnumeratedSet.
    Attention ce probleme est semi-decidable pour un ensemble infini.
    
    Ici, on ne profite pas des proprietes du RecursivelyEnumeratedSet, on le parcours alors qu'il est prealablement stocke, tel une vulgaire liste python.
 
    Le parcours est un DFS.
    """
    
    set_size = count(rec_set)
    if(set_size < max_search):
        max_search = set_size

    it = rec_set.depth_first_search_iterator()
    
    for _ in range(max_search): # On evite le probleme de l'arret
        if next(it) == element:
            return True
    return False # Si le cardinal de l'ensemble est plus grand que max_search, False ne signifie pas necessairement que l'element n'est pas dans l'ensemble.
        

"""

it = A.depth_first_search_iterator()

seeds = ['']
succ = lambda w: [w+'a', w+'b']
C = RecursivelyEnumeratedSet(seeds, succ, structure='forest')

seeds = [(0, 0)]
succ = lambda a: [(a[0]+1,a[1]), (a[0],a[1]+1)]
S = RecursivelyEnumeratedSet(seeds, succ, structure='graded', max_depth=1)

"""

### Tests map_reduce ###

def count(rec_set):
    """ Renvoi la taille d'un RecursivelyEnumeratedSet """
    map_function = lambda x: 1 # Chaque element compte pour 1
    reduce_function = lambda x,y: x+y # On somme les nombre d'elements entre eux (operation associative)
    reduce_init = 0 # On demarre le compteur a 0
    return rec_set.map_reduce(map_function, reduce_function, reduce_init) # Le tant attendu map_reduce


def contains_mr(rec_set, element):
    """ Contains avec un map_reduce """
    map_function = lambda x: x == element
    reduce_function = lambda x,y: x or y
    reduce_init = False
    return rec_set.map_reduce(map_function, reduce_function, reduce_init)

print(count(A)) # 5
print(contains(A, 'aa', 10)) # True
print(contains(A, 'ab', 10)) # False
print(contains_mr(A, 'aa')) # True
print(contains_mr(A, 'ab')) # False
