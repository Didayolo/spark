import copy
class semi_group:
    def __init__(self, c, g, m, G):
        self.c = c # conductor (max du complementaire +1)
        self.g = g # genus (cardinal complementaire)
        self.m = m # multiplicity (min S\{0})
        self.d = []
        self.irr = [] #ensemble complementaire irreductible
    def __str__(self):
        return "Conductor : {}, Genus : {}, Multiplicity : {}, Ds : {}, Irr : {}".format(self.c, self.g, self.m, self.d, self.irr)

def root(G):
    """ Racine de l'arbe Tg (les semi-groups avec genus inferieur ou egal a G"""
    R = semi_group(1, 0, 1, G)

    for i in range(3*G):
        R.d.append(True)

    R.irr.append(1)
    return [R]

def son(S, x, G): 
    """ fonction successeur """

    is_irreductible = False
    sx_irr = []
    for v in S.irr:
        #print "irr(x) " + str(x)
        if v < x:
            sx_irr = copy.deepcopy(S.irr)
            sx_irr.append(v)
        elif v == x:
            is_irreductible = True
            break
   
    if not is_irreductible:
        print "{} is not irreductible".format(x)
        return ;

    Sx = semi_group(x + 1, S.g + 1, x, G)
    Sx.d = S.d
    Sx.irr = sx_irr
    #remove x
    Sx.d[x] = False

    #To find new irreductibles : sons
    for y in range(x+1, 3*G):
        if Sx.d[y]:
            found = False
            
            for i in range(1, y/2):
                if Sx.d[i] and Sx.d[y-i]:
                    #y is not irreductible
                    #print str(y) + ": is not irreductible"
                    found = True
                    break
            
            if not found:
                Sx.irr.append(y)
    return Sx

seeds = root(4)
G_test = 4
#succ = lambda m: []
def succ(s):
    r = []
    for x in range(3 * G_test):
        sx = son(s, x, G_test)
        if sx:
            r.append(sx)
    return r


Tg = RecursivelyEnumeratedSet(seeds, succ, structure='forest')
it = Tg.breadth_first_search_iterator()
for i in range(10):
    print(next(it))



"""
# A -> N (le semigroupe numerique le plus evident)
seeds = [0]
succ = lambda m: [m+1]
A = RecursivelyEnumeratedSet(seeds, succ, structure='forest')

# B -> tqt meme pas
seeds = [0, 1]
succ = lambda m: [m+2]
B = RecursivelyEnumeratedSet(seeds, succ, structure='forest')

#it = A.depth_first_search_iterator()
#for i in range(10):
#    print(next(it))

itb = B.braidth_first_search_iterator()
for i in range(10):
    print(next(itb))
"""
