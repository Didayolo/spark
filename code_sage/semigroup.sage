
class semi_group:
    def __init__(self, c, g, m, G):
        self.c = c # conductor (max du complementaire +1)
        self.g = g # genus (cardinal complementaire)
        self.m = m # multiplicity (min S\{0})
        self.d = [0] * (G*3)
    def __str__(self):
        return "conducteur : {}, genus : {}, multiplicity : {}, ensemble complementaire {}".format(self.c, self.g, self.m, self.d)

def root(G):
    """ Racine de l'arbe Tg (les semi-groups avec genus inferieur ou egal a G"""
    R = semi_group(1, 0, 1, G) #(0, 0, 1, G)
    for x in range(3 * G):
        R.d[x] = 1 + (x/2)
    return [R]

def son(S, x, G):
    """ fonction successeur """
    if x > S.m:
        m = S.m
    else:
        m = S.m + 1
    Sx = semi_group(x + 1, S.g + 1, m, G)
    Sx.d = S.d

    for y in range(x, 3*G):
        if S.d[y - x] > 0:
            Sx.d[y] = S.d[y] - 1
    return Sx

seeds = root(4)
#succ = lambda m: []
def succ(m):
    r = []
    for x in range(3*4):
        r.append(son(m, x, 4))
    return r


Tg = RecursivelyEnumeratedSet(seeds, succ, structure='forest')
print(Tg)
it = Tg.depth_first_search_iterator()
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
