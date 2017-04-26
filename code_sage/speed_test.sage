
# Vitesse map reduce sur count 4^n #

import time

seeds = [0]
succ = lambda x: [x+1,x+1,x+1,x+1] if x <= 10 else []
S = RecursivelyEnumeratedSet(seeds, succ, structure='forest')

map_function = lambda x: 1 if x == 10 else 0 
reduce_function = lambda x,y: x+y
reduce_init = 0


t1 = time.time()
    
count = S.map_reduce(map_function, reduce_function, reduce_init)

t2 = time.time()


print(count)
print(t2 - t1)
