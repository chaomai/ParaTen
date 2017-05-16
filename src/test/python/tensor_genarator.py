#!/usr/bin/env python3

import random
import sys
import time

# input format: type[s | d] dimension[1,2,3,4,5]
# s: sparse
# d: dense
if len(sys.argv) != 3:
    print("wrong number of args")
    sys.exit(1)

random.seed(time.time())

def generator(dims):
    head, *tail = dims

    for idx in range(head):
        if len(tail) != 1:
            for i in generator(tail):
                yield str(idx) + " " + str(i)
        else:
            for x in range(tail[0]):
                yield str(idx) + " " + str(x) + " " + str(random.gauss(0, 0.5) * 100)

t = sys.argv[1]
nums = sys.argv[2]
nums = list(map(lambda x: int(x), nums.split(',')))

for r in generator(nums):
    if t == 's':
        if random.uniform(-3, 5) > 0:
            print(r)
    else:
        print(r)
