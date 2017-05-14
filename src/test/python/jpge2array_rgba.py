#!/usr/local/bin/python3

from PIL import Image
import numpy as np
import sys

if len(sys.argv) != 2:
    print("wrong number of args")
    sys.exit(1)

path = sys.argv[1]
im = Image.open(path)
arr = np.array(im.convert("RGBA"))

shape=arr.shape
x=shape[0]
y=shape[1]
z=shape[2]

for i in range(x - 1):
    for j in range(y - 1):
        for k in range(z - 1):
            print("%d %d %d %d" % (i, j, k, arr[i, j, k]))
