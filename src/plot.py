#!/usr/bin/env python
from pylab import *
from pprint import pprint

import sys

def best_average (values):
    return [(1 + min(x)) for x in best]

best = [[]]
average = [[]]
with open(sys.argv[1], "r") as file:
    current_gen = 0
    current_fold = 0
    for line in file:
        data = line.split("\t")

        if current_fold != int(data[0]):
            # If there is a new fold, plot the previous one and exit
            plot(best_average(best), '-', linewidth=1)
            best = [[]]
            average = [[]]
            current_fold = int(data[0])
            current_gen = 0

        if current_gen != int(data[1]):
            current_gen += 1
            best.append([])
            average.append([])

        best[current_gen].append(float(data[4]))
        average[current_gen].append(float(data[3]))

# Final plot
plot(best_average(best), '--', linewidth=1)

#subplot(211)
yscale("log")
grid(True)
xlabel('Generation')
ylabel('Average fitness (log)')

#subplot(212)
#yscale("log")
#grid(True)
#xlabel('Generation')
#ylabel('Average training fitness (log)')
#plot(average, '--', linewidth=1)

show()
