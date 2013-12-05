#!/usr/bin/env python
# -*- coding: utf-8 -*-
from multiprocessing import TimeoutError
from numap import NuMap
from time import sleep


SLEEP = 'sleep'
AWAKE = 'awake'

def sleeper(element):
    if element == SLEEP:
        sleep(2)
    return element

ELEMENTS = (AWAKE, SLEEP, AWAKE, AWAKE)

print "results, which timeout are **not** skipped"
result_iterator = NuMap(sleeper, ELEMENTS, skip=False)
print result_iterator.next(timeout=3)
try:
    result_iterator.next(timeout=1)
except TimeoutError:
    print 'timout'
print result_iterator.next(timeout=3)
print result_iterator.next(timeout=3)
print result_iterator.next(timeout=3)
print "got 4 results\n"


print "results, which timeout are skipped"
result_iterator = NuMap(sleeper, ELEMENTS, skip=True)
print result_iterator.next(timeout=3)
try:
    result_iterator.next(timeout=1)
except TimeoutError:
    print 'timout'
print result_iterator.next(timeout=3)
print result_iterator.next(timeout=3)
print "got 3 results\n"


