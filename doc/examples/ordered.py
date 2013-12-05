#!/usr/bin/env python
# -*- coding: utf-8 -*-
from numap import NuMap
from time import sleep

SLEEP = 'sleep'
AWAKE = 'awake'

def sleeper(element):
    if element == SLEEP:
        sleep(2)
    return element

ELEMENTS = (AWAKE, SLEEP, AWAKE, AWAKE)

print "if ordered results are in the order of the input"
result_iterator = NuMap(sleeper, ELEMENTS, ordered=True)
results = tuple(result_iterator)
print 'got: ', results

print "if not ordered results are in the order of the computation"
result_iterator = NuMap(sleeper, ELEMENTS, ordered=False)
results = tuple(result_iterator)
print 'got: ', results
