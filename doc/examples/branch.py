#!/usr/bin/env python
# -*- coding: utf-8 -*-
from numap import NuMap
from itertools import izip

def left(element):
    print "in left: %s" % (element,)
    return element

def right(element):
    print "in right: %s" % (element,)
    return element

def root(element):
    print "in root: %s" % (element,)
    return element

LEFT = ('left_0', 'left_1', 'left_2', 'left_3', 'left_4')
RIGHT = ('right_0', 'right_1', 'right_2', 'right_3', 'right_4')

nu_chain = NuMap()
left_out = nu_chain.add_task(left, LEFT)
right_out = nu_chain.add_task(right, RIGHT)
root_out = nu_chain.add_task(root, izip(left_out, right_out))
nu_chain.start()

results = tuple(root_out)
assert results == (('left_0', 'right_0'), ('left_1', 'right_1'),
                   ('left_2', 'right_2'), ('left_3', 'right_3'),
                   ('left_4', 'right_4'))

