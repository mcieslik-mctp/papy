#!/usr/bin/env python
# -*- coding: utf-8 -*-
from numap import NuMap


def printer(element):
    print element
    return element

LEFT_INPUT = ('L0', 'L1', 'L2', 'L3')
RIGHT_INPUT = ('R0', 'R1', 'R2', 'R3')


# LEFT_INPUT     RIGHT_INPUT
# |              |
# |(printer)     |(printer)
# |              |
# left_iter      right_iter

numap = NuMap(stride=2)
left_iter = numap.add_task(printer, LEFT_INPUT)
right_iter = numap.add_task(printer, RIGHT_INPUT)
numap.start()

#                2       2       2       2
#                ------  ------  ------  ------
# order of input L0, L1, R0, R1, L2, L3, R2, R3

L0 = left_iter.next()
L1 = left_iter.next()
R0 = right_iter.next()
R1 = right_iter.next()
L2 = left_iter.next()
L3 = left_iter.next()
R2 = right_iter.next()
R3 = right_iter.next()

assert (L0, L1, L2, L3) == LEFT_INPUT
assert (R0, R1, R2, R3) == RIGHT_INPUT

