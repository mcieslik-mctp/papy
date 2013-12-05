#!/usr/bin/env python
# -*- coding: utf-8 -*-
from numap import NuMap

def hello_world(element):
    print("Hello element: %s" % element)
    return element

ELEMENTS = ('element_0', 'element_1', 'element_2', 'element_3', 'element_4')

result_iterator = NuMap(hello_world, ELEMENTS)
results = tuple(result_iterator)
assert results == ('element_0', 'element_1', 'element_2', 'element_3', 'element_4')

