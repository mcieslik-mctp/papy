#!/usr/bin/env python
# -*- coding: utf-8 -*-
from numap import NuMap

def hello_world(element, *args, **kwargs):
    print "Hello element: %s " % element,
    print "Hello args: %s" % (args,),
    print "Hello kwargs: %s" % (kwargs,)
    return element

ELEMENTS = ('element_0', 'element_1', 'element_2', 'element_3', 'element_4')

result_iterator = NuMap(hello_world, ELEMENTS,
                        args=('arg_0', 'arg_1'),
                        kwargs={'kwarg_0':'val_0', 'kwarg_1':'val_1'})
results = tuple(result_iterator)
assert results == ('element_0', 'element_1', 'element_2', 'element_3', 'element_4')

