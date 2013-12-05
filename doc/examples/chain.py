#!/usr/bin/env python
# -*- coding: utf-8 -*-
from numap import NuMap

def source(element):
    print "in source: %s" % (element,)
    return element

def pipe(element):
    print "in pipe: %s" % (element,)
    return element

def sink(element):
    print "in sink: %s" % (element,)
    return element

ELEMENTS = ('element_0', 'element_1', 'element_2', 'element_3', 'element_4')


nu_chain = NuMap()
source_out = nu_chain.add_task(source, ELEMENTS)
pipe_out = nu_chain.add_task(pipe, source_out)
sink_out = nu_chain.add_task(sink, pipe_out)
nu_chain.start()

results = tuple(sink_out)
assert results == ('element_0', 'element_1', 'element_2', 'element_3', 'element_4')

