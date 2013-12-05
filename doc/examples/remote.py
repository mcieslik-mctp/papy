#!/usr/bin/env python
# -*- coding: utf-8 -*-
from numap import NuMap, imports

#python /usr/lib/python2.6/site-packages/rpyc/servers/classic_server.py -m 'forking'

@imports(['os'])
def hello_remote(element,):
    return "got%s" % element

ELEMENTS = (0, 1, 2, 3, 4)

result_iterator = NuMap(hello_remote, ELEMENTS, worker_num=0, \
                              worker_remote=[('localhost', 2)])
results = tuple(result_iterator)
print results

