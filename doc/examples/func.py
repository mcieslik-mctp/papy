#!/usr/bin/env python
# -*- coding: utf-8 -*-
from numap import imports

@imports(['os'])
def hello_you(inbox):
    name = inbox[0]
    print "hello %s I am %s" % (name, os.getpid())

hello_you(['world'])
