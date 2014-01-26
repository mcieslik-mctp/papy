#!/usr/bin/env python
# -*- coding: utf-8 -*-
from numap import NuMap
from papy.core import Dagger, Piper
from papy.util.func import ipasser

def merge(inbox):
    word1 = inbox[0]
    word2 = inbox[1]
    return word1 + word2

# function nodes
merge_p = Piper(merge)

inp1_p = Piper(ipasser)
inp2_p = Piper(ipasser)

# topology
pipeline = Dagger()
pipeline.add_pipe((inp2_p, merge_p), branch="2")
pipeline.add_pipe((inp1_p, merge_p), branch="1")

end = pipeline.get_outputs()[0]
# # runtime
pipeline.connect([['hello ', 'world '], ["world", "hello"]])
pipeline.start()
print list(end)


