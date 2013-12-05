#!/usr/bin/env python
# -*- coding: utf-8 -*-
from numap import NuMap
from papy.core import Plumber, Piper

def l33t(inbox):
    word = inbox[0]
    return word.replace('e', '3').replace('o', '0')

def l33ter(inbox):
    word = inbox[0]
    return word.replace('l', '1')

# execution endgine
numap = NuMap()

# function nodes
l33t_piper = Piper(l33t, parallel=numap)
l33ter_piper = Piper(l33ter, parallel=numap, track=True)

# topology
pipeline = Plumber()
pipeline.add_pipe((l33t_piper, l33ter_piper))
end = pipeline.get_outputs()[0]

# runtime
pipeline.start([['hello', 'world']])
pipeline.run()
pipeline.wait()
print pipeline.stats['pipers_tracked'][end]
assert [{0: 'h3110', 1: 'w0r1d'}] == pipeline.stats['pipers_tracked'][end]


