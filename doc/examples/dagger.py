#!/usr/bin/env python
# -*- coding: utf-8 -*-
from numap import NuMap
from papy.core import Dagger, Piper

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
l33ter_piper = Piper(l33ter, parallel=numap)

# topology
pipeline = Dagger()
pipeline.add_pipe((l33t_piper, l33ter_piper))
end = pipeline.get_outputs()[0]

# runtime
pipeline.connect([['hello', 'world']])
pipeline.start()
print list(end)


