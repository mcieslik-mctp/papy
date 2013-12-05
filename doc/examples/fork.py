#!/usr/bin/env python
# -*- coding: utf-8 -*-
from papy.core import Piper, Dagger

def upper(inbox):
    word = inbox[0]
    return word.upper()

def E_to_3(inbox):
    word = inbox[0]
    return word.replace('E', '3')

def O_to_0(inbox):
    word = inbox[0]
    return word.replace('O', '0')

upper_fork = Piper(upper)
E_end = Piper(E_to_3, branch=1)
O_end = Piper(O_to_0, branch=2)

pipeline = Dagger()
pipeline.add_pipe((upper_fork, E_end))
pipeline.add_pipe((upper_fork, O_end))

left_end, right_end = pipeline.get_outputs()

pipeline.connect([['hello', 'world']])
pipeline.start()
print zip(left_end, right_end)



