#!/usr/bin/env python
# -*- coding: utf-8 -*-
from papy.core import Piper, Dagger

def l33t(inbox):
    word = inbox[0]
    return word.replace('e', '3').replace('o', '0')

def join(inbox):
    left_word, right_word = inbox
    return left_word + " " + right_word

left_l33t = Piper(l33t, branch=1)
right_l33t = Piper(l33t, branch=2)
join_l33t = Piper(join)

pipeline = Dagger()
pipeline.add_pipe((left_l33t, join_l33t))
pipeline.add_pipe((right_l33t, join_l33t))
end = pipeline.get_outputs()[0]

pipeline.connect([
                  ['hello', 'hi'],
                  ['world', 'folks']
                  ])
pipeline.start()
print list(end)


