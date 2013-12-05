#!/usr/bin/env python
# -*- coding: utf-8 -*-
from papy.core import Piper

def nimm2(inbox):
    left_box, right_box = inbox
    left_word, right_word = left_box[0], right_box[0]
    return left_word + ' ' + right_word

l33t_piper = Piper(nimm2, consume=2)
l33t_piper([['hello', 'world', 'hi', 'folks']]) # length of 4
l33t_piper.start()

out = list(l33t_piper)
assert out == ['hello world', 'hi folks'] # length of 2
print out


