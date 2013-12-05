#!/usr/bin/env python
# -*- coding: utf-8 -*-
from papy.core import Piper

def l33t(inbox):
    word = inbox[0]
    return (word.replace('e', '3').replace('o', '0'),) * 2

l33t_piper = Piper(l33t, produce=2)





l33t_piper([['hello', 'world']])
l33t_piper.start()

print list(l33t_piper)


