#!/usr/bin/env python
# -*- coding: utf-8 -*-
from numap import NuMap
from papy.core import Piper

def l33t(inbox):
    word = inbox[0]
    return word.replace('e', '3').replace('o', '0')

numap = NuMap()

l33t_piper = Piper(l33t, parallel=numap)
l33t_piper([['hello', 'world']])
l33t_piper.start()

print list(l33t_piper)


