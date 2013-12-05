#!/usr/bin/env python
# -*- coding: utf-8 -*-
from papy.core import Piper

def l33t(inbox):
    word = inbox[0]
    leet_yuk = (word.replace('e', '3').replace('o', '0'), 'yuk')
    print "I'll produce: %s and %s" % leet_yuk
    return leet_yuk

def noyuk(inbox):
    print "I got 2 words and 2 yuks: %s" % inbox
    word_box1, yuk_box1, word_box2, yuk_box2 = inbox
    return word_box1[0] + ' ' + word_box2[0]


l33t_piper = Piper(l33t, produce=2)
noyuk_piper = Piper(noyuk, consume=4)

l33t_piper([['hello', 'world', 'hi', 'folks']])
noyuk_piper([l33t_piper])

l33t_piper.start()
noyuk_piper.start()
print list(noyuk_piper)


