#!/usr/bin/env python
# -*- coding: utf-8 -*-
from papy.core import Worker

def l33t(inbox):
    word = inbox[0]
    return word.replace('e', '3')

l33t_worker = Worker(l33t)

for word in ['hello', 'world']:
    print l33t_worker([word])




