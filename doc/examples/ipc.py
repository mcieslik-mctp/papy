#!/usr/bin/env python
# -*- coding: utf-8 -*-
from papy.util.func import dump_item, load_item
from numap import NuMap, imports
from papy.core import Piper, Worker

@imports(['os'])
def upstream(inbox):
    up_pid = os.getpid()
    return str(up_pid)

@imports(['os'])
def downstream(inbox):
    up_pid = inbox[0]
    down_pid = os.getpid()
    return "%s->%s" % (up_pid, down_pid)

host1 = NuMap()
host2 = NuMap()

up = Worker((upstream, dump_item))
dn = Worker((load_item, downstream))
up_ = Piper(up, parallel=host1)
dn_ = Piper(dn, parallel=host2)

up_([['hello', 'world', 'hi', 'folks']])
dn_([up_])

up_.start()
dn_.start()

print list(dn_)
