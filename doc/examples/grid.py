#!/usr/bin/env python
# -*- coding: utf-8 -*-
#python /usr/lib/python2.6/site-packages/rpyc/servers/classic_server.py -m 'forking'

from numap import NuMap, imports
from papy.core import Piper

@imports(['os'])
def hello_from(inbox):
    word = inbox[0]
    up_pid = os.getpid()
    return (word, up_pid)

somehost = NuMap(worker_num=0, worker_remote=[('localhost', 2)])

remote_piper = Piper(hello_from, parallel=somehost)
remote_piper([['hello', 'world', 'hi', 'folks']])
remote_piper.start()

print list(remote_piper)
