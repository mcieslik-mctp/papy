#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Step 0 (importing the library)
from numap import NuMap, imports
from papy.core import Worker, Piper, Plumber
from nubio import AaSeq, NtSeq

# Step 1 (writing a worker function)
# clean_Seq takes a raw sequence, creates an array object of amino- or 
# nucleic- acid and replaces certain characters. It can be used as follows
# >>> arr = clean_seq(['AGA.TA'], type='aa', fixes=[('.', '-')])
# >>> print arr
def clean_seq(inbox, type, fixes):
    seq = inbox[0]
    if type == 'aa':
        arr = AaSeq(seq)
    elif type == 'nt':
        arr = NtSeq(seq)
    else:
        raise ValueError("unknow sequence type %s" % type)
    for bad, good in fixes:
        arr.str('replace', bad, good)
    return arr

# timestamp take an array object and annotates it with the current data.
# the function depends on the ``time`` module, attached to the function 
# definition via the ``imports`` decorator. It is simply called:
# >>> arr = timesamp([arr])
# >>> print arr.meta['timestamp']
@imports(['time'])
def timestamp(inbox):
    arr = inbox[0]
    arr.meta['timestamp'] = "%s_%s_%s@%s:%s:%s" % time.localtime()[0:6]
    return arr

# Step 2 (wrapping function into workers)
# wraps clean_seq and defines a specific sequence type and fixes
cleaner = Worker(clean_seq, kwargs={'type':'aa', 'fixes':[('.', '-')]})
# >>> arr = cleaner(['AGA.TA'])
# wraps timestamp
stamper = Worker(timestamp)
# >>> arr = stamper([arr])

# Step 3 (representing computational resources)
# creates a resource that allows to utilize all local processors
local_computer = NuMap()

# Step 4 (creating processing nodes)
# this attaches a single computational resource to the two processing nodes
# the stamper_node will be tracked i.e. it will store the results of computation
# in memory.
cleaner_node = Piper(cleaner, parallel=local_computer)
stamper_node = Piper(stamper, parallel=local_computer, track=True)

# Step 5 (constructing a workflow graph)
# we construct a workflow graph add the two processing nodes and define the
# connection between them.
workflow = Plumber()
workflow.add_pipe((cleaner_node, stamper_node))

# Step 6 (execute the workflow)
# this starts the workflow, processes data in the "background" and waits
# until all data-items have been processed.
workflow.start([['AGA.TA', 'TG..AA']])
workflow.run()
workflow.wait()
results = workflow.stats['pipers_tracked'][stamper_node][0]
for seq in results.values():
    print "Object \"%s\" has time stamp: %s " % (seq, seq.meta['timestamp'])


