#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This is a prototype of a pipeline, use it as a start-point to construct your 
own. The construction of a pipeline is split into parts, this has several
reasons. First it makes your code modular as it detaches the definition of a
workflow from the runtime i.e. the real data and computational resources. It
further allows to group all the elements into a single-file executable script.

All the steps are as explicit as possible. If you prefer the less flexible but 
shorter implicit API features please refer to the documentation.

 xrange 
   |
  fork (linear)
 /    \
L      R (parallel, signle shared resource)
 \    /
  join (linear)
    |
  print         

"""
# Part 0: import the PaPy infrastructure.
# interface of the API: 
from papy import Plumber, Piper, Worker
from papy.util.func import print_
## the parallel NuMap function and imports wrapper: 
from numap import NuMap, imports
## logging support
import logging
from papy.util.config import start_logger
start_logger(log_to_stream=True, log_to_stream_level=logging.ERROR)


# Part 1: Define user functions
def fork(inbox):
    msg = inbox[0]
    return msg

@imports(['socket', 'os', 'threading'])
def who_am_i(inbox):
    """
    This function identifies the host process.
    """
    return "process:%s" % os.getpid()

def join(inbox):
    left_branch, right_branch = inbox
    return "joined result from %s and %s" % (left_branch, right_branch)

# Part 2: Define the topology
def pipeline(resource):
    # initialize Worker instances (i.e. wrap the functions).
    w_fork = Worker(fork)
    w_who_am_i = Worker(who_am_i)
    w_join = Worker(join)
    # initialize Piper instances (i.e. attach functions to runtime)
    p_fork = Piper(w_fork, name='Fork')
    L = Piper(w_who_am_i, parallel=resource, branch=1)
    R = Piper(w_who_am_i, parallel=resource, branch=2)
    p_join = Piper(w_join, name='Join')
    p_print = Piper(print_)
    # create the pipeline and connect pipers
    workflow = Plumber()
    workflow.add_pipe((p_fork, L))
    workflow.add_pipe((p_fork, R))
    workflow.add_pipe((L, p_join))
    workflow.add_pipe((R, p_join))
    workflow.add_pipe((p_join, p_print))
    return workflow

# Part 3: parse the arguments
def options(args):
    size = int(args.get('--size', 10))
    worker_num = int(args.get('--worker_num', 4))
    return (size, worker_num)

# Part 4: define the resources
def resources(args):
    size, worker_num = args
    rsrc = NuMap(worker_num=worker_num)
    return rsrc
#
# Part 5: create the input data
def data(args):
    size, worker_num = args
    return xrange(size)

# Part 6: Execute
if __name__ == '__main__':
    # get command-line arguments using getopt 
    import sys
    from getopt import getopt
    args = dict(getopt(sys.argv[1:], '', ['size=', 'worker_num='])[0])
    # parse options
    opts = options(args)
    # definie/initialize resources
    rsrc = resources(opts)
    # define/create input data
    inpt = data(opts)
    # attach resources to pipeline
    workflow = pipeline(rsrc)
    # connect and start pipeline
    workflow.start([inpt])
    # run and wait until pipeline is finished
    workflow.run()
    workflow.wait()
    workflow.pause()
    workflow.stop()
    print "Runtime: %.2fs" % workflow.stats['run_time']


