Optimization
############

The throughput of a pipeline will be most significantly limited by the slowest 
``Piper``. A processing node might be slow either because it does a 
CPU-intensive or IO-intensive task, because it waits for some data, or because
it synchronizes with other nodes and waits. 

Identifying bottlenecks
-----------------------

As a general rule you should optimize the bottleneck(s) only. Therefore it is
critical to understand where and what the bottleneck is.

This has good reason as most of your nodes will not limit the throughput of the
workflow while parallelization is quite expensive. If your pipeline has no 
obvious bottleneck it's probably fast enough. If not you might be able to 
use a shared pool.

Understanding bottlenecks
-------------------------

To Be Written.

Addressing synchronization
==========================

Unordered Pipers
----------------

Unordered pipers return results in an arbitrary order e.g for the input sequence
``[3,2,1]`` a parallel unordered ``Piper`` instance with a function that doubles
the input might return ``[6,2,4]`` or any other permutation of the doubled 
numbers. Unordered nodes do not compute faster they only make the results 
available sooner. Thus a down-stream computation that uses the same 
computational resource can start earlier and potentially utilize it to a fuller
extent. You should consider unordered ``Pipers`` if the computation time for
data items varies significantly.


Addressing serialization
========================

To Be Written.


Distributing Computational resources
====================================

As a general rule of you most likely should not use a shared ``NuMap`` instance
among all ``Pipers`` within a workflow.

If the throughput of your pipeline is limited by a cpu-intensive tasks you 
should parallelize this node. **PaPy** allows to parallelize cpu-bound 
``Pipers``.  The amount of cpu-power should be proportional to the computational
requirements of a processing task. The number of recommended ``NuMap`` pool 
worker processes should equal or  slightly larger than the number of physical 
CPU-cores on each local or remote computer.
