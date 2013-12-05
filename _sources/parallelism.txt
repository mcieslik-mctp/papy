About the Parallelism in *PaPy*
===============================

This document explains and gives code snippets how to use the parallel features
of *PaPy*. The first paragraphs introduce the ``map`` and ``imap`` functions and
the *IMap* object.  The types of parallelism, are explained in the later
sections where also typical optimizations, bottlenecks of pipelines are covered. 

Parallel functionality is provided by the *IMap* class. In the most basic mode
of operation it can be called exactly like ``itertools.imap`` or the
``multiprocessing`` *Pool* ``imap`` method. Setting additional options allows to
parallelise the evaluation using *Workers* of threads, processes or remote
processes and share those workers among multiple functions and inputs. A unique
feature of IMap is that it allows to fine-tune the memory-consumption,
parallelism and layziness trade-off of nested function maps.

**The interpreter will hang on exit if a pipeline does not finish or is halted 
abnormaly**
  
The Python interpreter exits (returns) if all spawned threads or forked
processes return. *PaPy* uses multiple threads to manage the pipeline and
evaluates functions in seperate threads or processes. All of them need to be
stopped before the parent python process can return. This is done
automatically whenever a pipeline finishes or some expected exception 
occurs, in all other cases it is required that the user stops the pipeline
manually.


Map basics
----------

A map function applies a function to the items of a sequence. This is in
Python expressed with the following syntax::

    def power(x):
        return x*x

    inp_list = [1,2,3,4]
    out_list = map(power, inp_list)

The resulting output is [1, 4, 9, 16]. Though unmatched in simplicity this
function has several computational drawbacks.

  #. results are evaluated sequentially i.e. power(1), power(2), power(3), power(4)
     and on a single processor.

  #. The function returns only after *all* results have been calculated.

  #. The list of results has to fit into memory together with the input.

  #. The results are always returned in order.

The last two issues can be addressed by using the imap (iterated map) function.
Its usage is almost as simple::

    from itertools import imap
    inp_list = [1,2,3,4]
    out_iterator = imap(power, inp_list)
    first_result = out_iterator.next()  # returns power(1)
    second_result = out_iterator.next() # returns power(2)

There are however a number of differences between the two. The imap function
returns a result object immediately, but this object is only a link (via the
out_iterator.next method) to the next result to be calculated. The evaluation
starts as soon as the next method is called (layzy evaluation) and the result is
returned as soon as it is calculated. The function, argument tuples are still 
evaluated sequentially and on a single processor, but they are returned as soon
as they are needed and only a single result needs to fit into memory.

Recent versions of Python (2.6+) provide implementations of a parallel map where
results are evaluated by a pool of worker processes. This functionality comes in
three flavours::
    
    from multiprocessing import Pool 
    pool = Pool() 
    out_list = pool.map(power, inp_list)
    out_iterator1 = pool.imap(power, inp_list)
    out_iterator1.next(timeout =1) 
    out_iterator2 = pool.imap_unordered(power, inp_list)

.. warning::

  This example like most of the following is a code snippet. It is or should be
  syntactically correct. But might outside of a valid script file.

The Pool.map does exactly the same as the simple map function, but it uses by
default all availble cores. This addresses the second drawback. Both imap
methods return a result object with the next method like itertools.imap. Calling
it returns the next calculated (imap_unordered) or expected result (imap). The
next method has an optional timeout argument i.e. if no result is availble
within the limit a TimeoutError is raised. Although it might seem that those
implementations have none of the above mentioned drawbacks several implementation 
choices make them inappropriate for constructing pipelines.


A pipeline is a nested imap
---------------------------

Map functions have a list as input and return a list so they can be nested
i.e.::

    from math import degrees, radians
    map(degrees, map(radians, [1,2,3]))
    [1.0, 2.0, 3.0]

In this example the function radians is first applied to the elements of the
input list then the the results are back-converted to degrees. The order of
evaluation:

    temp_list = []
    temp_list.append(radians(1))
    temp_list.append(radians(2))
    temp_list.append(radians(3))
    result_list = []
    result_list.append(degrees(temp_list[0]))
    result_list.append(degrees(temp_list[1]))
    result_list.append(degrees(temp_list[2]))
    << return list of results >>

If we use an iterated (layzy) version of the map function i.e.::

    from itertools import imap
    imap(degrees, imap(radians, [1,2,3]))
    [1.0, 2.0, 3.0]

The order of calculation changes:

   temp_result = radians(1)
   result = degree(temp_result)
   <<return result>>
   temp_result = radians(2)
   result = degree(temp_result)
   <<return result>>
   temp_result = radians(3)
   result = degree(temp_result)
   <<return result>>

Note that only one temporary result needs to be stored at any given time and
that the first result is returned after only two calculations. But multiprocessing 
Pools imap function yield yet another calculation order.::

    from multiprocessing import Pool
    pool = Pool()
    pool.imap(degrees, pool.imap(radians, [1,2,3]))
    [1.0, 2.0, 3.0]

What happens is a little bit unexpected, first the function is evaluated (by
multiple processes) and only after all temporary results are calculated the
second functions is iteratively applied::

    temp_list = []
    temp_list.append(radians(1))
    temp_list.append(radians(2))
    temp_list.append(radians(3))
    result = degree(temp_result[0])
    <<return result>>
    result = degree(temp_result[1])
    <<return result>>
    result = degree(temp_result[2])
    <<return result>>

The results are either returned imediately or stored in a result list. The
maximum size of this list is the size of temporary list and the size of the
input.  The reason for this behaviour is the order by which tasks i.e.
(function, data) tuples are submitted to the pool. If one pool handles two
functions first all (radians, x) tuples are submited and then all (degrees, x).
The outer function is evaluated last so for multiple and computationally
expensive functions the first result might be availble after a long lag phase
followed by a burst of results.

This problem can be solved by having a
seperate pool for each function.::

    from multiprocessing import Pool
    pool1 = Pool()
    pool2 = Pool()
    pool2.imap(degrees, pool1.imap(radians, [1,2,3]))
    [1.0, 2.0, 3.0]

Now the execution order is not defined anymore as processes in the two Pools
compete for CPU time from the OS. A possible evaluation order might be like
this::

    temp_list = []
    temp_list.append(radians(1))
    temp_list.append(radians(2))
    result = degree(temp_result[0])
    <<return result>>
    result = degree(temp_result[1])
    <<return result>>
    temp_list.append(radians(3))
    result = degree(temp_result[2])
    <<return result>>

As you can see a temporary result list is still built. Its maximum lenght is
not predictable and limited by the lenght of the input. Another drawback is that
if the number of functions is big the number of process-pool workers
significantly exceeds the number of availble CPUs or CPU-cores, which is
inefficient.


The task and the tasklet
------------------------

The imap implementation in *PaPy* (*IMap*) is different as it allows to control the
order by which function and data tuples are submitted to the worker pool. It
introduces the concept of a task which is a function, input and arguments tuple.
The input is a python iterator i.e. an object which has a next method it
obviously should return data to be calculated next. The argument is a tuple of
parameters which is given to the function for example::

    (function, data_iterator, ('rome', 1.17, some_object))   # a task 

The IMap role is to evaluate tasks. To evaluate a task means to evaluate all 
tasklets::

    (function, data_iterator.next(), 'rome', 1.17, someobject) # a tasklet
    result = function(data_iterator.next(), 'rome', 1.17, someobject) # evaluation

Until the data_iterator is empty.


IMaps parallelism is defined by a stride
----------------------------------------

IMap allows to control the order in which tasklets are evaluated. This is
accomplished by the stride parameter. A stride is the number of tasklets from 
one task submitted before any tasklet from the next task. The default stride is
equal to the number of pool workers and should not be smaller.::

    from IMap import IMap
    from math import radians, degrees
    Imap = IMap(worker_num =2)
    output = Imap.add_task(radians, [1,2,3,4])
    result = Imap.add_task(degrees, output)
    Imap.start() # finished adding tasks
    result.next() # or Imap.next(task =1) 0 is the first task

In this example the Imap instance has a pool with two workers (by default those
workers are separate processes), its default stride is therefore 2. The order in
which the tasks will be evaluated is as follows.::

    temp_list = []
    result_list = []
    temp_list.append(radians(1))
    temp_list.append(radians(2))
    result_list.append(degree(temp_list[0]))
    <<return result>>
    result_list.append(degree(temp_list[1]))
    <<return result>>
    temp_list = []
    result_list = []
    temp_list.append(radians(3))
    temp_list.append(radians(4))
    result_list.append(degree(temp_list[2]))
    <<return result>>
    result_list.append(degree(temp_list[3]))
    <<return result>>

The temp_list (in fact it is a queue) has a defined size limit (stride) and so
is the result_list. The details of memory consumption will be explained in the
next paragraphs here it suffices to say that a minimum memory requirement of 2
temporary results can ben enfored on this pipeline without loss of efficiency.
If the pipeline was longer and had computationally expensive functions it would
be noticable that the results from the outer function arrive in burst of 2 or
bursts of stirde size.


IMap needs tasks in the right order
-----------------------------------

In the previous example two nested tasks have been added to the IMap function
using the add_task method. The general way of working with IMap is as follows::

    #0. import IMap
    from IMap import IMap
    #1. define imap keyworded parameters e.g.
    imap_instance = IMap(worker_remote =[['host', 2]])
    #2. add tasks
    out0 = imap_instance.add_task(function0, input_data)
    out1 = imap_instance.add_task(function1, out0)
    out2 = imap_instance.add_task(function2, other_data)
    #3. start the evaluation
    imap_instance.start()
    #4. 
    << get the results >>

In this section we will focus on step 2. We have submitted 3 tasks to the imap
instance. Because the order of submission matters they will be evaluated in the
order.::

    # first stride 
    function0(input_data[0 .. n]) # where n is stride 
    function1(out1[0 .. n])
    function2(other_data[0 .. n])
    
    # second stride
    function0(input_data[n .. n+n]) # where n is stride 
    function1(out1[n .. n+n])
    function2(other_data[n .. n+n])1

    # and so on

Because function1 depends on the results from function0 it can't be added as a
task before function0. It might seem impossible because function1 takes the
output of function 0 (out0) as an argument, but in general the input could be an
object created before out0, which is modified with out0 after creation. This is
possible because evaluation starts only after the start method is called. 


IMap can limit the memory consumption.
--------------------------------------

By default the maximum memory consumption of an IMap instance is equal to the
number of tasks times the stide size, but this limit can be changed. Consider an 
imap instance with two tasks, which are not nested and a stride of 3, this means
that the default maximum memory consumption is 6. The evaluation will pause
whenever the IMap instance reaches the limit. In pseudo-python:

    list0 = []
    list1 = []
    # a stride of 3
    list0.append(function0(arg0)) 
    list0.append(function0(arg1))
    list0.append(function0(arg2)) # list0 has size 3
    list1.append(function1(arg0)) 
    list1.append(function1(arg1))
    list1.append(function1(arg2)) # list1 has size 3

Because at this moment the two list have together a lenght of 6 no further
evaluations takes place. The only way to clear a list is to get results from the
output iterators (say out0 for function0 and out1 for function1). If we take a
single result say::    

    result_0_0 = out0.next() # submits function0(arg3) to the pool
    
memory consumption lowers to 5 and the next task is submitted to the pool.::

    list0.append(function0(arg3))

memory consumption is once again at 6 and the next task (function0, arg4) waits.
By retrieving results from the output iterators we free the temporary result
lists (queues) and allow evaluation to proceed. Results do not have to be retrieved
in the order the tasks have been submitted to the pool or the order in which the
results have been calculated. Assume that in the last example the next method of
out0.next() has been called 5 more times::

    result_0_1 = out0.next() # submits function0(arg4) to the pool
    result_0_2 = out0.next() # submits function0(arg5) to the pool
    result_0_3 = out0.next() # submits function1(arg3) to the pool !note 1
    result_0_4 = out0.next() # submits function1(arg4) to the pool !note 1
    result_0_5 = out0.next() # submits function1(arg5) to the pool !note 1

after all workers finish list0 the imap reaches a stage where:
    
    * list0 - will be empty
    * list1 - will have 6 results (arg0 - arg5)
    * task (function0, arg6) will wait to be submitted

List0 is empty and the next task (function0, arg6) cannot be submitted because the
total memory consumption is 6. If we would call out0.next the result would
never arrive and the python interpreter would be blocked. A timeout argument can
be supplied it causes the next method to raise a TimeoutError if after the
specified number of seconds no result is availble.::

     result_0_6 = out0.next(timeout =2) # raise after 2 seconds
     Traceback (most recent call last):
     File "<stdin>", line 1, in <module>
     multiprocessing.TimeoutError

We have to empty the other functions output (out1) to get the 7th result for
out0. Note that the order of task submissions is defined at start by how the memory 
is freed (the order in which out0.next nad out1.next are called) does not change this
order::

     result_1_0 = out1.next() # submits function0(arg6) to the pool
     result_0_6 = out0.next() # submits function0(arg7) to the pool

To never run into an block only it is necessary to:

  * retrieve at most stride number of results from any output in a sequence
  * retrieve the result n from outputN before the result n from outputN+1

The most memory efficient way to do this is to get the results in batches of
stride size, which is equivalent to the order the tasks have been submitted to
the worker pool. If the two functions from the above example were nested this
would happen automatically for the inner function. If the the pipeline run in
the most memory efficient way the memory consumption can be lowered to the
stride of the IMap this is done using the buffer argument. For example::

    Imap = IMap(stride =3, buffer=5)



Parallel: local vs. remote and threads vs. processes
----------------------------------------------------

IMap supports parallelization using local threads (worker_type ='thread') and 
processes (worker_type='process'). Remote threads and processes are run by local
processes. IMap is designed to allow the user to choose the type of parallelization 
using the worker_type argument and/or by specifying the remote processes using the
worker_remote argument.

Because of the global interpreter lock (GIL) in the standard cPython
implementation of the Python programming language, only one os-thread can
execute interpreted python code. Multiple running threads are assigned
timeslices of "code access". In general it is not possible to speed cpu-bound
computations using threads. Multiple threads can however speed up certain
functions to a certain degree if either the function is:

  * IO-bound (e.g. waits for server responses)
  * uses libraries which release the global interpreter lock

The first case applies to all function which depend on user interaction and
other blocking Input/Output operations like reading or writing a file. the
second case applies mostly to external compiled libraries doing cpu-intensive
calculations. Those libraries can nativly be multithreaded. An IMap running
tasks in worker threads is therfore a good choice if the tasks submitted to
the function are IO-bound or if it uses a library which releases the GIL for
significant periods of time.

The GIL can be circumvented by forking the python interpreter process instead of
spawning additional threads within. Such forked processes have seperate memory
space and are seen by the operating system as another python interpreter.
Multiple processes have each their own GIL and are therefore suited to
parallelize intepreted python code. This parallelization will make the
computation faster if the operating system has enough resources to support the
processes. If a function is CPU-bound the most limiting resource is the CPU and
therefore the number of processes should not exceed the number of availble
CPUs. Using multiple processes within on parent process is called
multiprocessing and python 2.6 and above support this feature via the
multiprocessing module out of the box on most operating systems (Linux, MacOSX,
Windows). There are however implementation differences among UNIX systems and
Windows which make multiprocessing on Windows less efficient. This module is
also back-ported to python 2.5 as external module. It is also possible to 
parallelize local computations using the RPyC library. For cpu-bound tasks the
additional overhead is minor.

If a single machine is not fast enough for the task, distributed computing i.e.
computation on remote physical computers might be considered. IMap supports
distributed computation using the RPyC library. To use this feature a "classic"
RPyC server has to be running on a remote host. This server allows clients (i.e.
IMap instances) to connect to them and execute functions. An RPyC server can be
both thread or process based. Only a process-based RPyC will be able to use
multiple CPUs on the remote computer (not supported on Windows). An IMap which
connects to remote servers spawns a new process for each thread/process
spawned/forked remotely (by the RPyC server).

.. note::

  IMap has no possiblity to change the thread/process nature of the remote
  server.

In the following example snippet::

    Imap = IMap(worker_type ='process', worker_num =2, worker_remote =[['host1',1], ['host2', 4]])

An IMap instance is created which uses worker processes. It has a total of 7
local worker processes. Five of the local worker processes do the computation 
on remote hosts: 4 on host2 nad 1 on host1.

The following is illegal, because remote threads/processes can be managed only
by 'process' workers::

    Imap = IMap(worker_type ='process', worker_num =2, worker_remote =[['host1',1], ['host2', 4]])

The RPyC server can also be started on the local machine ('localhost'). In this
example the IMap instance has a total of 2 local worker processes which manage
two remote processes which happen to exist on the same physical machine.::

    Imap = IMap(worker_type ='process', worker_num =0, worker_remote =[[localhost', 2]])


If the order of the results is not important
--------------------------------------------

IMap supports unordered results via the unordered argument::

    Imap = IMap(ordered =True)  # the default is ordered
    Imap = IMap(ordered =False) # random order of results

If the results are allowed to be unordered the evaluation might be significantly
faster under certain circumstances described later, but this order is not
reproducible. As a general advice do not use unordered Imap instances in
branched papy pipelines, unless you really know what you are doing.


Timeouts and skipping
---------------------

IMap supports timeouts. A TimeoutError (from the multiprocessing module) is
raised whenever a result for a given task cannot be returned within
approximately the number of seconds specified. The timeout is only approximate
because IMap uses multiple threads to manage the input and output queues for the
worker threads/process. The thread, which receives result might not not have
access to the interpreter when the timeout passes.

The skipping argument allows to skip results which did timeout. If skipping is
not specified IMap will try to return the same result (for this task) once more.
If the timeout is not specified skipping is ignored. If IMap is used with nested
tasks a timeout should in practice not be specified unless IMap is used within
from a piper object or the IMap instance



and timeouts are specified the skipping argument should be true, the
reason for this is that 



The parallel stride revisited
-----------------------------

In one of the previous sections it has been described how IMap allows for
parallelism by introducing the concept of the stride. To recapitulate a stride
is the number of tasklets submitted to the pool for a specific task to be
executed/evaluated in parallel. Another tasklet can be submitted if the buffer
is larger than the stride or as soon as any of the results of the parallel
evaluations is retrieved. The user can add multiple tasks to one IMap instance.
If the evaluation is CPU-bound and the IMap uses worker processes an optimal
speed up equal to the number of CPUs or CPU-cores. However because of inter task
dependencies i.e. nested functions, the speed-up might be smaller as a result of
the memory trade-off and task submission order. 

  #. *The output needs to be retrieved*. If the output of an IMap instance is not
  retrieved it will pause whenever it fill the buffer of temporary results,
  therefore it is important to retrieve the results as soon as they are ready.
  Because results generaly arrive in batches of stride size it is best to try to
  retrieve stride number of results from each task. If the IMap is used within a
  papy pipeline the Plumber has a seperate thread (started using the plunge
  method) dedicated to keep the IMap evaluating.

  #. *Variable tasklet calculation times* 








If the next task depends on the results from the
first task then it's first tasklet 

































