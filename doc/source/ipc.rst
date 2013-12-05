Inter Process Communication
###########################

This chapter deals with the details of inter-process communication (IPC) in 
**PaPy**. By it's design *PaPy* is a rather high-level engine for the execution 
of workflows and the details of inter-process communication are by default
hidden from the user. This does not mean that it is not possible to influence
how processes are communicated i.e. synchronize and exchange data. This allows 
for optimization of workflow execution times, but requires some understanding
of the involved concepts and is done at the cost of generality e.g. knowing that
two processes will execute on a shared memory UNIX system allows to communicate
them via pipes (FIFOs), which will skip some computation and computations. This 
section should be read together with the API documentation for 
``papy.util.func.dump_item`` and ``papy.util.func.load_item``. By default in 
**PaPy** interprocess communication happens between ``Pipers`` and the 
manager process.


When does it happen?
====================

A ``Piper`` object is assigned to an ``NuMap`` instance which uses a 
process/thread pool to parallelize the evaluation. All functions used to create 
a ``Worker`` are evaluated in a single call and no IPC is necessary. Two 
``Pipers`` on the other hand might be executed in different processes and on 
different machines running possibly different operating systems. Therefore by 
default ``Pipers`` are connected by a **pair** of locked ``pipe`` objects via a
manager process, which is the process used to execute/start the workflow.


Why is it inefficient?
======================

Using a manager process to communicate two other process is inefficient as it 
involves two passes of pickling/unpickling and creates a potential bottleneck 
because half of all serialization computation is done by a single intermediate 
process, this obviously will not scale if the number of process to be 
communicated is large. The solutiond are a) bypass the double pipe connection
and connect processes directly, b) use a more efficient serialization protocol,
c) eliminate IPC by collapsing multiple ``Piper`` instances into a single one.
We will focus here on the first option, which involves adding dumping(output) 
and loading(input) worker-functions to the ``Workers`` instances i.e. in 
pseud-code::

    from papy import workers
    upstream = Worker((func, workers.io.pickle_dumps, workers.io.dump_item),\
                   ((),    (),                      ('tcp')))
    downstream = Worker((workers.io.load_item, workers.io.pickle_loads, func),\
                    ((),                    (),                      ()))
    up_piper = Piper(upstream, parallel =some_IMap_instance)
    down_piper = Piper(downstream, parallel =some_different_IMap_instance)
    pipes = Plumber()
    pipes.add_pipe((up_piper, down_piper))

In this example we created two *Worker* instances, which are used to create
*Piper* instances connected within a *Dagger* instance and executed by
different processes possibly on different physical machines. Because of this a
networked method of communication has been chosen 'tcp'. This method involves
sending data over a network socket. Data has to be serialized before it can be
passed to another process. Within ``multiprocessing`` this is done via pickling,
*RPyC* uses an internal protocol called brine. *PaPy* has built-in workers
which support the pickle, json and marshall protocols. Pickle is the most
general protocols and most Python objects can be pickled. Currently Json and
marshall might be faster, but they have limitations (compability between Python
versions and the range of serializable Python objects). The ``load_item`` worker
will auto discover the type of communication. The currently supported methods of
communication are 'tcp' or 'udp' for network communications, 'fifo' or 'shm' to
communicate processes on the same physical machine and hooks to databases
currently 'sqlite' and 'mysql'. Data can also be exchanged via temporary files
'file'. Temporary files can be used to communicate remote hosts if they are
accessible from both e.g. via NFS or Samba.


How does it work?
-----------------
If the user decides to use custom communication methods the inefficient
double-pipe connection is used to transfer only a very small amount of data and
effectively to synchronize the processes. For example for TCP based
communication it is the hostname and port and the type of the protocol 'tcp'.
For file, shm and FIFO based communication it is just the file name. This amount
of data will not be a bottleneck for pipelines of any size.


How do database "hooks" work?
-----------------------------

The *PaPy* ``dump_db_item`` and ``load_db_item`` worker functions allow to 
communicate Python processes via a database. The data in the database can be 
stored only until it is retrieved or persistantly and serve as a way to
check-point the pipeline. How the data is stored in a database depends on the
type of the database, currently 'mysql' and 'sqlite' databases are supported.
Sqlite database files should not be shared over NFS, but can be written and read 
by different processes.::

  from papy import workers
  upstream = Worker((func, workers.io.pickle_dumps, workers.io.dump_db_item),\
                   ((),    (),                      ('sqlite')))
  downstram = Worker((workers.io.load_db_item, workers.io.pickle_loads, func),\
                    ((),                    (),                         ()))
  up_piper = Piper(upstream, parallel =some_IMap_instance)
  down_piper = Piper(downstram, parallel =some_different_IMap_instance)
  pipes = Plumber()
  pipes.add_pipe((up_piper, down_piper))


Which method should I choose?
-----------------------------

The recommended method of communication depends whether the processes run on the
same or different physical machines, what the operating systems of the Python
processes and the size of the exchanged data. Small lists, objects, strings,
etc. (the size of up to hundreds of kB after serialization) should be
transferred using the default method i.e. without using the dump_item worker.
Only for large objects non-standard communication methods should be considered.
If the processes run on a shared memory UNIX system you should use FIFOs (pipes)
or shared memory (this requires the posix_ipx module). Using FIFOs on Windows
systems is currently not supported (but might be in future) and Windows is not
POSIX compliant so you are left with files, which might be fast enough for
typical applications. Files on a network share are also the recommended method
to communicate Windows-based processes. Networking i.e. 'tcp' and 'udp' requires
forking of the process, which evaluates the ``dump_item`` function.  Forking is
not supported on Windows, but should work well on all UNIX systems. UDP should
provide higher performance than TCP, but should only be used on reliable,
collision-less networks. When using UDP you are not guaranteed that all data
will be transmitted over the network, this will yield WorkerErrors, which in
turn will require you to re-run the pipeline for failed input items.
