Introduction
############

Many computational tasks require sequential processing of data i.e. the global
data set is split into items which are processed separately by multiple chained
processing nodes. This is generally called a dataflow. **PaPy** adapts 
flow-based programming paradigms and allows to create a data-processing pipeline
which allows for both data and task parallelism. In the process of design we 
tried to make the **PaPy** API as idiosyncrasy-free as possible, relying on 
familiar concepts of map functions and directed acyclic graphs. 


Feature summary
===============

This is a list of features of a workflow constructed using the **PaPy** package
and its additional components.

  * construction of arbitrarily complex pipelines (any directed acyclic graph is
    a valid workflow)
  * evaluation is lazy-buffered (allows to process datasets that do not fit into
    memory)
  * flexible local and remote parallelism (local and remote resources can be 
    pooled)
  * shared local and remote resources (resource pools can be flexibly assigned 
    to processing nodes)
  * robustness to exceptions
  * support for time-outs 
  * real-time logging / monitoring
  * os-independent (really a feature of ``multiprocessing``)
  * distributed (really a feature of *RPyC*)
  * small code-base
  * tested & documented.


Description
===========

Workflows are constructed from components of orthogonal functionality:

  * the function wrapping ``Workers``
  * the connection capable ``Pipers``
  * the topology defining ``Dagger``
  * the parallel executors ``NuMaps``

The ``Dagger`` connects ``Pipers`` via pipes into a directed acyclic graph while
the ``NuMaps`` are assigned to ``Pipers`` and evaluate their ``Workers`` either 
locally using threads or processes or on remote hosts. The ``Workers`` allow to 
compose multiple functions while the ``Pipers`` allow to connect the inputs and
outputs of ``Workers`` as defined by the ``Dagger`` topology. Data parallelism
is possible because data items are independent i.e. if it is a collection (or 
can be  split into one) of data-items: files, messages, sequences, arrays. 
**PaPy** enables task  parallelism by allowing the data processing functions to
be evaluated on  different computational resources represented by ``NuMap`` 
instances.

**PaPy** is written in and for Python this means that the user is expected to
write Python functions with defined call/return signatures, but the function 
code is largely arbitrary e.g. they can call a perl script or import a library.
**PaPy** focuses on modularity, functions should be re-used and composed within
pipelines and ``Workers``.

The **PaPy** workflow automatically logs it's execution is resistant to 
exceptions and timeouts and should work on all platforms where 
``multiprocessing`` is available. It also allows to compute over a 
cross-platform ad-hoc grid using the **RPyC** package.


Where/When should **PaPy** be used?
===================================

It is likely that you will benefit from using **PaPy** if some of the following 
is true:

  * you need to process large collections of data items.
  * your data collection is to large to fit into memory.
  * you want to utilize an ad-hoc grid.
  * you have to construct a complex workflow or data-flows.
  * you are likely to deal with timeouts or bogus data.
  * the execution of your workflow needs to be logged / monitored.
  * you want to refactor existing code.
  * you want to reuse (wrap) existing code.


Where/When should **PaPy** **not** be used?
===========================================

  * You do not need a workflow just a method to evaluate a function in parallel
    (consider ``NuMap`` for this).
  * The parallel evaluation will improve performance only if the functions have
    sufficient granularity i.e. a computation to communication ratio.
  * Your input is not a collection and it does not allow for data parallelism.

