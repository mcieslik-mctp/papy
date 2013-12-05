Dictionary of terms and definitions
###################################

A dictionary of terms used within the documentation.

map
---

Higher-order map function. A function which evaluates another function on all
elements of the input collection.

imap
----

Iterated higher-order map function. A function which evaluates another function
on all elements of the input collection returning and evaluating the restuls 
iteratively and lazily. **PaPy** depends on the ``imap`` implementation provided
by  the standard Python imap from ``itertools.imap`` and the advanced ``NuMap``.

NuMap
-----

A parallel implementation of a multi-task imap function, which is used within 
**PaPy**. It uses a pool of worker-threads or worker-processes and evaluates 
functions in parallel either locally or remotely.

Worker function
---------------

A function with a standarized input written to be used by a ``Worker`` class
instance. All processing of a **PaPy** pipeline has to be coded as ``Worker`` 
functions.

worker process / thread
-----------------------

A thread or process inside an ``NuMap`` instance evaluating a tasklet remotely 
or locally.


Worker
------

An object-oriented wrapper for worker functions, it is rougly equivalent to a 
"function with partially applied arguments".


Piper
-----

An object oriented wrapper for ``Worker`` instances, corresponds to "worker with
defined mode of evaluation".

Dagger
------

An directed acyclic graph (DAG) to store and connect ``Piper`` instances.

Plumber
-------

A wrapper for the ``Dagger`` designed to run and interact with a running 
pipeline.

input stream
------------

The input stream is the data that enters a **PaPy** pipeline. The data is
assumed to be a collection of items expressed as a Python iterator (or any 
object which has the next method). 

Any sequence (e.g. a ``list`` or a ``tuple``) can be made into an iterator using
the Python built-in ``iter`` function e.g::

   sample_sequence = [data_point1, data_point2, data_point3]
   sample_iterator = iter(sample_sequence)

Files are by default line-iterators i.e.::

   sample_file = open('sample_file.txt')
   sample_file.next() # returns the first line
   sample_file.next() # returns the second line

output stream
-------------

Input item saved (to disk) by an output ``Piper``. By default the output
``Piper`` should return a ``None`` for every input item, but save the result
persistently (somehow/somewhere).

item
----

A single element of the data stream.


input Piper
-----------

A ``Piper``, which is connected to a input stream (or multiple input streams) is
a input ``Piper``. Such a ``Piper`` corresponds to a node in the graph which has
no  upstream nodes within the **PaPy** workflow or in other words has no 
outgoing edges in the directed acyclic graph. An input ``Piper`` is an input 
node in the graph representing the workflow.

output Piper
------------

A ``Piper``, which generates the output stream is an output ``Piper``. A 
**PaPy** workflow might have multiple output *Pipers* in different places of the
pipeline. An output ``Piper`` corresponds to a node in the graph which has no
downstream nodes within the pipeline or in other words has no incoming edges in
the directed acyclic graph. An output ``Piper`` is an output node in the
graph representing the pipeline.

lazy evaluation
---------------

Is the technique of delaying a computation until the result is required.

task
----

A task is an ordered ``tuple`` of objects added to the ``NuMap`` instance it 
consists of:

  - a function, which will be evaulated on the input element-wise
  - an input (a ``list``, ``tuple`` or any iterator object like an ``array``)
  - a ``tuple`` of arguments e.g. ``(arg1, arg2, arg3)``
  - a ``dict`` of keyword arguments i.e. ``{'arg1': value_1, 'arg2': value_2}``

The optional arguments and keyworded arguments have to match the signature of 
the function. The task is iteratively split into evaluated calls in the 
following way::

  (func, element_from_iterable, arguments, keyworded_arguments)
  result = func(element_from_iterable, arguments, keyworded_arguments)

inbox
-----

The first argument of any ``Worker`` function. The elements of the function
correspond to the outputs of the upstream function in the ``Worker`` instance or
to outputs of other ``Pipers``. These outputs are defined by the pipeline
topology. The contents of the inbox depend on a specific input item to the
pipeline. All other arguments of a worker function are predetermined.

