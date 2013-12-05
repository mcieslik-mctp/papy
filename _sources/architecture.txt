Architecture
============

The architecture of **PaPy** is remarkably simple and intuitive yet flexible. It
consists of only four core components (classes) to construct a data processing
pipeline. Each component provides an isolated subset of the functionality, which
includes defining the: processing nodes, connectivity and computational 
resources of a workflow and further enables deployment and run-time interactions 
(e.g. monitoring).

**PaPy** is very modular, functions can be used in several places in a
pipeline or re-used in another pipelines. Computational resources can be
shared among workflows and processing nodes.

In this chapter we first introduce object-otiented programming in the context
of **PaPy**, explain briefly the core components (building blocks). In later 
sections we revisit each component and explain the how and why.


Understanding the object-oriented model
---------------------------------------

**PaPy** is written in an object-oriented(OO) way. The main components: Plumber,
Dagger, Pipers and Workers are in fact class objects. For the end-user it is
important to distinguish between classes and class instances. In Python both
classes and class instances are objects. When you import the module in your
script::

  import papy

A new object (a module) will be availble i.e. you will be able to access classes
and functions provided by **PaPy** e.g.::

  papy.SomeClass

The name of the imported ``object`` will be ``papy``. This object has several 
attributes which correspond to the components and interface of ``papy`` e.g.::

  papy.Plumber
  papy.Dagger
  papy.Piper
  papy.Worker

Attributes are accessed in Python using the ``object.attribute`` notation. These
components are classes not class instances. They are used to construct class
instances which correspond to the run-time of the program. A single class can in
general have multiple instances. A class instance is constructed by "calling"
(in fact initializing) the class i.e.::

  class_instance = Class(parameters)

The important part is that using ``papy`` involves constructing class 
instances.::

  worker_instance = Worker(custom_function(s), argument(s))
  piper_instance = Piper(worker_instance, options)
  your_interface = Plumber(options)


core components
---------------

The core components form the end-user interface i.e. the classes which the user
is expected use directly.

  * NuMap - An implementation of an iterated map function which can process
            multiple tasks (function-sequence tuples) in parallel using
            either threads or processes on the local machine or on remote
            **RPyC** servers. ``NuMap`` instances represent computational
            resources.
  * Pipers(Workers) - combined define the processing nodes by wrapping 
                      user-defined functions and handling exceptions.
  * Dagger - defines the connectivity of the pipeline in the form of a directed
             acyclic graph i.e. the connectivity of the flow (pipes).
  * Plumber - provides the interface to set-up run and monitor a workflow at
              run-time.


The ``NuMap`` class
-------------------

The ``NuMap`` class is provided by the separate module ``numap`` and is 
described further in the section about parallel and distributed workflows. Here 
it suffices to say that it is an ``object`` which models a pool of cumputational
resources and allows to execute **multiple** functions using a shared pool of 
local or remote of workers. A ``NuMap`` can be used in any python code as an 
alternative to ``multiprocessing.Pool`` or ``itertools.imap``. For details
please refer to the documentation and API for ``numap``.

object provides a method to evaluate a functions on a sequence of changing 
arguments provided with optional positional and keyworded arguments to modify
the behaviour of the function. Just like ``multiprocessing.Pool.imap`` or 
``itertools.imap`` with the key differences that unlike ``itertools.NuMap`` it 
evaluates results in parallel. Compared to ``multiprocessing.Pool.imap`` it 
supports multiple functions (called tasks), which are evaluated not one after 
another, but in an alternating fashion. ``NuMap`` is completely independent from 
``PaPy`` and can be used separately (it is a standalone package).


evaluation In ``PaPy`` the lazy
``imap`` functions is replaced with a pool implementation ``NuMap``, which 
allows for a parallelizm vs. memory requirements trade-off.



The ``Worker`` class
--------------------

The ``Worker`` is a class which is created with a function or multiple functions
(and the functions arguments) as arguments to the constructor. It is therefore a
function wrapper. If multiple functions are supplied they are assumed to be 
nested with the last function being the outer most i.e.::

    (f,g,h) is h(g(f()))

If a ``Worker`` instance is called this compsite function is evaluated on the
supplied argument.::

    from papy import Worker
    from math import radians, degrees
    def papy_radians(input):
        return radians(input[0])
    def papy_degrees(input):
        return degrees(input[0])
    worker_instance = Worker((papy_radians, papy_degrees))
    worker_instance([90.])
    90.0

In this example we have created a composite ``Worker`` from two functions 
``papy_radians`` and ``papy_degrees``. The first function converts degrees to 
radians the second converts radians to degrees. Obviously if those two functions
are nested their result is identical to their input. ``papy_radians`` is 
evaluated first and ``papy_degrees`` second so the result is in degrees.

The ``Worker`` performs several functions:

  * standarizes the inputs and outputs of nodes.
  * allows to reuse and combine multiple functions into as single node
  * catches and wraps exceptions raised within functions.
  * allows functions to be evaluated on remote hosts.

A ``Worker`` expects that the wrapped function has a defined input and output
signature. The input is expected to be boxed in a tuple relative to the output, 
which should not be boxed. For example the ``Worker`` instance expects 
``[item]``, but returns just ``item``. Any function which conforms to this is a
valid Worker function. Most built-in functions need to be wrapped. Please refer
to the API documentation and examples on how to write Worker functions. 

If an exception is raised within any of the user written functions it is cought
by the ``Worker``, but is **not** raised, instead it is wrapped as a 
``WorkerError`` exception and returned.

The functionality of a ``Worker`` instance is defined by the functions it is
composed of and their arguments. Two ``Workers`` which are composed of the same
functions **and** are called with the same arguments are functionally identical
and a single ``Worker`` instance could replace them i.e. be used in multiple 
places of a pipeline or in other words in multiple ``Piper`` instances.

The functions within a ``Worker`` instance might not be evaluated by the same
process as the process that created (and calls) the ``Worker`` instance. This is
accomplished by the **RPyC** package and ``multiprocessing`` module. A 
``Worker`` knows how to inject its functions into a **RPyC** ``connection`` 
instance, after this the worker method will called in the local process,
but the wrapped functions on the remote host.

    import rpyc # import the RPyC module
    from papy import Worker
    power = Worker(pow, (2,)) # power of two
    power([2]) # evaluated locally
    4
    conn = rpyc.classic.connect("some_host") 
    power._incject(conn) # replace pow with remot pow
    power([3]) # evaluated remotely
    9

A function can run on the remote host i.e. remote Python process/thread only if
the modules on which this function depends are availble on that host and they 
are imported. ``NuMap`` provides means to attach import statements to function 
definitions using the ``imports`` decorator. In this way code sent to the remote
host will work if the imported module is availble remotely.::

   @imports(['re'])
   def match_string(input, string):
       unboxed = input[0]
       return re.match(string, unboxed)

The above example shows a valid worker function with the equivalent of the
import statment attached.::

    import re

The ``re`` module will be availble remotely in the namespace of this function 
i.e. other injected functions might not have access to ``re``. For more 
informations see the ``NuMap`` documentation.


Built-in worker functions
-------------------------

Several classes of ``Worker`` functions are already part of **PaPy**. This 
collection is expected to grow, currently the following types of workers are included.

  * core - basic data-flow
  * io - serialisation, printing and file operations

These are available in the ``papy.util.func`` module. This includes the family 
of passer functions. They do not alter the incoming data, but are used to pass 
only streams from certain imput pipes. For example a ``Piper`` connected to 
``3`` other Pipers might propagate input from only one.

  * ``ipasser`` - propagates the "i"th input pipe
  * ``npasser`` - propagates the "n"-first input pipes
  * ``spasser`` - propagetes the pipes with numbers in "s"

For example::

  from papy.util.func import *
  worker = Worker(ipasser, (0,)) # passes only the first pipe
  worker = Worker(ipasser, (1,)) # passes only the second pipe
  worker = Worker(npasser, (2,)) # passes the first two pipes
  worker = Worker(spasser, ((0,1),) # passes pipes 0 and 1
  worker = Worker(spasser, ((1,0),) # passes pipes 1 and 0

The output of the passes is a *single* tuple of the passed pipes::

  input0 = [0,1,2,3,4,5]
  input1 = [6,7,8,9,10,11]

  worker = Worker(spasser, (1,0))
  # will produce output
  [(6,0), (7,1), ...]

Functions dealing with input/output relations i.e. data storage and 
serialization currently allow serialization using the pickle and JSON protocols
and file-based data storage.

Data serialization is a way to convert objects (and in Python almost everything
is an object) into a sequence, which can be stored or transmitted. **PaPy** uses
the ``pickle`` serialization format to transmit data between local processes and
``brine`` (an internal serialization protocol from ``RPyC``) to transmit data 
between hosts. The user might however want to save and load data in a different 
format.


Writing functions for Workers
-----------------------------

A worker is an instance of the class Worker. Worker instaces are created by
calling the Worker class with a function or several functions as the argument.
optionally an argument set (for the function) or argument sets (for multiple
functions) can be supplied i.e.::

  worker_instance = Worker(function, argument)

or::

  worker_instance = Worker(list_of_functions, list_of_arguments)

A worker instance is therefore defined by two elements: the function or list of
functions and the argument or list of arguments. This means that two different
instances which have been initialized using the same functions *and* respecitve 
arguments are functionally equal. You should think of worker instances as nested
curried functions (search for "partial application").

Writing functions suitable for workers is very easy and adapting existing
functions should be the same. The idea is that any function is valid if it
conforms to a defined input/output scheme. There are only few rules which need to
be followed:

     #. The first input argument: each function in a worker will be given a n-tuple
        of objects, where n is the number input iterators to the Worker. For example 
        a function which sums two numbers should expect a tuple of lenght 2. 
        Remember python uses 0-based counting. If the Worker has only one input
        stream the input to the function will still be a tuple i.e. a 1-tuple.

     #. The additional (and optional) input arguments: a function can
        be given additional arguments.

     #. The output: a function should return a single object _not_ enclosed in a 
        wrapping 1-tuple. If a python function has no explicit return value it 
        implicitly returns None.

Examples:

single input, single ouput::

    def water_to_water(inp):
      result = inp[0]
      return result

single input, no explicit output::

    def water_to_null(inp):
      null = inp[0]

multiple input, single output::

    def water_and_wine(inp):
      juice = inp[0] + inp[1]
      return juice

multiple input, single output, parameters::

    def water_and_wine_dilute(inp, dilute =1):
      juice = inp[0] * dilute + inp[1]
      return juice

Note that in the last exemples inp is a 2-tuple i.e. the Piper based on such a 
worker/function will expect two input streams or in other words will have two 
incoming pipes. If on the other hand we would like to combine elements in the 
input/object from a single pipe we have to define a function like the
following::
        
    def sum2elements(inp):
        unwrapped_inp = inp[0]
        result = unwrapped_inp[0] + unwrapped_inp[1]
        return result

In other words the function receives a wrapped object but returns an unwrapped. 
All python objects can be used as results except Excptions. This is because 
Exceptions are not evaluated down-stream but are passively propagated.


Writing functions for output workers
------------------------------------

An output worker is a worker, which is used in a piper instance at the end of
a pipeline i.e. in the last piper.  Any valid worker function is also a valid
output worker function, but it is recommended for the last piper to persistently
safe the output of the pipeline. The output worker function should therefore
store it's input in a file, database or eventually print it on screen. The
function should not return data. The reason for this recommendation are related
to the implementation details of the IMap and Plumber objects.

    #. The Plumber instance runs a pipeline by retrieving results from output
       pipers *without* saving or returning those results

    #. The IMap instance will retrieve results from the output pieprs *without*
       saving whenever it is told to stop *before* it consumed all input.

The latter point requires some explanation. When the stop method of a running
IMap instance is called the IMap does not stop immediately, but is schedeuled to
stop after the current stride is finished for all tasks. To do this the output
of the pipeline has to be 'cleared' which means that results from output pipers
are retrieved, but not stored. Therefore the 'storage' should be a built-in
function of the last piper. An output worker function might therefore require an
argument which is a connection to some persistent storage e.g. a file-handle.


The ``Piper`` class
-------------------

A ``Piper`` instance represents a node in the directed graph of the workflow. 
It defines what function(s) should at this node be evaluated (via the supplied 
``Worker`` instance) and how they should be evaluated (via the optional 
``NuMap`` instance, which defines the uses computational resources). Besides 
that it performs additional functions which include:

  * logging and reporting
  * exception handling
  * timeouts
  * produce/spawn/consume schemes

To use a ``Piper`` outside a workflow three steps are required:

  * creation - requires a ``Worker`` instance, optional arguments e.g. a 
    ``NuMap``  instance. (``__init__`` method)
  * connection - connects the ``Piper`` to the input. (``connect`` method)
  * start - allows the ``Piper`` to return results, starts the evaluation in 
    ``NuMap``. (``start`` method)

In the first step we define the ``Worker`` which will be evaluated by the 
``Piper`` and the ``NuMap`` resource to do this computation. Computational 
resources are represented by ``NuMap`` instances. An ``NuMap`` instance can 
utilize local or remote threads or processes. If no ``NuMap`` instance is given
to the constructor the ``itertools.imap`` function will be used instead. This 
function will be called by the Python process used to construct and start the
**PaPy** pipeline.

**PaPy** has been designed to monitor the execution of a workflow by logging at
multiple levels and with a level of detail which can be specified. It uses the 
built-in Python logging (the ``logging`` module). The ``NuMap`` function, which
should at this  stage be bug free logs only DEBUG statements. Exceptions within
``Worker`` functions are wrapped as ``WorkerError`` exceptions, these errors are
logged by the ``Piper`` instance, which wraps this ``Worker`` (a single 
``Worker`` instance can be used by multiple ``Pipers``). By default the pipeline
is robust to ``WorkerErrors`` and these exceptions are logged, but they do not 
stop the flow. In this mode if the called ``Worker`` instance returns a 
``WorkerError`` the calling ``Piper`` instance wraps this error as a 
``PiperError`` and **returns** (not raises) it downstream into the pipeline. On
the other end if a ``Worker`` receives a ``PiperError`` as input it just 
propagates it further downstream i.e. it does not try meaningless calculations
on exceptions. In this way errors in the pipeline propagate downstream as place 
holder ``PiperErrors``.

A ``Piper`` instance evaluates the ``Worker`` either by the supplied ``NuMap`` 
instance (described elswhere) or by the builtin ``itertools.imap`` function 
(default). In reality after a ``Piper`` is connected to the input it creates a 
task i.e. function, data, arguments ``tuples``, which are added to the ``NuMap``
instance used to call the imap function.

``NuMap`` instances support timeouts via the optional timeout argument supplied to the
next method. If the ``NuMap`` is not able to return a result within the specified
time it raises a TimeoutError. This exception is cought by the ``Piper`` instance
which expects the result, wrapped into a PiperError exception and propagated
down-stream exactly like WorkerErrors. If the ``Piper`` is used within a pipeline
and a timeout argument given the skipping argument should be set to true
otherwise the number of results from a ``Piper`` will be bigger then the number of
tasklets, which will hang the pipeline.::

   # valid with or without timeouts
   universal_piper = Piper(worker_instance, parallel =imap_instance, skipping =True)
   # valid only with timeouts
   nontimeout_piper = Piper(worker_instance, parallel =imap_instance, skipping =False)

Note that the timeouts specified here are 'computation time' timeouts. If for
example a worker function waits for a server response and the server response
does not arrive within some timeout (which can be an argument for the Worker)
then if this exception is raise within the function it will be wrapped into a
WorkerError and raturned not raised as TimeoutErrors.

A single ``Piper`` instance can only be used once within a pipeline (this is 
unlike ``Worker`` instances). ``Pipers`` are created first and connected to the
input data later. The latter is accomplished by their ``connect`` method.::

    piper_instance.connect(input_data)

If the ``Piper`` is used within a **PaPy** pipeline i.e. a ``Dagger`` or 
``Plumber`` instance the user does not have to care about connecting individual
``Pipers``. A ``Piper`` can only be started or disconnected if it has been 
connected before.::

    piper_instance.connect(input_data)
    piper_instance.disconnect()
    # or
    piper_instance.start()

After starting a ``Piper`` the tasks are submitted to the thread/process workers
in the ``NuMap`` instance and they are evaluated. This is a process that 
continues until either the memory "buffer" is filled or the input is consumed. 
Therefore a ``Piper``cannot be simply disconnected when it is "running". A 
special method is needed to tell the ``NuMap`` instance to stop input 
consumption. Because ``NuMap`` instances are shared among ``Pipers`` such a 
stop can only occur at "stride" boundaries, which are batches of data traversing
the workflow. The ``Piper`` stop method will eventually stop the ``NuMap`` 
instance and put the ``Piper`` in a stopped state that allows the ``Piper`` to 
be disconnected.::

    piper_instance.start()
    piper_instance.stop()
    piper_instance.disconnect() # can be connected and started

Because the stop happens at "stride" boundary data is not lost during a stop. 
This can be illustraded as follows::

    #           plus2            plus1
    # [1,2,3,4] -----> [3,4,5,6] -----> [4,5,6,7]
    # which is equivalent to the following:
    # plus1(plus2([1,2,3,4]) 

If the ``Pipers`` ``plus2`` and ``plus1`` share a single ``NuMap`` and the 
"stride" is ``2`` then the order of evaluation can be (if the results are 
retrieved)::

    temp1 = plus2(1)
    temp2 = plus2(2)
    plus1(temp1)
    plus1(temp2)
    <<return>>
    <<return>>
    temp1 = plus2(3)
    temp2 = plus2(4)
    plus1(temp1)
    plus1(temp2)
    <<return>>
    <<return>>

Now let's assume the the stop method has been called just after ``plus2(1)``. We
do not want to loose the ``temp1`` result (as ``1`` has been already consumed 
from the input iterator and iterators cannot rewind), but we can achieve this 
only if ``plus1(temp1)`` is evaluated this in turn (due to the order of e
valuation) can happen only after ``plus2(2)`` has been evaluated (i.e. ``2`` 
consumed from the input iterator). To not loose ``temp2`` ``plus1(temp2)`` has
to be evaluated and finally the evaluation can stop.::

    temp1 = plus2(1)
    temp2 = plus2(2)
    plus1(temp1)
    plus1(temp2)
    (stopped)

After the stop method returns all worker processes/threads and helper threads
return (join) and the user can close the Python interpreter. 

It is **very** important to realize what happens with the two calculated 
results. As has been already mentioned a proper **PaPy** pipeline should have
an output ``Piper`` i.e. a one that persistently stores the result.


The ``Dagger``
--------------

The ``Dagger`` is an object to connect ``Piper`` instances into a directed 
acyclic graph (DAG). It inherits most methods of the ``DictGraph`` object, which
is a concise implementation of a graph data-structure. The ``DictGraph`` 
instance is a dictionary of arbitary hashable objects i.e. the "object nodes" 
e.g. a ``Piper``. The values for the objects are instances of the ``Node`` class
i.e. "topological nodes". A "topological node" instance is a also dictionary of 
"object nodes" and their corresponding "topological nodes". An "object node"(A) 
of the ``DictGraph`` is contained in a "topological node" corresponding to 
another "object node"(B) if there exist an edge from (A) to (B). A and B might 
even be the same "object node" (self-loop). A "topological node" is therefore a
sub-graph of the ``DictGraph`` instance centered around a "object node" and the
whole ``DictGraph`` is a recursively nested dictionary. The ``Dagger`` is 
designed to store ``Piper`` instances as "object nodes" and provides additional
methods, whereas the ``DictGraph`` makes no assumptions about the ``object``
type. 


Edges vs. pipes
_______________

A ``Piper`` instance is created by specifiying a ``Worker`` (and optionally 
``NuMap`` instance) and needs to be connected to an input. The input might be 
another ``Piper`` or any Python iterator. The output of a ``Piper`` (upstream) 
can be consumed by several ``Pipers`` (downstream), while a ``Piper`` 
(downstream) might consume the results of multiple ``Pipers`` (upstream). This 
allows ``Pipers`` to be used as arbitrary nodes in a directed acyclic graph the
``Dagger``.

To be precise the direction of the edges is opposite
to the direction of the data stream (pipes). Upstream ``Pipers`` have incomming
edges from downstream ``Pipers`` this is represented as a pipe with a opposite
orientation i.e. upstream -> downstream. 

As a result of the above it is much more natural to think of connections between
``Pipers`` in terms of data-flow upstream --> downstream (data flows from 
upstream to downstream) then dependency downstream --> upstream (downstream
depends on upstream). The ``DictGraph`` represents dependancy information as 
directed edges (downstream --> upstream), while the ``Dagger`` class introduces
the concept of pipes to ease the understanding of **PaPy** and make mistakes 
less common. A pipe is nothing else then a reversed edge. To make this 
explicit::

    input -> piper0 -> piper1 -> output # -> represents a pipe (data-flow)
    input <- piper0 <- piper1 <- output # <- represents an edge (dependancy)

The data is stored internally as edges, but the interface uses pipes. Method
names are explicit.::

    dagger_instance.add_edge() # inherited expects and edge as input 
    dagger_instance.add_pipe() # expecs a pipe as input 

.. note::

    Although all ``DictGraph`` methods are availble from the ``Dagger`` the 
    end-user should use ``Dagger`` specific methods. For example the 
    ``DictGraph`` method ``add_edge`` will allow to add any edge to the 
    instance, whereas ``add_pipe`` method will not allow to introduce cycles.


Working with the ``Dagger``
___________________________

Creation of the a ``Dagger`` instance is very easy. An empty ``Dagger`` instance 
is created without any arguments to the constructor.::

    dagger_instance = Dagger()

Optionally a set of ``Pipers`` and/or pipes can be given:: 

    dagger_instance = Dagger(sequence_of_pipers, sequence_of_pipes)
    # which is equivalent to: 
    dagger_instance.add_pipers(sequence_of_pipers)
    dagger_instance.add_pipes(sequence_of_pipes)
    # a sequence of pipers allows to easily add branches
    dagger_instance.add_pipers([1, 2a, 3a, 4])
    dagger_instance.add_pipers([1, 2b, 3b, 4])
    # in this example a Dagger will have 6 pipers (1, 2a, 2b, 3a, 3b, 4), one 
    # branch point 1, one merge point 4, and two branches (2a, 3a) and (2b, 3b).

The ``Dagger`` allows to add/delete ``Pipers`` and pipes::

    dagger_instance.add_piper(``Piper``) 
    dagger_instance.del_piper(``Piper`` or piper_id)
    dagger_instance.add_pipers(pipers)
    dagger_instance.del_pipers(pipers or piper_ids)

The id of a ``Piper`` is a run-time specific number associated with a given 
``Piper`` instance. This number can be obtained by calling the built-in function
id::

    id(``Piper``)

This number is also shown when a ``Piper`` instance is printed.::

    print piper_instance

or represented::

    repr(piper_instance)

The representation of a ``Dagger`` instance also shows the id of the ``Pipers``
which are contained in the workflow.::

    print dagger_instance

The id of a ``Piper`` instance is define at run-time (it corresponds to the 
memory address of the object) therefore it should not be used in scripts or 
saved in  any way. Note that the lenght of this number is platform-specific and
that no guarantee is made that two ``Pipers`` with non-overlapping will not have
the same id. The resolve method::

   dagger_instance.resolve(``Piper`` or piper_id)

returns a ``Piper`` instance if the supplied ``Piper`` or a ``Piper`` with the 
supplied id is contained in the dagger_instance. This method by default raises a
``DaggerError`` if the ``Piper`` is not found. If the argument forgive is ``True``
the method returns ``None`` instead::

   dagger_instance.resolve(missing_piper) # raise DaggerError
   dagger_instance.resolve(missing_piper, forgive =True) # returns None


The ``Dagger`` run-time
_______________________

The run-time of a ``Dagger`` instance begins when it's start method is called.
A ``Dagger`` can only be started if it is connected. Connecting a ``Dagger`` 
means to connect all ``Pipers`` which it contains as defined by the pipes in the 
``Dagger``. After the ``Dagger`` is connected it can be started, starting a ``Dagger``
means to start all it's ``Pipers``. ``Pipers`` have to be started in the order of 
the data-flow i.e. a ``Piper`` can only be started after all it's up-stream 
``Pipers`` have been started. An ordering of nodes / ``Pipers`` of a graph / ``Dagger`` 
which has this property is called a postorder. There are possibly more then one
postorder per graph ``Dagger``. The exact postorder used to connect the ``Pipers``
has some additional properties

    - all down-stream ``Pipers`` for a ``Piper`` (A) come before the next ``Piper`` 
      (B) for which no such relationship can be established. This can be thought
      as maintaining branch contiguity.
      
    - such branches can additionally be sorted according to the branch argument
      passed to the ``Piper`` constructor.

Another aspect of order of a ``Dagger`` is the sequence by which a down-stream 
``Piper`` connects multiple up-stream ``Pipers``. The inputs cannot be sorted 
based solely on their postorder because the down-stream ``Piper`` might be 
connected directly to a ``Piper`` to which one of it's other inputs has been 
connected before. The inputs of a ``Piper`` are additionaly sorted so that all 
down-stream ``Pipers`` come before up-stream ``Pipers``, while ``Pipers`` for which no
such relation can be established are still sorted according to their index in 
the postorder. This can be thought of as sorting branches by their "generation".

You could
think of a workflow as an ``imap`` function composed  from nested ``imap`` functions i.e.::

  # nested imaps as pipelines
  pipeline = imap(h, izip([imap(f, input_for_f), imap(g, input_for_g)]))

This is a pipeline of ``3`` functions ``f``, ``g``, ``h``. Functions ``f`` and 
``g`` are upstream relative to ``h``. Because of the ``izip`` function 
input_for_f and input_for_g have to be of the same lenght.

A started ``Dagger`` is able to process input data. The simplest way to process 
all inputs is to zip it's output ``Pipers``::

    output_pipers = dagger_instance.get_outputs()
    final_results = zip(output_pipers)
    
If any of the ``Pipers`` used within a ``Dagger`` uses an ``NuMap`` instance and the 
``Dagger`` is started. The Python process can only be exited cleanly if the 
``Dagger`` instance is stopped by calling it's `stop` method. 
    

The ``Plumber``
---------------

The ``Plumber`` is an easy to use interface to **PaPy**. It inherits from the 
``Dagger`` object and can be used like a ``Dagger``, but the ``Plumber`` class 
adds  methods related to the "run time" of a pipeline. A ``Plumber`` can 
start/run/pause/stop a pipeline and additionally load and save a workflow (not 
implemented) A **PaPy** workflow is loaded and saved as executable Python code, 
which has the  same priviliges as the Python process. Please keep this in mind 
starting workflows from untrusted sources!


The additional components
-------------------------

Those classes and functions are used by the core components, but are general and
might find application in your code.

  * ``DictGraph``(``Node``) - Two classes which implement a graph data-structure
    using a recursively nested dictionary. This allows for simplicity of 
    algorithms/methods i.e. there are no edge objects because edges are the keys
    of the ``Node`` dictionary which in turn is the value in the dictionary for 
    the arbitrary object in the ``DictGraph`` instance i.e.::

      from papy import Graph
      graph = Graph()
      object1 = '1'
      object2 = '2'
      graph.add_edge((object1, object2))
      node_for_object1 = graph[object1]
      node_for_object2 = graph[object2]

    The ``Dagger`` is a ``DictGraph`` object with directed edges only and no 
    cycles.

  * imports - a function wrapper, which allows to inject import statments to
    a functions local namespace at creation (code execution) e.g. on a remote
    Python process.
