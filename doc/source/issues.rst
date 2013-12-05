Known Issues
############

PaPy in the Interactive Interpreter
===================================

Parallel features of **PaPy** do **not** work in the interactive interpreter. 
This is a limitation of the Python ``multiprocessing`` module.

This means that **PaPy** workflows can be created, manipulated, tested and run
from within the interactive interpreter freely as long as they do not use the 
parallel or remote evaluation.

Code snippets, examples and use cases are not meant to be typed into the 
interactive interpreter console. They should be run from the command line::
  
  $ python example_file.py

The reason for this is that functions defined in the interactive interpreter 
will not work (and will hang Python) if passed into ``NuMap`` instances!  A 
Python function can only be communicated to multiple processes if it can be 
serialized i.e. pickled. This is not possible if the function is in the same
namespace as a child process (created using the multiprocessing library).


Object Picklability
===================

Objects are submitted to the worker-threads by means of queues to
worker-processes by pipes and to remote processes using sockets. This requires
serialization, which is internally done by the cPickle module. Additionally RPyC
uses it's own 'brine' for serialization. The submitted objects include the
functions, data, arguments and keyworded arguments all of which need to be
picklable! 

Worker-methods are impossible
+++++++++++++++++++++++++++++

Class instances (i.e. Worker instances) are picklable only if all of their
attributes are.  On the other hand class instance methods are not picklable
(because they are not top-level module functions) therefore class methods
(either static, class, bound or unbound) will not work for a parallel piper.

File-handles make remotely no sense
+++++++++++++++++++++++++++++++++++

Function arguments should be picklable, but file-handles are not. It is
recommeded that output pipers store data persistently, therfore output workers
should be run locally and not use a parallel IMap, circumventing the requirement
for picklable attributes.