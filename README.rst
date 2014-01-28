**PaPy** - Parallel Pipelines in Python
#######################################

The ``papy`` package provides an implementation of the flow-based programming 
paradigm in Python that enables the construction and deployment of distributed
workflows.

The ``NuMap`` package is a parallel (thread- or process-based, local or 
remote), buffered, multi-task, ``itertools.imap`` or 
``multiprocessing.Pool.imap`` function replacment. Like ``imap`` it 
evaluates a function on elements of a sequence or iterable, and it does so 
lazily. Laziness can be adjusted via  the "stride" and "buffer" arguments. 
Unlike ``imap``, ``NuMap`` supports  **multiple pairs** of function and 
iterable **tasks**. The **tasks** are **not** queued rather they are 
**interwoven** and share a pool or **worker** "processes" or "threads" and 
a memory "buffer".

Documentation can be found `here <http://mcieslik-mctp.github.io/papy/>`_

The package is tested on Python 2.7.6
