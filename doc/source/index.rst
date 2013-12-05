**PaPy** - Parallel Pipelines in Python
#######################################

A parallel pipeline is a workflow, which consists of a series of connected 
processing steps to model computational processes and automate their execution
in parallel on a single multi-core computer or an ad-hoc grid. 

You will find **PaPy** useful if you need to design and deploy a scalable data 
processing workflow that depends on Python libraries or external tools. 
**PaPy** makes it reasonably easy to convert existing code bases into proper 
workflows. 

    * project page: 
        http://mcieslik-mctp.github.io/papy
    * repository lives at: 
        https://github.com/mcieslik-mctp/papy.git
    * most recent documentation sources:
        http://mcieslik-mctp.github.io/papy
    * author email: 
        mcieslik-mctp@med.umich.edu

This documentation covers the design, implementation and usage of **PaPy**. It 
consists of a hand-written manual and an API-reference. Please refer also to the
rich comments in the source code, examples, workflows and test cases (all 
included in the source-code distribution). 

``NuMap`` is a parallel (thread- or process-based, local or remote), 
buffered, multi-task, ``itertools.imap`` or ``multiprocessing.Pool.imap`` 
function replacment. Like ``imap`` it evaluates a function on elements of a
sequence or iterable, and it does so lazily. Laziness can be adjusted via 
the "stride" and "buffer" arguments. Unlike ``imap``, ``NuMap`` supports 
**multiple pairs** of function and iterable **tasks**. The **tasks** are 
**not** queued rather they are **interwoven** and share a pool or **worker**
"processes" or "threads" and a memory "buffer".

The package is tested on Python 2.7+


Contents
========

.. toctree::
   :maxdepth: 2

   Bullet Points <bullets.rst>
   Installation <installation.rst>
   First Pipeline <first.rst>
   PaPy Architecture Explained <architecture.rst>   
   Parallelism Explained <parallelism.rst>
   Inter-Process Communication Explained <ipc.rst>
   The Produce / Spawn / Consume Idiom <psc.rst>   
   Runtime (Logging, Interaction, Errors and Timeouts) <runtime.rst> 
   Optimization and Tweaking <optimization.rst>
   Examples <examples.rst>
   PaPy for Bioinformatics <nubio.rst>
   Complete Workflows <workflows.rst>
   Known Issues <issues.rst>
   FAQ <faq.rst>
   Terms <terms.rst>
   Application Programming Interface (API) <api.rst>


Indices and tables
##################

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

