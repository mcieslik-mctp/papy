Quick Introduction
##################

.. warning::

    Parallel features of **PaPy** do **not** work in the interactive 
    interpreter. This is a limitation of the Python ``multiprocessing`` module.
    
This part of the documentation is all about creating a valid **PaPy** workflow. 

Creating a workflow invovles several steps:

  #. Writing worker functions (the functions in the nodes)
  #. Creating ``Worker`` instances (to specify worker function parameters)
  #. (optionally) creating ``NuMap`` instances to represent parallel 
     computational resources.
  #. Creating ``Piper`` instances (to specify how/where to evaluate the workers)
  #. Creating a ``Dagger`` instance (to specify the workflow connections)

After construction a workflow can be deployed (or run) by connecting it to input
data and retrieving batches of results. 

Workflows exist in two very distinct states before and after they are started.
Those states correspond to the "construction-time" and "run-time" of a workflow
lifecycle. At "creation-time" functions are defined (written or imported) and
the the data-flow is defined by connecting functions by directed pipes. At run 
time data is actually "pumped" through the pipeline. A pipeline can be saved 
and loaded as a python script.


The Workflow Graph
==================
        
In ``PaPy`` a workflow is a directed acyclic graph. The direction of the graph 
is defined by the data flow (pipes) between the data-processing units (nodes, 
``Pipers``). ``PaPy`` pipelines can be branched i.e. two downstream ``Pipers``
might consume input from the same upstream ``Piper`` or one down-stream 
``Piper`` consumes data from several upstream ``Pipers``. ``Pipers``, which 
consume (are connected to) data from outside the directed  acyclic graph are 
input ``Pipers``, while ``Pipers`` to which no other ``Pipers`` are connected to
are output ``Pipers``. ``PaPy`` supports pipelines with multiple inputs and
outputs, also several input nodes can consume the same external data. 


Workflow Execution
==================

Workflows process data items either in parallel or layzily one after another.
Distributed evaluation is enabled by the ``numap`` package that provides the 
``NuMap`` object. Instances of this object represent computational resources 
(pools of local and/or remote processes) that are assigned to processing nodes.
It is possible that multiple processing nodes share a single resource pool.


Creating a workflow
===================

Our first workflow



Writing a worker function
-------------------------

**PaPy** imposes restrictions on the inputs and outputs of a worker function. 
Further we recommend a generic way to construct a pipeline::

  input_iterator -> input_piper -> ... processing pipers ... -> output_piper

The ``input_piper`` should transform input data items into a specific Python
objects. Processing pipers should manipulate or transform this objects finally
the ``output_piper`` should be used to store the output of the workflow
persistently i.e. it should return ``None``. **PaPy** provides useful functions 
to create input/output pipers.

We will start by writing the simples possible function it will do nothing, but 
return the input::

    def pass_input(inbox):
        input = inbox[0]
        return input
        
This function can only be used in a ``Piper`` that has only one input i.e. it 
uses only the first element of the "inbox" ``tuple``. A function with two 
input values could be used at a point in a workflow where two branches join.::

    def merge_inputs(inbox):
        input1 = inbox[0]
        input2 = inbox[1]
        return (input1, input2)        

Writing a worker function which takes arguments
-----------------------------------------------

A function can be written to take additional arguments (with or without default 
values). In the first example the first element in the inbox is multiplied by a
constant factor specified as the number argument. In the second example an 
arithmetical operation will be done on two numbers as specified by the operation
argument.::

    def multiply_by(inbox, number):
        input = inbox[0]
        output = input * number
        return output
        
    def calculator(inbox, operation):
        input1 = inbox[0]
        input2 = inbox[1]
        if operation == 'add':
            result = input1 + input2
        elif operation == 'subtract':
            result = input1 - input2
        elif operation == 'multiply':
            result = input1 * input2
        elif operation == 'divide':
            result = input1 / input2
        else:
            result = 'error'
        return result
                
The additional arguments for these functions are specified when a ``Worker`` is
constructed::

    multiply_by_2 = Worker(multiply_by, number =2)         
    calculate_sum = Worker(calculator, operation ="sum")

The argument names need not to be given::

    multiply_by_3 = Worker(multiply_by, 3)
    calculate_product = Worker(calculator, "multiply")

If a worker is constructed from multiple functions i.e. "sum the two 
inputs and multiply the result by 2"::

    # positional arguments
    sum_and_multiply_by_2 = Worker((calculator, multiply_by), \
                                   (('sum',),   (3,))):
    # keyworded arguments
    sum_and_multiply_by_2 = Worker((calculator, multiply_by), \
                                    kwargs = \
                                   ({'operation':'sum'}, {'number':3}))

In the last example the second argument given is in the first version a 
tuple of tuples which are the positional arguments for the function or
as in the second example a tuple of dictionaries with named arguments.

