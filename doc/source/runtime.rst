Runtime (Logging, Interaction, Errors and Timeouts)
###################################################

A started workflow evaluates the functions of the processing nodes on the data
items traversing the pipeline using the assigned computational resources. The 
execution happens in the background i.e. the user has the choic to interact
with the running pipeline *via* the ``Plumber`` object. Further components of 
a workflow i.e. ``Piper`` and ``NuMap`` instances log runtime messeges. These
logs can be saved to disk and inspected when needed.  

Logging behavior is custimized by ``papy.util.runtime``, interaction is possible
through ``papy.core.Plumber``, exceptions, error and timeouts are handled by 
the processing nodes i.e. ``papy.core.Piper`` instances. 


Logging
=======

A function to configure at what importance level and how logged messeges should 
be saved or displayed is available in the ``papy.util.runtime`` module.

Interaction with the Plumber
============================

Please see the methods of the ``Plumber`` object.

Errors and Exceptions
=====================

**PaPy** workflows are by default resistant to all exceptions that occur as 
a result of error in user-provided worker functions.

Dealing with Timeouts
=====================

To Be Written.




