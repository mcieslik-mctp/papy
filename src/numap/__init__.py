# -*- coding: utf-8 -*-
"""
**NuMap** - lazy, parallel, remote, multi-task map function
###########################################################

``NuMap`` is a parallel (thread- or process-based, local or remote), 
buffered, multi-task, ``itertools.imap`` or ``multiprocessing.Pool.imap`` 
function replacment. Like ``imap`` it evaluates a function on elements of a
sequence or iterable, and it does so lazily. Laziness can be adjusted via 
the "stride" and "buffer" arguments. Unlike ``imap``, ``NuMap`` supports 
**multiple pairs** of function and iterable **tasks**. The **tasks** are 
**not** queued rather they are **interwoven** and share a pool or **worker**
"processes" or "threads" and a memory "buffer".

The package is tested on Python 2.6+

"""
__author__ = 'Marcin Cieslik <mpc4p@virginia.edu>'

from .NuMap import NuMap, imports
