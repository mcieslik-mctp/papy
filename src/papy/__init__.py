# -*- coding: utf-8 -*-
"""
**PaPy** - Parallel Pipelines in Python
#######################################

The ``papy`` package provides an implementation of the flow-based programming 
paradigm in Python that enables the construction and deployment of distributed
workflows.

The package is tested on Python 2.7

"""
__author__ = 'Marcin Cieslik <mpc4p@virginia.edu>'

from .core import Worker, Piper, Dagger, Plumber
from .core import WorkerError, PiperError, DaggerError, PlumberError

