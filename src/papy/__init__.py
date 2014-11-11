0;115;0c# -*- coding: utf-8 -*-
"""
**PaPy** - Parallel Pipelines in Python
#######################################

The ``papy`` package provides an implementation of the flow-based programming 
paradigm in Python that enables the construction and deployment of distributed
workflows.

The package is tested on Python 2.7

"""
__version__ = "1.0.7"

from .core import Worker, Piper, Dagger, Plumber
from .core import WorkerError, PiperError, DaggerError, PlumberError

