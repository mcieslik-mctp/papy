#!/usr/bin/env python
# -*- coding: utf-8 -*-
from numap import NuMap
from papy.core import Dagger, Piper, Worker
from papy.util.script import script

import os

sh_cfg = {
    "evaluator":"bash",
    "preamble":"",
    "dir":os.getcwd(),
    "executable":"bash",
    "script":"bash_script.sh",
    "in":(
        ("message", "file"),
    ),
    "out":(
         ("greeting", "file", False), # type = file, keep = false
    ),
    "params":{}
}

py_cfg = {
    "evaluator":"bash",
    "preamble":"",
    "dir":os.getcwd(),
    "executable":"python",
    "script":"python_script.py",
    "in":(
        ("greeting", "file"), # input type file
    ),
    "out":(
        ("package", "file", True),
    ),
    "params":{}
}

sh_worker = Worker(script, (sh_cfg,))
py_worker = Worker(script, (py_cfg,))

# execution engine
numap = NuMap()

# function nodes
sh_piper = Piper(sh_worker, parallel=numap)
py_piper = Piper(py_worker, parallel=numap)

# topology
pipeline = Dagger()
pipeline.add_pipe((sh_piper, py_piper))
end = pipeline.get_outputs()[0]

# runtime
pipeline.connect(
    [[
        {"message":("work moar", "file", True)}, 
        {"message":("nevar give up", "file", True)}
    ]])

pipeline.start()
print list(end)


