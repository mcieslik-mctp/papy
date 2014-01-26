#!/usr/bin/env python2
# -*- coding: utf-8 -*-
from numap import NuMap
from papy.core import Dagger, Piper, Worker
from papy.util.script import script
from papy.util.func import npasser

import os

sh_cfg = {
    "id":"sh",
    "evaluator":"bash",
    "preamble":"",
    "dir":os.getcwd(),
    "executable":"bash",
    "script":"bash_script.sh",
    "in":("message",),
    "out":(
        ("greeting", "txt"),
    ),
    "params":{
    }
}

py_cfg = {
    "id":"py",
    "evaluator":"bash",
    "preamble":"",
    "dir":os.getcwd(),
    "executable":"python",
    "script":"python_script.py",
    "in":(
        "greeting",
    ),
    "out":(
        ("package", "txt"),
    ),
    "params":{
    }
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
pipeline.add_pipe((sh_piper, py_worker))
end = pipeline.get_outputs()[0]

# runtime
pipeline.connect(
    [[
        {"message":"work_moar.txt"}, 
        {"message":"nevar_give_up.txt"}
    ]])

pipeline.start()
print list(end)
