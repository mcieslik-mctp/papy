#!/usr/bin/env python2
# -*- coding: utf-8 -*-
import sys, json, logging
from subprocess import Popen, PIPE
ARGS = json.load(open(sys.argv[1]))
PARAMS = ARGS.pop("params")
IN = ARGS.pop("in")
OUT = ARGS.pop("out")

log = OUT.get("log", None)
if log:
    out = open(log, "wb")
else:
    out = sys.stdout

def run_cmd(cmd):
    app = Popen(cmd, stdin=None, shell=True, stdout=PIPE, stderr=PIPE)
    stdout, stderr = app.communicate(None)
    code = app.wait()
    return (code, stdout, stderr, cmd)

def log_run(code_stdout_stderr_cmd):
    code, stdout, stderr, cmd = code_stdout_stderr_cmd
    if not code:
        out.write("Success: %s\n" % cmd)
    else:
        out.write("Failure (code: %s): %s\n" % (code, cmd))
        out.write("stdout: \n" + stdout)
        out.write("stderr: \n" + stderr)
    return code
